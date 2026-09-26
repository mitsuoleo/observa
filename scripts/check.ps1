#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet('all','node','python','relay-db','images','audit','secrets','manifests','rules')]
    [string]$Only = 'all'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:npm_config_cache = Join-Path $root '.local/npm-cache'
New-Item -ItemType Directory $env:npm_config_cache -Force | Out-Null

function Invoke-Step {
    param([string]$Name, [scriptblock]$Body)
    Write-Host "==> $Name"
    & $Body
    if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE" }
}

function Invoke-Native {
    param([string]$File, [string[]]$Arguments)
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$File $($Arguments -join ' ') failed with exit code $LASTEXITCODE" }
}

function Test-Tool {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) { throw "Required tool missing: $Name" }
}

function Test-Node {
    $version = if (Get-Command node -ErrorAction SilentlyContinue) { (& node --version).Trim() } else { 'missing' }
    if ($version -notmatch '^v22\.') {
        Test-Tool docker
        Write-Host "Node.js 22 is required for the native Kafka addon; found $version. Using pinned Node 22 test images."
        foreach ($item in @(
            @('packages/node/Dockerfile','observa-check-messaging-node',$false),
            @('probes/node/Dockerfile','observa-check-probe-node',$true),
            @('services/payment/Dockerfile','observa-check-payment',$true),
            @('services/inventory/Dockerfile','observa-check-inventory',$true)
        )) {
            $file,$image,$hasTestStage = $item
            $buildArgs = @('build')
            if ($hasTestStage) { $buildArgs += @('--target','test') }
            $buildArgs += @('-f',$file,'-t',$image,'.')
            Invoke-Step "$file Node 22 test image" { Invoke-Native docker $buildArgs }
            Invoke-Step "$file Node 22 tests" { Invoke-Native docker @('run','--rm','--network','none',$image) }
        }
        return
    }
    Test-Tool npm
    foreach ($directory in @('packages/node','probes/node','services/payment','services/inventory')) {
        Push-Location $directory
        try {
            Invoke-Step "$directory install" { Invoke-Native npm @('ci','--no-audit','--no-fund') }
            foreach ($command in @('build','lint','typecheck','test')) {
                Invoke-Step "$directory $command" { Invoke-Native npm @('run',$command) }
            }
        } finally { Pop-Location }
    }
}

function Test-Python {
    Test-Tool docker
    Test-Tool python
    Test-Tool pwsh
    foreach ($item in @(
        @('packages/python/Dockerfile','observa-check-messaging-python'),
        @('probes/python/Dockerfile','observa-check-probe-python'),
        @('harness/Dockerfile','observa-check-harness'),
        @('services/order/Dockerfile','observa-check-order'),
        @('services/notification/Dockerfile','observa-check-notification')
    )) {
        $file,$image = $item
        $buildArgs = @('build')
        if ($file -notin @('packages/python/Dockerfile','harness/Dockerfile')) { $buildArgs += @('--target','test') }
        $buildArgs += @('-f',$file,'-t',$image,'.')
        Invoke-Step "$file test image" { Invoke-Native docker $buildArgs }
        $runArgs = @('run','--rm','--network','none')
        if ($file -eq 'services/order/Dockerfile') {
            $runArgs += @('-e','DATABASE_URL=postgresql+psycopg://test:test@127.0.0.1:5432/test')
        }
        if ($file -eq 'services/notification/Dockerfile') {
            $runArgs += @('-e','DATABASE_URL=postgresql://test:test@127.0.0.1:5432/test')
        }
        $runArgs += $image
        Invoke-Step "$file tests" { Invoke-Native docker $runArgs }
    }
    Invoke-Step 'offline evidence tests' { Invoke-Native python @('-m','unittest','discover','-s','tests/evidence','-v') }
    Invoke-Step 'consumer lag exporter tests' { Invoke-Native python @('-m','unittest','discover','-s','infra/mvp/scaling','-p','test_lag_exporter.py') }
    Invoke-Step 'PowerShell automation tests' { Invoke-Native pwsh @('-NoProfile','-File','scripts/test-automation.ps1') }
    Invoke-Step 'PowerShell scaling automation tests' { Invoke-Native pwsh @('-NoProfile','-File','scripts/test-scale-automation.ps1') }
}

function Test-RelayDatabase {
    Test-Tool docker
    $suffix = [Guid]::NewGuid().ToString('N').Substring(0, 12)
    $network = "observa-relay-test-$suffix"
    $database = "observa-relay-postgres-$suffix"
    $password = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(16)).ToLowerInvariant()
    $postgresImage = 'postgres:16-alpine@sha256:3c5c8892d184f738f4fe282d14ddaa613a38f00f4189d2d94725ebe6f2909ddb'
    $networkCreated = $false
    $databaseStarted = $false
    $nodeUrl = "postgresql://observa:${password}@postgres:5432/relay_tests"
    $orderUrl = "postgresql+psycopg://observa:${password}@postgres:5432/relay_tests"
    $environment = @{
        POSTGRES_PASSWORD = $password
        OBSERVA_PAYMENT_TEST_DATABASE_URL = $nodeUrl
        OBSERVA_INVENTORY_TEST_DATABASE_URL = $nodeUrl
        OBSERVA_ORDER_TEST_DATABASE_URL = $orderUrl
        DATABASE_URL = $orderUrl
    }
    $previousEnvironment = @{}
    foreach ($name in $environment.Keys) {
        $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $environment[$name], 'Process')
    }
    $originalError = $null
    $cleanupErrors = @()
    try {
        Invoke-Native docker @('network','create',$network) | Out-Null
        $networkCreated = $true
        Invoke-Native docker @('run','-d','--name',$database,'--network',$network,'--network-alias','postgres',
            '-e','POSTGRES_USER=observa','-e','POSTGRES_PASSWORD','-e','POSTGRES_DB=relay_tests',$postgresImage) | Out-Null
        $databaseStarted = $true
        $ready = $false
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            & docker exec $database pg_isready -U observa -d relay_tests *> $null
            if ($LASTEXITCODE -eq 0) { $ready = $true; break }
            Start-Sleep -Seconds 2
        }
        if (-not $ready) { throw 'Disposable PostgreSQL did not become ready within 60 seconds' }

        foreach ($item in @(
            @('services/payment/Dockerfile','observa-check-payment','OBSERVA_PAYMENT_TEST_DATABASE_URL'),
            @('services/inventory/Dockerfile','observa-check-inventory','OBSERVA_INVENTORY_TEST_DATABASE_URL'),
            @('services/order/Dockerfile','observa-check-order','OBSERVA_ORDER_TEST_DATABASE_URL')
        )) {
            $file,$image,$variable = $item
            Invoke-Step "$file relay test image" { Invoke-Native docker @('build','--target','test','-f',$file,'-t',$image,'.') }
            $runArgs = @('run','--rm','--network',$network,'-e',$variable)
            if ($image -eq 'observa-check-order') { $runArgs += @('-e','DATABASE_URL') }
            $runArgs += $image
            Invoke-Step "$file PostgreSQL relay tests" { Invoke-Native docker $runArgs }
        }
    } catch {
        $originalError = $_
    } finally {
        if ($databaseStarted) {
            & docker rm -f $database *> $null
            if ($LASTEXITCODE -ne 0) { $cleanupErrors += "container $database" }
        }
        if ($networkCreated) {
            & docker network rm $network *> $null
            if ($LASTEXITCODE -ne 0) { $cleanupErrors += "network $network" }
        }
        foreach ($name in $previousEnvironment.Keys) {
            [Environment]::SetEnvironmentVariable($name, $previousEnvironment[$name], 'Process')
        }
    }
    if ($cleanupErrors.Count -gt 0) { Write-Warning "Disposable Docker cleanup failed: $($cleanupErrors -join ', ')" }
    if ($originalError) { throw $originalError }
    if ($cleanupErrors.Count -gt 0) { throw 'Disposable Docker cleanup failed' }
}

function Test-Images {
    Test-Tool docker
    foreach ($service in @('order','payment','inventory','notification')) {
        Invoke-Step "$service runtime image" {
            Invoke-Native docker @('build','--target','runtime','-f',"services/$service/Dockerfile",'-t',"observa-check-$service`:runtime",'.')
        }
    }
}

function Test-Audit {
    Test-Tool npm
    Test-Tool docker
    foreach ($directory in @('packages/node','probes/node','services/payment','services/inventory')) {
        Push-Location $directory
        try { Invoke-Step "$directory production dependency audit" { Invoke-Native npm @('audit','--omit=dev','--audit-level=high') } }
        finally { Pop-Location }
    }
    $pythonImage = 'python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c'
    foreach ($requirements in @('services/order/requirements.txt','services/notification/requirements.txt','probes/python/requirements.txt')) {
        Invoke-Step "$requirements dependency audit" {
            Invoke-Native docker @('run','--rm','--mount',"type=bind,source=$root,target=/src,readonly",'-w','/src',$pythonImage,
                'sh','-c',"python -m pip install --quiet 'pip-audit==2.9.0' && python -m pip_audit -r $requirements")
        }
    }
}

function Test-Secrets {
    Test-Tool git
    $gitleaks = if (Get-Command gitleaks -ErrorAction SilentlyContinue) { 'gitleaks' }
        elseif (Test-Path '.tools/gitleaks/gitleaks.exe') { (Resolve-Path '.tools/gitleaks/gitleaks.exe').Path }
        else { throw 'Required tool missing: gitleaks (install it or place it in .tools/gitleaks).' }
    Invoke-Step 'Git history secret scan' { Invoke-Native $gitleaks @('git','--no-banner','--redact','--exit-code','1','--log-opts','--all','.') }
    $snapshot = Join-Path $root ('.local/check-source-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory $snapshot -Force | Out-Null
    try {
        $files = @(& git ls-files --cached --others --exclude-standard)
        if ($LASTEXITCODE -ne 0 -or $files.Count -eq 0) { throw 'Unable to list source files for secret scan' }
        foreach ($file in $files) {
            $destination = Join-Path $snapshot $file
            New-Item -ItemType Directory (Split-Path $destination -Parent) -Force | Out-Null
            Copy-Item -LiteralPath $file -Destination $destination
        }
        Invoke-Step 'current source secret scan' { Invoke-Native $gitleaks @('dir','--no-banner','--redact','--exit-code','1',$snapshot) }
    } finally { Remove-Item -LiteralPath $snapshot -Recurse -Force }
}

function Test-Manifests {
    Test-Tool kubectl
    Invoke-Step 'Kustomize render' {
        $rendered = & kubectl kustomize infra/kubernetes
        if ($LASTEXITCODE -ne 0 -or -not $rendered) { throw 'Kustomize render failed or returned no resources' }
        Write-Host "Rendered $(@($rendered | Where-Object { $_ -eq '---' }).Count + 1) base resources."
    }
    $snapshot = Join-Path $root ('.local/check-manifests-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory $snapshot -Force | Out-Null
    try {
        $mvpRoot = Join-Path $root 'infra/mvp'
        $files = @(Get-ChildItem $mvpRoot -File -Recurse | Where-Object { $_.FullName -notmatch '[/\\]__pycache__[/\\]' -and $_.Extension -ne '.pyc' })
        $resources = @()
        foreach ($file in $files) {
            $relative = [IO.Path]::GetRelativePath($mvpRoot, $file.FullName).Replace('\','/')
            $destination = Join-Path $snapshot $relative
            New-Item -ItemType Directory (Split-Path $destination -Parent) -Force | Out-Null
            Copy-Item -LiteralPath $file.FullName -Destination $destination
            if ($relative -notmatch '/' -and $file.Extension -eq '.yaml' -and $file.Name -ne 'kustomization.yaml') {
                $resources += '  - ' + $relative
            }
        }
        $resources += @(Get-ChildItem $mvpRoot -Filter 'kustomization.yaml' -File -Recurse |
            Where-Object { $_.DirectoryName -ne $mvpRoot } |
            ForEach-Object { '  - ' + [IO.Path]::GetRelativePath($mvpRoot, $_.DirectoryName).Replace('\','/') })
        @('apiVersion: kustomize.config.k8s.io/v1beta1','kind: Kustomization','resources:') + $resources |
            Set-Content -LiteralPath (Join-Path $snapshot 'kustomization.yaml') -Encoding utf8
        Invoke-Step 'MVP manifests render' {
            $rendered = & kubectl kustomize $snapshot
            if ($LASTEXITCODE -ne 0 -or -not $rendered) { throw 'MVP manifests render failed or returned no resources' }
            Write-Host "Rendered $(@($rendered | Where-Object { $_ -eq '---' }).Count + 1) MVP resources."
        }
    } finally { Remove-Item -LiteralPath $snapshot -Recurse -Force }
}

function Test-Rules {
    Test-Tool docker
    $images = (Get-Content -LiteralPath 'infra/images.lock.json' -Raw | ConvertFrom-Json).images
    $prometheus = @($images | Where-Object { $_.image -like 'prom/prometheus:*' })
    if ($prometheus.Count -ne 1 -or $prometheus[0].digest -notmatch '^sha256:[0-9a-f]{64}$') {
        throw 'Expected exactly one digest-pinned Prometheus image in infra/images.lock.json'
    }
    $image = "$($prometheus[0].image)@$($prometheus[0].digest)"
    $mount = "type=bind,source=$root,target=/src,readonly"
    foreach ($arguments in @(
        @('check','rules','infra/kubernetes/config/operations-rules.yaml'),
        @('test','rules','tests/operations/operations-rules.test.yaml')
    )) {
        Invoke-Step "promtool $($arguments -join ' ')" {
            Invoke-Native docker (@('run','--rm','--mount',$mount,'-w','/src','--entrypoint','/bin/promtool',$image) + $arguments)
        }
    }
}

$actions = [ordered]@{ node = ${function:Test-Node}; python = ${function:Test-Python}; 'relay-db' = ${function:Test-RelayDatabase}; images = ${function:Test-Images}; audit = ${function:Test-Audit}; secrets = ${function:Test-Secrets}; manifests = ${function:Test-Manifests}; rules = ${function:Test-Rules} }
foreach ($name in $actions.Keys) {
    if ($Only -eq 'all' -or $Only -eq $name) { & $actions[$name] }
}
Write-Host "Checks passed: $Only"
