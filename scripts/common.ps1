Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:Root = Split-Path $PSScriptRoot -Parent
$script:Local = Join-Path $script:Root '.local'
$script:Profile = 'observa-spike0'
$script:Namespace = 'observa-spike0'
$script:Minikube = Join-Path $script:Root '.tools/minikube.exe'

function Invoke-Checked {
    param([string]$File, [string[]]$Arguments, [int]$Seconds = 300)
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $File
    $start.WorkingDirectory = $script:Root
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit($Seconds * 1000)) {
        $process.Kill($true)
        throw "$File timed out after ${Seconds}s. Inspect .local/evidence and cluster status."
    }
    $output = $stdout.GetAwaiter().GetResult()
    $errors = $stderr.GetAwaiter().GetResult()
    if ($process.ExitCode -ne 0) { throw "$File exited $($process.ExitCode): $errors $output" }
    if ($errors) { Write-Verbose $errors }
    return $output
}

function Invoke-Kube {
    param([string[]]$Arguments, [int]$Seconds = 300)
    Invoke-Checked 'kubectl' (@('--kubeconfig',(Join-Path $script:Local 'kubeconfig'),'--context', $script:Profile, '--namespace', $script:Namespace) + $Arguments) $Seconds
}

function Assert-DeploymentReady {
    param([string]$Name)
    $deployment = Invoke-Kube @('get', "deployment/$Name", '-o', 'json') 30 | ConvertFrom-Json
    $desired = [int]$deployment.spec.replicas
    $ready = [int]$deployment.status.readyReplicas
    if ($desired -lt 1 -or $ready -ne $desired -or [int]$deployment.status.updatedReplicas -ne $desired) {
        throw "$Name deployment is not fully ready: desired=$desired updated=$($deployment.status.updatedReplicas) ready=$ready"
    }
}

function Save-Json {
    param($Value, [string]$Path)
    $Value | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $Path -Encoding utf8
}

function Get-OrderFlowSnapshot {
    $base = 'D:/Work/OrderFlow'
    $gitArgs = @('-c', "safe.directory=$base", '-C', $base)
    $paths = (Invoke-Checked git ($gitArgs + @('ls-files','--modified','--others','--exclude-standard'))).Trim() -split '\r?\n'
    $hashes = @($paths | Where-Object { $_ } | Sort-Object -Unique | ForEach-Object {
        $path = Join-Path $base $_
        [ordered]@{path=$_; sha256=if(Test-Path -LiteralPath $path){(Get-FileHash -LiteralPath $path).Hash}else{'MISSING'}}
    })
    [ordered]@{
        head=(Invoke-Checked git ($gitArgs + @('rev-parse','HEAD'))).Trim()
        status=(Invoke-Checked git ($gitArgs + @('status','--porcelain'))).TrimEnd()
        diff=(Invoke-Checked git ($gitArgs + @('diff','--binary','HEAD'))).TrimEnd()
        files=$hashes
    }
}

function Save-ClusterDiagnostics {
    param([string]$Directory)
    foreach ($entry in @(@('pods','get','pods','-o','json'), @('events','get','events','-o','json'), @('resources','top','pods','--containers'))) {
        try { Invoke-Kube $entry[1..($entry.Count-1)] 30 | Set-Content (Join-Path $Directory "$($entry[0]).txt") }
        catch { $_.Exception.Message | Set-Content (Join-Path $Directory "$($entry[0])-error.txt") }
    }
}

function Wait-Http {
    param([string]$Uri, [int]$Seconds=120)
    $deadline=[DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try { return Invoke-RestMethod -Uri $Uri -TimeoutSec 5 }
        catch { Start-Sleep -Seconds 2 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Endpoint not ready after ${Seconds}s: $Uri"
}

function Start-LocalForward {
    param([string]$Service, [int]$LocalPort, [int]$RemotePort)
    $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$LocalPort)
    try {$listener.Start()} catch {throw "Local port $LocalPort is occupied; refusing to query another listener."} finally {$listener.Stop()}
    $start=[Diagnostics.ProcessStartInfo]::new('kubectl')
    $start.UseShellExecute=$false
    $start.CreateNoWindow=$true
    $start.RedirectStandardOutput=$true
    $start.RedirectStandardError=$true
    foreach($arg in @('--kubeconfig',(Join-Path $script:Local 'kubeconfig'),'--context',$script:Profile,'-n',$script:Namespace,'port-forward',"service/$Service","${LocalPort}:${RemotePort}",'--address','127.0.0.1')) {$start.ArgumentList.Add($arg)}
    $proc=[Diagnostics.Process]::Start($start)
    $errors=$proc.StandardError.ReadToEndAsync()
    $ready=$proc.StandardOutput.ReadLineAsync()
    if(-not $ready.Wait(30000) -or $ready.Result -notlike 'Forwarding from 127.0.0.1:*'){
        if(-not $proc.HasExited){$proc.Kill($true)}
        throw "Port-forward for $Service did not bind successfully: $($errors.GetAwaiter().GetResult())"
    }
    $null=$proc.StandardOutput.ReadToEndAsync()
    return $proc
}
