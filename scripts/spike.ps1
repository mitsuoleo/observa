#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('preflight','up','status','test','demo','down')][string]$Action,
    [ValidateRange(60,3600)][int]$TimeoutSeconds=600,
    [switch]$UnitOnly
)
. "$PSScriptRoot/common.ps1"
New-Item -ItemType Directory -Force "$script:Local/evidence" | Out-Null
$previousKubeconfig=$env:KUBECONFIG
$previousMinikubeHome=$env:MINIKUBE_HOME
$env:KUBECONFIG=Join-Path $script:Local 'kubeconfig'
$env:MINIKUBE_HOME=Join-Path $script:Local 'minikube'
$runDirectory=Join-Path $script:Local ('evidence/'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')+'-'+$Action)
New-Item -ItemType Directory -Force $runDirectory | Out-Null

function Invoke-Preflight {
    $docker=Invoke-Checked docker @('info','--format','{{json .}}') 30 | ConvertFrom-Json
    if($docker.OSType -ne 'linux'){throw 'Docker must use Linux containers.'}
    if($docker.NCPU -lt 4 -or $docker.MemTotal -lt 8GB){throw 'Docker cannot supply approved 4 CPUs / 8 GiB; do not increase the budget silently.'}
    $free=(Get-PSDrive -Name ([IO.Path]::GetPathRoot($script:Root).Substring(0,1))).Free
    if($free -lt 20GB){throw 'At least 20 GiB free disk is required for images and evidence.'}
    $snapshot=Get-OrderFlowSnapshot
    Save-Json $snapshot (Join-Path $runDirectory 'orderflow-preflight.json')
    $baseline=Join-Path $script:Local 'orderflow-baseline.json'
    if(-not(Test-Path $baseline)){Save-Json $snapshot $baseline}
    $versions=[ordered]@{docker=$docker.ServerVersion;cpus=$docker.NCPU;memory_bytes=$docker.MemTotal;disk_free_bytes=$free;budget_cpus=4;budget_memory_mib=8192}
    if(-not(Test-Path $script:Minikube)){throw 'Missing .tools/minikube.exe. Run scripts/install-tools.ps1 (verified local download).'}
    if((Get-FileHash $script:Minikube -Algorithm SHA256).Hash -ne '776386465ded2cf610ae397fe302de32c44d12134b5ce7ce98ab05bd7713b360'){throw 'Local minikube checksum differs from the pinned release.'}
    $versions.minikube=(Invoke-Checked $script:Minikube @('version','--short') 30).Trim()
    $baseImage=((Get-Content (Join-Path $script:Root 'probes/python/Dockerfile') -First 1) -split '=',2)[1]
    $meminfo=Invoke-Checked docker @('run','--rm','--network=none','--read-only','--cap-drop=ALL','--entrypoint','cat',$baseImage,'/proc/meminfo') 120
    if($meminfo -notmatch '(?m)^MemAvailable:\s+(\d+) kB'){throw 'Cannot measure Docker VM available memory.'}
    $available=[long]$Matches[1]*1KB
    $existing=(Invoke-Checked docker @('ps','--filter',"name=^/$script:Profile$",'--format','{{.Names}}') 30).Trim()
    $required=if($existing -eq $script:Profile){512MB}else{8GB}
    if($available -lt $required){throw "Docker VM available memory $available bytes is below required $required bytes."}
    $versions.available_memory_bytes=$available
    foreach($port in @(13000,13100,13200,19090)){
        $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$port)
        try {$listener.Start()} catch {throw "Required local demo port $port is occupied."} finally {$listener.Stop()}
    }
    $images=Get-Content (Join-Path $script:Root 'infra/images.lock.json') -Raw | ConvertFrom-Json
    foreach($image in $images.images){
        $null=Invoke-Checked docker @('manifest','inspect',"$($image.image)@$($image.digest)") 60
    }
    $versions.demo_ports_available=$true
    $versions.upstream_manifests_accessible=$true
    Save-Json $versions (Join-Path $runDirectory 'preflight.json')
    Invoke-Checked docker @('stats','--no-stream','--format','{{json .}}') 30 | Set-Content (Join-Path $runDirectory 'existing-container-resources.jsonl')
    Write-Host 'Preflight passed. Budget: 4 CPUs, 8192 MiB; tools and Docker accessible.'
}

function Build-Probes {
    $clusterUid=(Invoke-Kube @('get','namespace','kube-system','-o','jsonpath={.metadata.uid}') 30).Trim()
    $cachePath=Join-Path $script:Local 'loaded-images.json'
    $loaded=if(Test-Path $cachePath){Get-Content $cachePath -Raw | ConvertFrom-Json -AsHashtable}else{@{}}
    foreach($runtime in @('python','node')){
        Write-Host "Building $runtime probe..."
        Invoke-Checked docker @('build','--target','runtime','-f',"probes/$runtime/Dockerfile",'-t',"observa-probe-${runtime}:spike0",'.') $TimeoutSeconds | Set-Content (Join-Path $runDirectory "build-$runtime.log")
        $imageId=(Invoke-Checked docker @('image','inspect',"observa-probe-${runtime}:spike0",'--format','{{.Id}}') 30).Trim()
        $key="$clusterUid/$runtime"
        if($loaded[$key] -ne $imageId){
            Invoke-Checked $script:Minikube @('-p',$script:Profile,'image','load',"observa-probe-${runtime}:spike0") $TimeoutSeconds | Write-Host
            $loaded[$key]=$imageId
            Save-Json $loaded $cachePath
        }
    }
}

function Invoke-Up {
    Invoke-Preflight
    $running=$false
    try {
        $state=Invoke-Checked $script:Minikube @('status','-p',$script:Profile,'-o','json') 30 | ConvertFrom-Json
        $running=$state.Host -eq 'Running' -and $state.APIServer -eq 'Running' -and $state.Kubelet -eq 'Running'
    } catch {Write-Verbose 'Dedicated cluster is not running yet.'}
    if(-not $running){
        Invoke-Checked $script:Minikube @('start','-p',$script:Profile,'--driver=docker','--cpus=4','--memory=8192','--kubernetes-version=v1.35.0','--keep-context',"--wait-timeout=${TimeoutSeconds}s") $TimeoutSeconds | Write-Host
    }
    $limits=Invoke-Checked docker @('inspect',$script:Profile,'--format','{{json .HostConfig}}') 30 | ConvertFrom-Json
    if($limits.Memory -ne 8GB -or $limits.NanoCpus -ne 4000000000){throw 'Existing profile differs from approved 4 CPU / 8 GiB budget.'}
    Invoke-Checked $script:Minikube @('-p',$script:Profile,'addons','enable','metrics-server') 120 | Write-Host
    Build-Probes
    Invoke-Kube @('apply','-f','infra/kubernetes/namespace.yaml') | Write-Host
    $secretPath=Join-Path $script:Local 'grafana-secret.json'
    if(-not(Test-Path $secretPath)){
        $password=[Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
        Save-Json @{apiVersion='v1';kind='Secret';metadata=@{name='grafana-admin';namespace=$script:Namespace};type='Opaque';stringData=@{'admin-user'='admin';'admin-password'=$password}} $secretPath
    }
    Invoke-Kube @('apply','-f',$secretPath) | Write-Host
    # A failed bootstrap Job cannot become successful by re-applying its manifest.
    $jobs=Invoke-Kube @('get','jobs','-o','json') 30 | ConvertFrom-Json -AsHashtable
    foreach($job in $jobs.items){
        if($job.metadata.name -eq 'kafka-topics' -and $job['status']['failed']){Invoke-Kube @('delete','job/kafka-topics','--wait=true') 60 | Write-Host}
    }
    Invoke-Kube @('apply','-k','infra/kubernetes') | Write-Host
    Invoke-Kube @('rollout','status','statefulset/kafka',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    Invoke-Kube @('wait','--for=condition=complete','job/kafka-topics',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    foreach($runtime in @('node','python')){
        $imageId=(Invoke-Checked docker @('image','inspect',"observa-probe-${runtime}:spike0",'--format','{{.Id}}') 30).Trim()
        $deployment=if($runtime -eq 'node'){'probe-node'}else{'probe-python-sink'}
        $patchPath=Join-Path $runDirectory "image-$runtime.json"
        Save-Json @{spec=@{template=@{metadata=@{annotations=@{'observa.dev/image-id'=$imageId}}}}} $patchPath
        Invoke-Kube @('patch',"deployment/$deployment",'--type=merge','--patch-file',$patchPath) | Write-Host
    }
    foreach($kind in @('deployment','statefulset','daemonset')){
        $items=Invoke-Kube @('get',$kind,'-o','name') 30
        foreach($item in ($items.Trim() -split '\r?\n')){
            if($item){Invoke-Kube @('rollout','status',$item,"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host}
        }
    }
    Invoke-Kube @('wait','--for=condition=complete','job/kafka-topics','--timeout=180s') 190 | Write-Host
    Save-ClusterDiagnostics $runDirectory
}

function Invoke-Tests {
    & "$PSScriptRoot/test-automation.ps1"
    foreach($runtime in @('python','node')){
        Invoke-Checked docker @('build','--target','test','-f',"probes/$runtime/Dockerfile",'-t',"observa-probe-${runtime}-test:spike0",'.') $TimeoutSeconds | Set-Content (Join-Path $runDirectory "test-build-$runtime.log")
        Invoke-Checked docker @('run','--rm',"observa-probe-${runtime}-test:spike0") $TimeoutSeconds | Tee-Object (Join-Path $runDirectory "test-$runtime.log") | Write-Host
    }
    Invoke-Checked docker @('run','--rm','--mount',"type=bind,source=$script:Root,target=/workspace,readonly",'--workdir','/workspace','--entrypoint','python','observa-probe-python:spike0','-m','unittest','discover','-s','tests/evidence','-p','test_*.py') 120 | Write-Host
    if(-not $UnitOnly){
        & "$PSScriptRoot/demo.ps1" -Directory $runDirectory -TimeoutSeconds $TimeoutSeconds
        & "$PSScriptRoot/recovery.ps1" -Directory (Join-Path $runDirectory 'recovery') -TimeoutSeconds $TimeoutSeconds
    }
}

$executionLock=$null
if($Action -ne 'status'){
    try {$executionLock=[IO.File]::Open((Join-Path $script:Local 'execution.lock'),[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)}
    catch {
        $env:KUBECONFIG=$previousKubeconfig
        $env:MINIKUBE_HOME=$previousMinikubeHome
        throw 'Another spike command is running. Wait for it to finish; parallel mutations are not supported.'
    }
}
$sampler=$null
if($Action -in @('up','test','demo')){
    $sampler=Start-Job -ArgumentList $env:KUBECONFIG,$runDirectory -ScriptBlock {
        param($Config,$Out)
        $env:KUBECONFIG=$Config
        for($i=0;$i -lt 360;$i++){
            $record=@{timestamp=[DateTime]::UtcNow.ToString('o')}
            $record.pods=@(& kubectl --context observa-spike0 -n observa-spike0 --request-timeout=5s top pods --containers 2>&1 | ForEach-Object {"$_"})
            $record.node=@(& kubectl --context observa-spike0 --request-timeout=5s top nodes 2>&1 | ForEach-Object {"$_"})
            $record | ConvertTo-Json -Compress | Add-Content (Join-Path $Out 'resource-samples.jsonl')
            Start-Sleep -Seconds 5
        }
    }
}
try {
    Save-Json (Get-OrderFlowSnapshot) (Join-Path $runDirectory 'orderflow-before.json')
    switch($Action){
        preflight {Invoke-Preflight}
        up {Invoke-Up}
        status {
            Invoke-Kube @('get','pods,services,pvc','-o','wide') 30 | Write-Host
            Save-ClusterDiagnostics $runDirectory
            foreach($kind in @('deployments','statefulsets','daemonsets')){
                $workloads=Invoke-Kube @('get',$kind,'-o','json') 30 | ConvertFrom-Json -AsHashtable
                if($workloads.items.Count -eq 0){throw "Spike $kind are absent."}
                foreach($workload in $workloads.items){
                    $desired=if($kind -eq 'daemonsets'){$workload['status']['desiredNumberScheduled']}else{$workload.spec.replicas}
                    $ready=if($kind -eq 'daemonsets'){$workload['status']['numberReady']}else{$workload['status']['readyReplicas']}
                    if($desired -lt 1 -or $ready -lt $desired){throw "$kind/$($workload.metadata.name) is not ready; see diagnostics."}
                }
            }
        }
        test {Invoke-Tests}
        demo {& "$PSScriptRoot/demo.ps1" -Directory $runDirectory -TimeoutSeconds $TimeoutSeconds}
        down {
            if(Test-Path $script:Minikube){Invoke-Checked $script:Minikube @('delete','-p',$script:Profile) $TimeoutSeconds | Write-Host}
        }
    }
    $final=Get-OrderFlowSnapshot
    Save-Json $final (Join-Path $runDirectory 'orderflow-after.json')
    $baseline=Join-Path $runDirectory 'orderflow-before.json'
    if(Test-Path $baseline){
        $before=Get-Content $baseline -Raw | ConvertFrom-Json -AsHashtable
        if(($before | ConvertTo-Json -Depth 20 -Compress) -cne ($final | ConvertTo-Json -Depth 20 -Compress)){throw 'OrderFlow changed during this run. Investigate concurrent edits; do not restore files.'}
    }
    Write-Host "Evidence: $runDirectory"
} catch {
    $_.Exception.Message | Set-Content (Join-Path $runDirectory 'failure.txt')
    if($Action -in @('up','test','demo','status')){Save-ClusterDiagnostics $runDirectory}
    throw
} finally {
    try {Save-Json (Get-OrderFlowSnapshot) (Join-Path $runDirectory 'orderflow-after.json')} catch {Write-Warning 'Could not capture final OrderFlow state.'}
    if($sampler){Stop-Job $sampler; Remove-Job $sampler}
    if($executionLock){$executionLock.Dispose()}
    $env:KUBECONFIG=$previousKubeconfig
    $env:MINIKUBE_HOME=$previousMinikubeHome
}

