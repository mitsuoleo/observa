#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('up','status','contract','demo','recovery','down')][string]$Action,
    [ValidateRange(60,3600)][int]$TimeoutSeconds=900
)
. "$PSScriptRoot/common.ps1"
$runDirectory=Join-Path $script:Local ('evidence/mvp-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')+'-'+$Action)
New-Item -ItemType Directory -Force $runDirectory | Out-Null

function Ensure-PostgresSecret {
    $path=Join-Path $script:Local 'observa-postgres-secret.json'
    if(-not(Test-Path $path)){
        $password=[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24)).ToLowerInvariant()
        $base="postgresql://observa:${password}@postgres:5432"
        $secret=@{
            apiVersion='v1';kind='Secret';type='Opaque'
            metadata=@{name='observa-postgres';namespace=$script:Namespace}
            stringData=@{
                password=$password
                'orders-url'="postgresql+psycopg://observa:${password}@postgres:5432/orders"
                'payments-url'="$base/payments"
                'inventory-url'="$base/inventory"
                'notifications-url'="$base/notifications"
                'harness-url'="$base/harness"
            }
        }
        Save-Json $secret $path
    }
    Invoke-Kube @('apply','-f',$path) 60 | Write-Host
}

function Build-Applications {
    foreach($service in @('order','payment','inventory','notification')){
        $tag="observa-${service}:dev"
        Invoke-Checked docker @('build','--target','runtime','-f',"services/$service/Dockerfile",'-t',$tag,'.') $TimeoutSeconds |
            Set-Content (Join-Path $runDirectory "build-$service.log")
        $prior=$env:MINIKUBE_HOME
        try {
            $env:MINIKUBE_HOME=Join-Path $script:Local 'minikube'
            Invoke-Checked $script:Minikube @('-p',$script:Profile,'image','load',$tag) $TimeoutSeconds | Write-Host
        } finally {$env:MINIKUBE_HOME=$prior}
    }
}

function Invoke-Up {
    & "$PSScriptRoot/spike.ps1" up -TimeoutSeconds $TimeoutSeconds
    Invoke-Kube @('delete','job/kafka-domain-topics','--ignore-not-found=true','--wait=true') 90 | Out-Null
    Invoke-Kube @('apply','-f','infra/mvp/topics.yaml') 60 | Write-Host
    Invoke-Kube @('wait','--for=condition=complete','job/kafka-domain-topics',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    Ensure-PostgresSecret
    Invoke-Kube @('apply','-f','infra/mvp/postgres.yaml') 60 | Write-Host
    Invoke-Kube @('rollout','status','statefulset/postgres',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    Build-Applications
    Invoke-Kube @('apply','-f','infra/mvp/apps.yaml') 60 | Write-Host
    foreach($service in @('order','payment','inventory','notification')){
        $tag="observa-${service}:dev"
        $imageId=(Invoke-Checked docker @('image','inspect',$tag,'--format','{{.Id}}') 30).Trim()
        $patchPath=Join-Path $runDirectory "image-$service.json"
        Save-Json @{spec=@{template=@{metadata=@{annotations=@{'observa.dev/image-id'=$imageId}}}}} $patchPath
        Invoke-Kube @('patch',"deployment/$service",'--type=merge','--patch-file',$patchPath) 60 | Write-Host
        Invoke-Kube @('rollout','status',"deployment/$service","--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    }
    Invoke-Kube @('scale','deployment/probe-node','deployment/probe-python-sink','--replicas=0') 60 | Write-Host
    Write-Host "MVP ready. Evidence: $runDirectory"
}

function Invoke-Status {
    Invoke-Kube @('get','pods,services,pvc','-o','wide') 30 | Write-Host
    foreach($name in @('order','payment','inventory','notification','prometheus','tempo','loki','grafana')){
        Assert-DeploymentReady $name
    }
    foreach($name in @('postgres','kafka')){
        $ready=(Invoke-Kube @('get',"statefulset/$name",'-o','jsonpath={.status.readyReplicas}') 30).Trim()
        if($ready -ne '1'){throw "$name is not ready"}
    }
    $collector=Invoke-Kube @('get','daemonset/collector','-o','json') 30 | ConvertFrom-Json
    if($collector.status.desiredNumberScheduled -lt 1 -or
       $collector.status.numberReady -ne $collector.status.desiredNumberScheduled){
        throw 'collector is not ready'
    }
}

function Invoke-Contract {
    foreach($runtime in @('python','node')){
        $tag="observa-messaging-$runtime-test:us002"
        Invoke-Checked docker @('build','-f',"packages/$runtime/Dockerfile",'-t',$tag,'.') $TimeoutSeconds |
            Set-Content (Join-Path $runDirectory "build-contract-$runtime.log")
        $prior=$env:MINIKUBE_HOME
        try {
            $env:MINIKUBE_HOME=Join-Path $script:Local 'minikube'
            Invoke-Checked $script:Minikube @('-p',$script:Profile,'image','load',$tag) $TimeoutSeconds | Out-Null
        } finally {$env:MINIKUBE_HOME=$prior}
        $job=if($runtime -eq 'python'){'observa-us002-contract'}else{'observa-us002-node-contract'}
        Invoke-Kube @('delete',"job/$job",'--ignore-not-found=true','--wait=true') 90 | Out-Null
        $manifest=if($runtime -eq 'python'){'contract-test.yaml'}else{'node-contract-test.yaml'}
        Invoke-Kube @('apply','-f',"infra/mvp/$manifest") 60 | Write-Host
        $jobTimeout=[Math]::Min($TimeoutSeconds,120)
        try {
            Invoke-Kube @('wait','--for=condition=complete',"job/$job","--timeout=${jobTimeout}s") ($jobTimeout+10) | Write-Host
        } finally {
            Invoke-Kube @('logs',"job/$job") 60 | Set-Content (Join-Path $runDirectory "contract-$runtime.log")
        }
    }
    $tag='observa-harness-test:us002'
    Invoke-Checked docker @('build','-f','harness/Dockerfile','-t',$tag,'.') $TimeoutSeconds |
        Set-Content (Join-Path $runDirectory 'build-contract-postgres.log')
    $prior=$env:MINIKUBE_HOME
    try {
        $env:MINIKUBE_HOME=Join-Path $script:Local 'minikube'
        Invoke-Checked $script:Minikube @('-p',$script:Profile,'image','load',$tag) $TimeoutSeconds | Out-Null
    } finally {$env:MINIKUBE_HOME=$prior}
    $job='observa-us002-postgres-harness'
    Invoke-Kube @('delete',"job/$job",'--ignore-not-found=true','--wait=true') 90 | Out-Null
    Invoke-Kube @('apply','-f','infra/mvp/harness-test.yaml') 60 | Write-Host
    $jobTimeout=[Math]::Min($TimeoutSeconds,120)
    try {
        Invoke-Kube @('wait','--for=condition=complete',"job/$job","--timeout=${jobTimeout}s") ($jobTimeout+10) | Write-Host
    } finally {
        Invoke-Kube @('logs',"job/$job") 60 | Set-Content (Join-Path $runDirectory 'contract-postgres.log')
    }
    Write-Host "Python and Node Kafka contracts and Postgres harness passed. Evidence: $runDirectory"
}

function Invoke-Demo {
    Invoke-Status
    $forward=Start-LocalForward 'order' 13300 8000
    try {
        $scenarios=@(
            @{name='happy';payment='approve';stock='reserve';expected='CONFIRMED'},
            @{name='payment_rejected';payment='reject';stock='reserve';expected='FAILED'},
            @{name='stock_unavailable';payment='approve';stock='unavailable';expected='CANCELLED'}
        )
        $results=@()
        foreach($scenario in $scenarios){
            $body=@{
                customer_id=[Guid]::NewGuid().ToString()
                items=@(@{product_id='11111111-1111-1111-1111-111111111111';quantity=1;unit_price=49.90})
                simulate=@{payment=$scenario.payment;stock=$scenario.stock}
            } | ConvertTo-Json -Depth 8
            $order=Invoke-RestMethod -Uri 'http://127.0.0.1:13300/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 15
            $deadline=[DateTime]::UtcNow.AddSeconds(120)
            do {
                Start-Sleep -Seconds 2
                $current=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$($order.id)" -TimeoutSec 10
            } while($current.status -ne $scenario.expected -and [DateTime]::UtcNow -lt $deadline)
            if($current.status -ne $scenario.expected){throw "Scenario $($scenario.name) ended at $($current.status), expected $($scenario.expected)"}
            $timeline=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$($order.id)/timeline" -TimeoutSec 10
            $results+=@{name=$scenario.name;order_id=$order.id;final_status=$current.status;timeline=@($timeline)}
        }
        Save-Json $results (Join-Path $runDirectory 'scenarios.json')
        Write-Host "Three scenarios passed. Evidence: $runDirectory"
    } finally {
        if($forward -and -not $forward.HasExited){$forward.Kill($true);$forward.WaitForExit()}
    }
}

function Invoke-Recovery {
    Invoke-Status | Out-Null
    $pods=Invoke-Kube @('get','pods','-l','app=inventory','-o','json') 30 | ConvertFrom-Json
    if(@($pods.items).Count -ne 1){throw 'Recovery requires exactly one Inventory pod'}
    $oldPod=$pods.items[0]
    $forward=Start-LocalForward 'order' 13300 8000
    try {
        $body=@{
            customer_id=[Guid]::NewGuid().ToString()
            items=@(@{product_id='11111111-1111-1111-1111-111111111111';quantity=1;unit_price=49.90})
            simulate=@{payment='approve';stock='reserve'}
        } | ConvertTo-Json -Depth 8
        $order=Invoke-RestMethod -Uri 'http://127.0.0.1:13300/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 15
        $orderId=([Guid]::Parse($order.id)).ToString()
        Invoke-Kube @('delete','pod',$oldPod.metadata.name,'--wait=false') 30 | Write-Host
        $deadline=[DateTime]::UtcNow.AddSeconds(180)
        do {
            Start-Sleep -Seconds 2
            $currentPods=Invoke-Kube @('get','pods','-l','app=inventory','-o','json') 30 | ConvertFrom-Json
            $replacement=@($currentPods.items | Where-Object {
                $_.metadata.uid -ne $oldPod.metadata.uid -and
                @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
            })
        } while($replacement.Count -ne 1 -and [DateTime]::UtcNow -lt $deadline)
        if($replacement.Count -ne 1){throw 'Inventory pod was not recreated ready'}
        do {
            Start-Sleep -Seconds 2
            $current=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$orderId" -TimeoutSec 10
        } while($current.status -ne 'CONFIRMED' -and [DateTime]::UtcNow -lt $deadline)
        if($current.status -ne 'CONFIRMED'){throw "Recovered order ended at $($current.status)"}
        $timeline=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$orderId/timeline" -TimeoutSec 10
        foreach($kind in @('stock.reserved','order.completed')){
            if(@($timeline | Where-Object { $_.event_type -eq $kind }).Count -ne 1){throw "$kind was duplicated or missing"}
        }
        $reservations=(Invoke-Kube @('exec','postgres-0','--','psql','-U','observa','-d','inventory','-tAc',"select count(*) from reservations where order_id='$orderId'") 30).Trim()
        if($reservations -ne '1'){throw "Expected one reservation after recovery; found $reservations"}
        Save-Json @{order_id=$orderId;final_status=$current.status;old_pod=$oldPod.metadata.name;new_pod=$replacement[0].metadata.name;reservations=[int]$reservations;timeline=$timeline} (Join-Path $runDirectory 'recovery.json')
        Write-Host "Inventory pod recreated; order confirmed once. Evidence: $runDirectory"
    } finally {
        if($forward -and -not $forward.HasExited){$forward.Kill($true);$forward.WaitForExit()}
    }
}

$before=Get-OrderFlowSnapshot
try {
    switch($Action){
        up {Invoke-Up}
        status {Invoke-Status}
        contract {Invoke-Contract}
        demo {Invoke-Demo}
        recovery {Invoke-Recovery}
        down {& "$PSScriptRoot/spike.ps1" down -TimeoutSeconds $TimeoutSeconds}
    }
} finally {
    $after=Get-OrderFlowSnapshot
    Save-Json @{unchanged=(($before | ConvertTo-Json -Depth 40 -Compress) -ceq ($after | ConvertTo-Json -Depth 40 -Compress))} (Join-Path $runDirectory 'orderflow-comparison.json')
    if(($before | ConvertTo-Json -Depth 40 -Compress) -cne ($after | ConvertTo-Json -Depth 40 -Compress)){
        throw 'OrderFlow changed during MVP command; investigate without restoring user files.'
    }
}
