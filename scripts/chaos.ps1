#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('payment','kafka')][string]$Target,
    [ValidateRange(60,900)][int]$TimeoutSeconds=300
)
. "$PSScriptRoot/common.ps1"

$directory=Join-Path $script:Local ('evidence/chaos-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')+'-'+$Target)
New-Item -ItemType Directory -Force $directory | Out-Null
$hypothesis=if($Target -eq 'kafka'){
    'The single Kafka pod is recreated from its PVC; a new order can complete after readiness returns.'
} else {
    'The Payment pod is recreated; a new order completes without duplicate logical payment effects.'
}
$result=[ordered]@{target=$Target;hypothesis=$hypothesis;blast_radius='observa-spike0 namespace only';
    stop_condition="Deployment/StatefulSet not ready within ${TimeoutSeconds}s or demo fails";passed=$false;
    started=[DateTimeOffset]::UtcNow.ToString('o')}

try {
    & "$PSScriptRoot/mvp.ps1" status -TimeoutSeconds $TimeoutSeconds | Set-Content (Join-Path $directory 'status-before.txt')
    $pods=Invoke-Kube @('get','pods','-l',"app=$Target",'-o','json') 30 | ConvertFrom-Json
    if(@($pods.items).Count -ne 1){throw "Experiment requires exactly one $Target pod"}
    $old=$pods.items[0]
    $result.old_pod=$old.metadata.name
    $result.old_uid=$old.metadata.uid
    Invoke-Kube @('delete','pod',$old.metadata.name,'--wait=false') 30 | Set-Content (Join-Path $directory 'delete.txt')
    $owner=if($Target -eq 'kafka'){'statefulset/kafka'}else{'deployment/payment'}
    Invoke-Kube @('rollout','status',$owner,"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) |
        Set-Content (Join-Path $directory 'rollout.txt')
    $deadline=[DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $replacement=Invoke-Kube @('get','pods','-l',"app=$Target",'-o','json') 30 | ConvertFrom-Json
        $new=@($replacement.items | Where-Object {
            $_.metadata.uid -ne $old.metadata.uid -and
            @($_.status.conditions | Where-Object {$_.type -eq 'Ready' -and $_.status -eq 'True'}).Count -eq 1
        })
        if($new.Count -eq 1){break}
        Start-Sleep -Seconds 2
    } while([DateTime]::UtcNow -lt $deadline)
    if($new.Count -ne 1){throw "No ready replacement $Target pod observed"}
    $result.new_pod=$new[0].metadata.name
    $result.new_uid=$new[0].metadata.uid
    $priorDemos=@(Get-ChildItem -LiteralPath (Join-Path $script:Local 'evidence') -Directory -Filter 'mvp-*-demo' |
        ForEach-Object {$_.FullName})
    & "$PSScriptRoot/mvp.ps1" demo -TimeoutSeconds $TimeoutSeconds |
        Set-Content (Join-Path $directory 'demo.txt')
    $newDemos=@(Get-ChildItem -LiteralPath (Join-Path $script:Local 'evidence') -Directory -Filter 'mvp-*-demo' |
        Where-Object {$_.FullName -notin $priorDemos})
    if($newDemos.Count -ne 1){throw "Expected one new demo evidence directory; found $($newDemos.Count)"}
    $demoDirectory=$newDemos[0].FullName
    $scenarios=Get-Content -LiteralPath (Join-Path $demoDirectory 'scenarios.json') -Raw | ConvertFrom-Json
    $effects=@(foreach($scenario in $scenarios){
        $id=([guid]::Parse($scenario.order_id)).ToString()
        $count=(Invoke-Kube @('exec','postgres-0','--','psql','-U','observa','-d','payments','-tAc',
            "select count(*) from payments where order_id='$id'") 30).Trim()
        if($count -ne '1'){throw "Payment effect count for $id was $count, expected 1"}
        @{order_id=$id;payments=[int]$count;final_status=$scenario.final_status}
    })
    $result.effects=$effects
    & "$PSScriptRoot/mvp.ps1" status -TimeoutSeconds $TimeoutSeconds | Set-Content (Join-Path $directory 'status-after.txt')
    $result.passed=$true
    Write-Host "Recovery experiment passed. Evidence: $directory"
} catch {
    $result.error=$_.Exception.Message
    throw
} finally {
    $result.completed=[DateTimeOffset]::UtcNow.ToString('o')
    Save-Json $result (Join-Path $directory 'result.json')
    Save-ClusterDiagnostics $directory
}
