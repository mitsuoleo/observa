#requires -Version 7.0
[CmdletBinding()]
param([ValidateRange(60,600)][int]$TimeoutSeconds=300)
. "$PSScriptRoot/common.ps1"

$directory=Join-Path $script:Local ('evidence/gateway-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff'))
New-Item -ItemType Directory -Force $directory | Out-Null
$keys=@('PAYMENT_GATEWAY_FAIL_FIRST','PAYMENT_GATEWAY_MAX_RETRIES',
    'PAYMENT_GATEWAY_FAILURE_THRESHOLD','PAYMENT_GATEWAY_BACKOFF_MS')
$deployment=Invoke-Kube @('get','deployment/payment','-o','json') 30 | ConvertFrom-Json
$prior=@{}
foreach($item in @($deployment.spec.template.spec.containers[0].env)){
    if($item.name -in $keys){
        if($item.valueFrom){throw "Cannot safely replace $($item.name) sourced from a Secret or ConfigMap"}
        $prior[$item.name]=[string]$item.value
    }
}
$result=[ordered]@{started=[DateTimeOffset]::UtcNow.ToString('o');passed=$false;
    fault='First two synthetic gateway attempts fail; bounded retries recover with backoff.'}
$changed=$false
$forward=$null
try {
    Invoke-Kube @('set','env','deployment/payment','PAYMENT_GATEWAY_FAIL_FIRST=2',
        'PAYMENT_GATEWAY_MAX_RETRIES=2','PAYMENT_GATEWAY_FAILURE_THRESHOLD=3',
        'PAYMENT_GATEWAY_BACKOFF_MS=50') 60 | Out-Null
    $changed=$true
    Invoke-Kube @('rollout','status','deployment/payment',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Out-Null
    $priorDemos=@(Get-ChildItem -LiteralPath (Join-Path $script:Local 'evidence') -Directory -Filter 'mvp-*-demo' |
        ForEach-Object {$_.FullName})
    & "$PSScriptRoot/mvp.ps1" demo -TimeoutSeconds $TimeoutSeconds | Out-Null
    $newDemos=@(Get-ChildItem -LiteralPath (Join-Path $script:Local 'evidence') -Directory -Filter 'mvp-*-demo' |
        Where-Object {$_.FullName -notin $priorDemos})
    if($newDemos.Count -ne 1){throw 'Expected one demo evidence directory'}
    $scenarios=Get-Content -LiteralPath (Join-Path $newDemos[0].FullName 'scenarios.json') -Raw | ConvertFrom-Json
    $forward=Start-LocalForward 'payment' 13301 8000
    $metrics=(Invoke-WebRequest 'http://127.0.0.1:13301/metrics' -TimeoutSec 20).Content
    $metrics | Set-Content (Join-Path $directory 'gateway-metrics.prom')
    $failures=[regex]::Match($metrics,'(?m)^payment_gateway_failures_total (\d+)$')
    $attempts=[regex]::Match($metrics,'(?m)^payment_gateway_attempts_total (\d+)$')
    $open=[regex]::Match($metrics,'(?m)^payment_gateway_circuit_open (\d+)$')
    if(-not $failures.Success -or -not $attempts.Success -or -not $open.Success -or
        [int]$failures.Groups[1].Value -ne 2 -or [int]$attempts.Groups[1].Value -lt 5 -or
        [int]$open.Groups[1].Value -ne 0){throw 'Gateway retry/circuit metrics did not match the injected two-failure scenario'}
    $effects=@(foreach($scenario in $scenarios){
        $id=([guid]::Parse($scenario.order_id)).ToString()
        $count=(Invoke-Kube @('exec','postgres-0','--','psql','-U','observa','-d','payments','-tAc',
            "select count(*) from payments where order_id='$id'") 30).Trim()
        if($count -ne '1'){throw "Expected one payment effect for $id, found $count"}
        @{order_id=$id;payments=[int]$count;final_status=$scenario.final_status}
    })
    $result.metrics=@{attempts=[int]$attempts.Groups[1].Value;failures=[int]$failures.Groups[1].Value;circuit_open=0}
    $result.effects=$effects
    $result.passed=$true
    Write-Host "Gateway retry verified in cluster. Evidence: $directory"
} catch {
    $result.error=$_.Exception.Message
    throw
} finally {
    if($forward){if(-not $forward.HasExited){$forward.Kill($true);$forward.WaitForExit()};$forward.Dispose()}
    if($changed){
        $restore=@('set','env','deployment/payment')
        foreach($key in $keys){$restore+=if($prior.ContainsKey($key)){"$key=$($prior[$key])"}else{"$key-"}}
        Invoke-Kube $restore 60 | Out-Null
        Invoke-Kube @('rollout','status','deployment/payment',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Out-Null
    }
    $result.completed=[DateTimeOffset]::UtcNow.ToString('o')
    Save-Json $result (Join-Path $directory 'result.json')
}
