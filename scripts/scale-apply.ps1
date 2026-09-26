#requires -Version 7.0
[CmdletBinding()]
param([switch]$RelayConcurrencyVerified)
. "$PSScriptRoot/common.ps1"

if (-not $RelayConcurrencyVerified) {
    throw 'Run the US-101 two-replica relay and redelivery checks first, then pass -RelayConcurrencyVerified.'
}
foreach ($name in @('order', 'payment', 'inventory', 'notification')) {
    $ready = (Invoke-Kube @('get', "deployment/$name", '-o', 'jsonpath={.status.readyReplicas}') 30).Trim()
    if ($ready -ne '1') { throw "Expected one ready $name replica before enabling HPA; found $ready." }
}
& "$PSScriptRoot/install-keda.ps1"
foreach ($name in @('order', 'payment', 'inventory', 'notification')) {
    $existing=(Invoke-Kube @('get',"hpa/$name",'-o','json','--ignore-not-found') 30).Trim()
    if($existing){
        $hpa=$existing | ConvertFrom-Json
        $refs=if($hpa.metadata.PSObject.Properties['ownerReferences']){@($hpa.metadata.ownerReferences)}else{@()}
        $owner=@($refs | Where-Object { $_.kind -eq 'ScaledObject' })
        if(-not $owner.Count){Invoke-Kube @('delete',"hpa/$name") 30 | Write-Host}
    }
}
Invoke-Kube @('apply', '-k', 'infra/mvp/scaling') 60 | Write-Host
Invoke-Kube @('rollout', 'status', 'deployment/lag-exporter', '--timeout=120s') 130 | Write-Host
foreach ($name in @('order', 'payment', 'inventory', 'notification')) {
    $deadline=[DateTime]::UtcNow.AddSeconds(120)
    do {
        $hpa=(Invoke-Kube @('get',"hpa/$name",'-o','json','--ignore-not-found') 30).Trim()
        if($hpa){break}
        Start-Sleep -Seconds 3
    } while([DateTime]::UtcNow -lt $deadline)
    if(-not $hpa){throw "KEDA did not create HPA/$name"}
    $object=$hpa | ConvertFrom-Json
    if(@($object.metadata.ownerReferences | Where-Object { $_.kind -eq 'ScaledObject' }).Count -ne 1){
        throw "HPA/$name is not owned by a KEDA ScaledObject"
    }
    if(@($object.spec.metrics | Where-Object { $_.type -eq 'External' }).Count -ne 1){
        throw "HPA/$name has no Kafka external lag metric"
    }
}
$forward = Start-LocalForward 'lag-exporter' 19108 9108
try {
    $metrics = (Invoke-WebRequest 'http://127.0.0.1:19108/metrics' -TimeoutSec 30).Content
    if ($metrics -notmatch 'observa_lag_exporter_scrape_success 1') {
        throw 'Lag exporter cannot query Kafka; inspect deployment/lag-exporter logs.'
    }
    foreach ($group in @('observa.order', 'observa.payment', 'observa.inventory', 'observa.notification')) {
        $escaped = [regex]::Escape('group="' + $group + '"')
        if ($metrics -notmatch $escaped) { throw "Missing lag metric for $group; run the MVP demo to establish committed offsets." }
    }
    Write-Host 'KEDA HPAs combine CPU and Kafka lag for all domain consumer groups.'
} finally {
    if (-not $forward.HasExited) { $forward.Kill($true); $forward.WaitForExit() }
    $forward.Dispose()
}
