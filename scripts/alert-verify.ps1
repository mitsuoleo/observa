#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet('payment','notification')][string]$Target = 'payment',
    [ValidateRange(180,600)][int]$TimeoutSeconds = 300
)
. "$PSScriptRoot/common.ps1"

$directory = Join-Path $script:Local ('evidence/alert-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff'))
New-Item -ItemType Directory -Force -Path $directory | Out-Null
$scaledObject = "$Target-lag"
$forward = $null
$paused = $false
$failure = $null
$result = [ordered]@{
    target = $Target
    started = [DateTimeOffset]::UtcNow.ToString('o')
    alert_fired = $false
    service_restored = $false
    alert_resolved = $false
    passed = $false
}

function Get-Prometheus {
    param([string]$Path)
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:13090$Path" -TimeoutSec 10
    if ($response.status -ne 'success') { throw "Prometheus API failed: $Path" }
    return $response.data
}

function Get-TargetAlert {
    param([string]$Instance)
    $data = Get-Prometheus '/api/v1/alerts'
    return @($data.alerts | Where-Object {
        $_.labels.alertname -eq 'ObservaDomainTargetDown' -and $_.labels.instance -eq $Instance
    })
}

try {
    & "$PSScriptRoot/mvp.ps1" status -TimeoutSeconds 120 | Out-Null
    Invoke-Kube @('get','pods','-o','wide') 30 |
        Set-Content (Join-Path $directory 'pods-before.txt')
    $objectJson = (Invoke-Kube @('get',"scaledobject/$scaledObject",'-o','json','--ignore-not-found') 30).Trim()
    if (-not $objectJson) {
        throw "KEDA $scaledObject is absent; run check.ps1 -Only relay-db, then scale-apply.ps1 -RelayConcurrencyVerified"
    }
    $object = $objectJson | ConvertFrom-Json
    $annotations = $object.metadata.annotations
    if ($annotations -and @($annotations.PSObject.Properties.Name | Where-Object {
        $_ -in @('autoscaling.keda.sh/paused','autoscaling.keda.sh/paused-replicas')
    }).Count) { throw "$scaledObject is already paused; refusing to change it" }
    $deployment = Invoke-Kube @('get',"deployment/$Target",'-o','json') 30 | ConvertFrom-Json
    if ($deployment.spec.replicas -ne 1 -or $deployment.status.readyReplicas -ne 1) {
        throw "$Target must have exactly one ready replica before the experiment"
    }
    $service = Invoke-Kube @('get',"service/$Target",'-o','json') 30 | ConvertFrom-Json
    $instance = "$($service.spec.clusterIP):8000"
    $forward = Start-LocalForward 'prometheus' 13090 9090
    $up = @((Get-Prometheus '/api/v1/query?query=up%7Bjob%3D%22domain%22%7D').result)
    Save-Json $up (Join-Path $directory 'up-before.json')
    if ($up.Count -ne 4 -or @($up | Where-Object { $_.value[1] -ne '1' }).Count) {
        throw 'Expected four healthy domain scrape targets before the experiment'
    }
    if (@(Get-TargetAlert $instance).Count) { throw "$Target alert already active" }

    Invoke-Kube @('annotate',"scaledobject/$scaledObject",'autoscaling.keda.sh/paused-replicas=0','--overwrite') 30 | Out-Null
    $paused = $true
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        $replicas = (Invoke-Kube @('get',"deployment/$Target",'-o','jsonpath={.spec.replicas}') 30).Trim()
        if ($replicas -eq '0') { break }
        Start-Sleep -Seconds 3
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($replicas -ne '0') { throw "$Target did not scale to zero within 90 seconds" }
    $result.scaled_to_zero_at = [DateTimeOffset]::UtcNow.ToString('o')

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $alerts = @(Get-TargetAlert $instance)
        $firing = @($alerts | Where-Object { $_.state -eq 'firing' })
        if ($firing.Count) { break }
        Start-Sleep -Seconds 10
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $firing.Count) { throw "$Target alert did not fire within $TimeoutSeconds seconds" }
    Save-Json $firing (Join-Path $directory 'alert-firing.json')
    $result.alert_fired = $true
    $result.alert_fired_at = [DateTimeOffset]::UtcNow.ToString('o')
} catch {
    $failure = $_.Exception.Message
} finally {
    if ($paused) {
        try {
            Invoke-Kube @('annotate',"scaledobject/$scaledObject",'autoscaling.keda.sh/paused-replicas-') 30 | Out-Null
            Invoke-Kube @('rollout','status',"deployment/$Target",'--timeout=180s') 190 | Out-Null
            $result.service_restored = $true
            $result.restored_at = [DateTimeOffset]::UtcNow.ToString('o')
            if ($forward) {
                $deadline = [DateTime]::UtcNow.AddSeconds(120)
                do {
                    $up = @((Get-Prometheus '/api/v1/query?query=up%7Bjob%3D%22domain%22%7D').result)
                    $alerts = @(Get-TargetAlert $instance)
                    $targetUp = @($up | Where-Object { $_.metric.instance -eq $instance -and $_.value[1] -eq '1' })
                    if ($up.Count -eq 4 -and $targetUp.Count -eq 1 -and -not $alerts.Count) { break }
                    Start-Sleep -Seconds 5
                } while ([DateTime]::UtcNow -lt $deadline)
                Save-Json $up (Join-Path $directory 'up-after.json')
                if ($up.Count -eq 4 -and $targetUp.Count -eq 1 -and -not $alerts.Count) {
                    $result.alert_resolved = $true
                } else { throw "$Target scrape target or alert did not recover within 120 seconds" }
            }
        } catch {
            if ($failure) { $failure += "; restoration: $($_.Exception.Message)" }
            else { $failure = "restoration: $($_.Exception.Message)" }
        }
    }
    if ($forward) {
        if (-not $forward.HasExited) { $forward.Kill($true); $forward.WaitForExit() }
        $forward.Dispose()
    }
    $result.passed = $result.alert_fired -and $result.service_restored -and $result.alert_resolved -and -not $failure
    if ($failure) { $result.error = $failure }
    $result.completed = [DateTimeOffset]::UtcNow.ToString('o')
    Save-Json $result (Join-Path $directory 'result.json')
    if (-not $result.passed) { Save-ClusterDiagnostics $directory }
}

if (-not $result.passed) { throw "Alert experiment failed: $failure. Evidence: $directory" }
Write-Host "Alert fired and resolved; service restored. Evidence: $directory"
