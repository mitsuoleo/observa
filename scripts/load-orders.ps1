#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateRange(1, 500)][int]$Count = 240,
    [ValidateRange(1, 40)][int]$RatePerSecond = 12,
    [ValidateRange(60, 900)][int]$TimeoutSeconds = 420,
    [ValidateSet('happy', 'stock_unavailable')][string]$Outcome = 'stock_unavailable',
    [switch]$CreateLagBacklog,
    [string]$Directory = ''
)
. "$PSScriptRoot/common.ps1"
if ($Outcome -eq 'happy' -and $Count -gt 80) {
    throw 'Happy-path load is capped at 80 orders because the fixture product starts with 100 units.'
}
if($CreateLagBacklog -and $Count -lt 250){throw 'The isolated lag experiment needs at least 250 orders.'}
if (-not $Directory) {
    $Directory = Join-Path $script:Local ('evidence/scale-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff'))
}
New-Item -ItemType Directory -Force -Path $Directory | Out-Null
$forward = Start-LocalForward 'order' 13300 8000
$client = [Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(20)
$orders = [Collections.Generic.List[string]]::new()
$samples = [Collections.Generic.List[object]]::new()
$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$backlogPaused=$false
$cpuIsolated=$false
$paymentThrottled=$false

function Save-ScaleSample {
    $hpas = Invoke-Kube @('get', 'hpa', '-o', 'json') 30 | ConvertFrom-Json
    $counts = @{order=0;payment=0;inventory=0;notification=0}
    $metrics = @{order=@();payment=@();inventory=@();notification=@()}
    foreach ($hpa in $hpas.items) {
        $counts[$hpa.metadata.name] = if($hpa.status.PSObject.Properties['currentReplicas']){
            [int]$hpa.status.currentReplicas
        }else{0}
        $metrics[$hpa.metadata.name] = if($hpa.status.PSObject.Properties['currentMetrics']){
            @($hpa.status.currentMetrics)
        }else{@()}
    }
    $samples.Add(@{time = [DateTimeOffset]::UtcNow.ToString('o'); replicas = $counts; metrics = $metrics})
    Save-Json $samples.ToArray() (Join-Path $Directory 'replica-samples.json')
}

try {
    if($CreateLagBacklog){
        $scaledObject=Invoke-Kube @('get','scaledobject/payment-lag','-o','json') 30 | ConvertFrom-Json
        if($scaledObject.spec.triggers[1].type -ne 'cpu' -or $scaledObject.spec.triggers[1].metadata.value -ne '70'){
            throw 'Expected the standard Payment CPU trigger before isolating the Kafka lag experiment.'
        }
        $annotations=$scaledObject.metadata.PSObject.Properties['annotations']
        if($annotations -and @($scaledObject.metadata.annotations.PSObject.Properties.Name | Where-Object {
            $_ -in @('autoscaling.keda.sh/paused','autoscaling.keda.sh/paused-replicas')
        }).Count){throw 'Payment ScaledObject is already paused; refusing to change its state.'}
        Invoke-Kube @('patch','scaledobject/payment-lag','--type=json','-p',
            '[{"op":"remove","path":"/spec/triggers/1"}]') 30 | Out-Null
        $cpuIsolated=$true
        $payment=Invoke-Kube @('get','deployment/payment','-o','json') 30 | ConvertFrom-Json
        if($payment.spec.template.spec.containers[0].resources.limits.cpu -ne '300m'){
            throw 'Expected the standard Payment CPU limit before the isolated lag experiment.'
        }
        Invoke-Kube @('patch','deployment/payment','--type=json','-p',
            '[{"op":"replace","path":"/spec/template/spec/containers/0/resources/limits/cpu","value":"50m"}]') 30 | Out-Null
        $paymentThrottled=$true
        Invoke-Kube @('rollout','status','deployment/payment','--timeout=120s') 130 | Out-Null
        Invoke-Kube @('annotate','scaledobject/payment-lag','autoscaling.keda.sh/paused-replicas=0','--overwrite') 30 | Out-Null
        $backlogPaused=$true
        $pauseDeadline=[DateTime]::UtcNow.AddSeconds(90)
        do {
            $replicas=(Invoke-Kube @('get','deployment/payment','-o','jsonpath={.spec.replicas}') 30).Trim()
            if($replicas -eq '0'){break}
            Start-Sleep -Seconds 2
        } while([DateTime]::UtcNow -lt $pauseDeadline)
        if($replicas -ne '0'){throw 'KEDA did not pause Payment at zero replicas.'}
    }
    for ($sent = 0; $sent -lt $Count; $sent += $RatePerSecond) {
        $tasks = [Collections.Generic.List[Threading.Tasks.Task[Net.Http.HttpResponseMessage]]]::new()
        $batch = [Math]::Min($RatePerSecond, $Count - $sent)
        for ($index = 0; $index -lt $batch; $index++) {
            $body = @{
                customer_id = [Guid]::NewGuid().ToString()
                items = @(@{product_id = '11111111-1111-1111-1111-111111111111'; quantity = 1; unit_price = 49.90})
                simulate = @{payment = 'approve'; stock = if ($Outcome -eq 'happy') { 'reserve' } else { 'unavailable' }}
            } | ConvertTo-Json -Depth 8 -Compress
            $content = [Net.Http.StringContent]::new($body, [Text.Encoding]::UTF8, 'application/json')
            $tasks.Add($client.PostAsync('http://127.0.0.1:13300/orders', $content))
        }
        foreach ($task in $tasks) {
            $response = $task.GetAwaiter().GetResult()
            try {
                if (-not $response.IsSuccessStatusCode) {
                    throw "Order POST failed: HTTP $([int]$response.StatusCode) $($response.Content.ReadAsStringAsync().GetAwaiter().GetResult())"
                }
                $order = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
                $orders.Add([string]$order.id)
            } finally { $response.Dispose() }
        }
        Save-ScaleSample
        Start-Sleep -Seconds 1
    }
    Save-Json @{count = $orders.Count; ids = $orders.ToArray(); rate_per_second = $RatePerSecond; outcome = $Outcome} (Join-Path $Directory 'load-input.json')
    if($CreateLagBacklog){
        $lagForward=Start-LocalForward 'lag-exporter' 19108 9108
        try {
            $lagDeadline=[DateTime]::UtcNow.AddSeconds(180)
            do {
                $lag=(Invoke-WebRequest 'http://127.0.0.1:19108/metrics' -TimeoutSec 30).Content
                $lag | Set-Content (Join-Path $Directory 'lag-before-resume.prom')
                $rows=@($lag -split '\r?\n' | Where-Object {$_ -match '^observa_consumer_group_lag\{group="observa.payment"' -and $_ -match '\} (\d+)$'} |
                    ForEach-Object {[int]([regex]::Match($_,'\} (\d+)$').Groups[1].Value)})
                if(($rows | Measure-Object -Sum).Sum -gt 200){break}
                Start-Sleep -Seconds 3
            } while([DateTime]::UtcNow -lt $lagDeadline)
            if(($rows | Measure-Object -Sum).Sum -le 200){throw 'Payment lag did not exceed 200 messages while paused.'}
        } finally {
            if(-not $lagForward.HasExited){$lagForward.Kill($true);$lagForward.WaitForExit()}
            $lagForward.Dispose()
        }
        Invoke-Kube @('annotate','scaledobject/payment-lag','autoscaling.keda.sh/paused-replicas-') 30 | Out-Null
        $backlogPaused=$false
    }
    $terminal = @{}
    do {
        foreach ($id in $orders) {
            if ($terminal.ContainsKey($id)) { continue }
            try {
                $order = Invoke-RestMethod "http://127.0.0.1:13300/orders/$id" -TimeoutSec 10
                if ($order.status -in @('CONFIRMED', 'FAILED', 'CANCELLED')) { $terminal[$id] = $order.status }
            } catch { Write-Verbose "Waiting for order $id : $_" }
        }
        Save-ScaleSample
        if ($terminal.Count -eq $orders.Count) { break }
        Start-Sleep -Seconds 5
    } while ([DateTime]::UtcNow -lt $deadline)
    Save-Json @{count = $orders.Count; completed = $terminal.Count; statuses = $terminal} (Join-Path $Directory 'load-results.json')
    if ($terminal.Count -ne $orders.Count) { throw "Only $($terminal.Count) of $($orders.Count) orders reached a terminal status." }
    $expected = if ($Outcome -eq 'happy') { 'CONFIRMED' } else { 'CANCELLED' }
    $bad = @($terminal.Values | Where-Object { $_ -ne $expected })
    if ($bad.Count) { throw "$($bad.Count) orders did not reach $expected; inspect load-results.json." }
    Write-Host "$($orders.Count) orders reached $expected. Evidence: $Directory"
} finally {
    $cleanupErrors=[Collections.Generic.List[string]]::new()
    if($backlogPaused){
        try { Invoke-Kube @('annotate','scaledobject/payment-lag','autoscaling.keda.sh/paused-replicas-') 30 | Out-Null }
        catch { $cleanupErrors.Add("remove KEDA pause: $($_.Exception.Message)") }
    }
    if($cpuIsolated){
        try {
            Invoke-Kube @('patch','scaledobject/payment-lag','--type=json','-p',
                '[{"op":"add","path":"/spec/triggers/-","value":{"type":"cpu","metricType":"Utilization","metadata":{"value":"70"}}}]') 30 | Out-Null
        } catch { $cleanupErrors.Add("restore CPU trigger: $($_.Exception.Message)") }
    }
    if($paymentThrottled){
        try {
            Invoke-Kube @('patch','deployment/payment','--type=json','-p',
                '[{"op":"replace","path":"/spec/template/spec/containers/0/resources/limits/cpu","value":"300m"}]') 30 | Out-Null
            Invoke-Kube @('rollout','status','deployment/payment','--timeout=120s') 130 | Out-Null
        } catch { $cleanupErrors.Add("restore Payment resources: $($_.Exception.Message)") }
    }
    $client.Dispose()
    if (-not $forward.HasExited) { $forward.Kill($true); $forward.WaitForExit() }
    $forward.Dispose()
    if($cleanupErrors.Count){throw "Lag experiment cleanup failed: $($cleanupErrors -join '; ')"}
}
