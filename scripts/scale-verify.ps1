#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateRange(1, 500)][int]$Count = 240,
    [ValidateRange(1, 40)][int]$RatePerSecond = 12,
    [ValidateRange(60, 900)][int]$TimeoutSeconds = 420,
    [switch]$CreateLagBacklog,
    [string]$EvidenceDirectory = ''
)
. "$PSScriptRoot/common.ps1"
$directory = if($EvidenceDirectory){(Resolve-Path -LiteralPath $EvidenceDirectory).Path}else{
    Join-Path $script:Local ('evidence/scale-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff'))
}
New-Item -ItemType Directory -Force -Path $directory | Out-Null
if($EvidenceDirectory){
    $recordedInput=Get-Content (Join-Path $directory 'load-input.json') -Raw | ConvertFrom-Json
    $Count=[int]$recordedInput.count
}
foreach ($name in @('order', 'payment', 'inventory', 'notification')) {
    $null = Invoke-Kube @('get', "hpa/$name", '-o', 'name') 30
}
if(-not $EvidenceDirectory){
    & "$PSScriptRoot/load-orders.ps1" -Count $Count -RatePerSecond $RatePerSecond -TimeoutSeconds $TimeoutSeconds -Outcome stock_unavailable -CreateLagBacklog:$CreateLagBacklog -Directory $directory
}

$samples = @(Get-Content (Join-Path $directory 'replica-samples.json') -Raw | ConvertFrom-Json)
$scaled = @($samples | Where-Object {
    $_.replicas.order -ge 2 -or $_.replicas.payment -ge 2 -or
    $_.replicas.inventory -ge 2 -or $_.replicas.notification -ge 2
})
if (-not $scaled.Count) {
    throw "No domain HPA reached two replicas during the load. Evidence: $directory"
}
if($CreateLagBacklog){
    $lagBefore=Get-Content (Join-Path $directory 'lag-before-resume.prom') -Raw
    $lagRows=@($lagBefore -split '\r?\n' | Where-Object {$_ -match '^observa_consumer_group_lag\{group="observa.payment"' -and $_ -match '\} (\d+)$'} |
        ForEach-Object {[int]([regex]::Match($_,'\} (\d+)$').Groups[1].Value)})
    if(($lagRows | Measure-Object -Sum).Sum -le 200){throw 'Recorded Payment backlog did not exceed 200.'}
    $hpa=Invoke-Kube @('get','hpa/payment','-o','json') 30 | ConvertFrom-Json
    $events=Invoke-Kube @('get','events','-o','json') 30 | ConvertFrom-Json
    $lagEvents=@($events.items | Where-Object {
        $_.involvedObject.uid -eq $hpa.metadata.uid -and $_.reason -eq 'SuccessfulRescale' -and
        $_.message -match 'external metric .* above target'
    })
    Save-Json $lagEvents (Join-Path $directory 'keda-lag-rescale-events.json')
    if(-not $lagEvents.Count -or -not @($samples | Where-Object {$_.replicas.payment -ge 2}).Count){
        throw "No HPA scale-up attributed to Kafka external lag was recorded. Evidence: $directory"
    }
}
$deadline = [DateTime]::UtcNow.AddMinutes(4)
do {
    $hpas = Invoke-Kube @('get', 'hpa', '-o', 'json') 30 | ConvertFrom-Json
    $current = @($hpas.items | Where-Object {
        $_.metadata.name -in @('order', 'payment', 'inventory', 'notification') -and
        [int]$_.status.currentReplicas -eq 1
    })
    if ($current.Count -eq 4) { break }
    Start-Sleep -Seconds 10
} while ([DateTime]::UtcNow -lt $deadline)
if ($current.Count -ne 4) { throw "HPAs did not return to one replica within four minutes. Evidence: $directory" }
foreach ($name in @('order', 'payment', 'inventory', 'notification')) {
    Assert-DeploymentReady $name
}

$forward = Start-LocalForward 'lag-exporter' 19108 9108
try {
    $lagDeadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        $metrics = (Invoke-WebRequest 'http://127.0.0.1:19108/metrics' -TimeoutSec 30).Content
        $metrics | Set-Content (Join-Path $directory 'consumer-lag.prom')
        if ($metrics -notmatch 'observa_lag_exporter_scrape_success 1') {
            throw 'Consumer lag exporter query failed.'
        }
        $lagRows = @($metrics -split '\r?\n' | Where-Object { $_ -match '^observa_consumer_group_lag\{' })
        $groups = @($lagRows | ForEach-Object { if ($_ -match 'group="([^"]+)"') { $Matches[1] } } | Sort-Object -Unique)
        $lagged = @($lagRows | Where-Object { $_ -notmatch '\} 0$' })
        if ($groups.Count -eq 4 -and $lagged.Count -eq 0) { break }
        Start-Sleep -Seconds 5
    } while ([DateTime]::UtcNow -lt $lagDeadline)
    if ($groups.Count -ne 4 -or $lagged.Count -ne 0) {
        throw 'Lag did not drain to zero in all four consumer groups within 90 seconds.'
    }
} finally {
    if (-not $forward.HasExited) { $forward.Kill($true); $forward.WaitForExit() }
    $forward.Dispose()
}

$input = Get-Content (Join-Path $directory 'load-input.json') -Raw | ConvertFrom-Json
$ids = @($input.ids | ForEach-Object { "'$( [Guid]::Parse($_).ToString() )'::uuid" }) -join ','
$checks = @(
    @{name='orders';db='orders';sql="SELECT count(*) FROM orders WHERE id IN ($ids) AND status='CANCELLED'";expected=$Count},
    @{name='payments';db='payments';sql="SELECT count(*) FROM payments WHERE order_id IN ($ids) AND status='REFUNDED'";expected=$Count},
    @{name='reservations';db='inventory';sql="SELECT count(*) FROM reservations WHERE order_id IN ($ids)";expected=0},
    @{name='timeline';db='orders';sql="SELECT count(*) FROM order_events_timeline WHERE order_id IN ($ids) AND event_type='payment.refunded'";expected=$Count}
)
$effects = @{}
foreach ($check in $checks) {
    $raw = (Invoke-Kube @('exec', 'postgres-0', '--', 'psql', '-U', 'observa', '-d', $check.db, '-tAc', $check.sql) 60).Trim()
    $effects[$check.name] = [int]$raw
    if ($effects[$check.name] -ne $check.expected) {
        Save-Json $effects (Join-Path $directory 'logical-effects.json')
        throw "$($check.name) logical effect count was $raw, expected $($check.expected)."
    }
}
Save-Json $effects (Join-Path $directory 'logical-effects.json')
Write-Host "Observed scale-up, scale-down, lag metrics and one logical effect per order. Evidence: $directory"
