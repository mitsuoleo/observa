#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateRange(1,100)][int]$Rounds=10,
    [ValidateRange(10,600)][int]$TimeoutSeconds=120,
    [string]$AnalyzeOnly
)
. "$PSScriptRoot/common.ps1"

function Get-NearestRank {
    param([double[]]$Values,[double]$Percentile)
    if($Values.Count -eq 0){return $null}
    $sorted=@($Values | Sort-Object)
    return [Math]::Round($sorted[[Math]::Ceiling($Percentile*$sorted.Count)-1],3)
}

function Get-BaselineSummary {
    param([object[]]$Samples)
    $expected=@('happy','payment_rejected','stock_unavailable')
    $byScenario=@()
    foreach($name in $expected){
        $group=@($Samples | Where-Object {$_.scenario -eq $name})
        $completed=@($group | Where-Object {$_.outcome -eq 'completed'})
        $durations=[double[]]@($completed | ForEach-Object {$_.duration_seconds})
        $byScenario+=@{
            scenario=$name;attempted=$group.Count;completed=$completed.Count
            completion_ratio=if($group.Count){[Math]::Round($completed.Count/$group.Count,4)}else{$null}
            duration_seconds=@{
                p50=Get-NearestRank $durations 0.50
                p95=Get-NearestRank $durations 0.95
                max=if($durations.Count){[Math]::Round(($durations | Measure-Object -Maximum).Maximum,3)}else{$null}
            }
        }
    }
    $allCompleted=@($Samples | Where-Object {$_.outcome -eq 'completed'})
    $withinTrial=@($allCompleted | Where-Object {[double]$_.duration_seconds -le 5.0})
    return @{
        schema_version=1;generated_at=[DateTimeOffset]::UtcNow.ToString('o')
        definition='Completion means the expected terminal state and exactly one matching causal timeline event before timeout. Duration is persisted causal event occurred_at minus order.created occurred_at, only for completed samples.'
        attempted=$Samples.Count;completed=$allCompleted.Count
        completion_ratio=if($Samples.Count){[Math]::Round($allCompleted.Count/$Samples.Count,4)}else{$null}
        trial_target=@{duration_seconds=5.0;completed_within_target=$withinTrial.Count;ratio=if($Samples.Count){[Math]::Round($withinTrial.Count/$Samples.Count,4)}else{$null}}
        scenarios=$byScenario
        interpretation='Experimental local baseline; no approved SLO or production error budget.'
    }
}

if($AnalyzeOnly){
    $samples=@(Get-Content -LiteralPath $AnalyzeOnly -Raw | ConvertFrom-Json)
    Get-BaselineSummary $samples | ConvertTo-Json -Depth 10
    exit 0
}

$directory=Join-Path $script:Local ('evidence/operations-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff'))
New-Item -ItemType Directory -Force -Path $directory | Out-Null
$scenarios=@(
    @{name='happy';payment='approve';stock='reserve';expected='CONFIRMED';terminal='stock.reserved'},
    @{name='payment_rejected';payment='reject';stock='reserve';expected='FAILED';terminal='payment.rejected'},
    @{name='stock_unavailable';payment='approve';stock='unavailable';expected='CANCELLED';terminal='payment.refunded'}
)
$samples=@()
$forward=$null
try {
    $forward=Start-LocalForward 'order' 13300 8000
    $null=Wait-Http 'http://127.0.0.1:13300/health' 30
    for($round=1;$round -le $Rounds;$round++){
        foreach($scenario in $scenarios){
            $sample=@{round=$round;scenario=$scenario.name;expected_status=$scenario.expected;order_id=$null;observed_status=$null;outcome='error';duration_seconds=$null;error=$null}
            try {
                $body=@{
                    customer_id=[Guid]::NewGuid().ToString()
                    items=@(@{product_id='11111111-1111-1111-1111-111111111111';quantity=1;unit_price=49.90})
                    simulate=@{payment=$scenario.payment;stock=$scenario.stock}
                } | ConvertTo-Json -Depth 8
                $order=Invoke-RestMethod -Uri 'http://127.0.0.1:13300/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 15
                $sample.order_id=$order.id
                $deadline=[DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
                do {
                    Start-Sleep -Seconds 1
                    $current=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$($order.id)" -TimeoutSec 10
                    $sample.observed_status=$current.status
                } while($current.status -eq 'PENDING' -and [DateTime]::UtcNow -lt $deadline)
                if($current.status -eq 'PENDING'){$sample.outcome='timeout'}
                elseif($current.status -ne $scenario.expected){$sample.outcome='unexpected_status'}
                else {
                    $timeline=Invoke-RestMethod -Uri "http://127.0.0.1:13300/orders/$($order.id)/timeline" -TimeoutSec 10
                    $start=@($timeline | Where-Object { $_.event_type -eq 'order.created' })
                    $end=@($timeline | Where-Object { $_.event_type -eq $scenario.terminal })
                    if($start.Count -ne 1 -or $end.Count -ne 1){throw "Expected one order.created and one $($scenario.terminal) timeline event"}
                    $duration=([DateTimeOffset]$end[0].occurred_at-[DateTimeOffset]$start[0].occurred_at).TotalSeconds
                    if($duration -lt 0){throw 'Terminal event precedes order.created'}
                    $sample.duration_seconds=[Math]::Round($duration,3)
                    $sample.outcome='completed'
                }
            } catch {$sample.error=$_.Exception.Message}
            $samples+=@($sample)
            Save-Json $samples (Join-Path $directory 'samples.json')
            Write-Host "Round $round / $Rounds, $($scenario.name): $($sample.outcome)"
        }
    }
} finally {
    if($forward){if(-not $forward.HasExited){$forward.Kill($true);$forward.WaitForExit()};$forward.Dispose()}
    Save-Json $samples (Join-Path $directory 'samples.json')
    Save-Json (Get-BaselineSummary $samples) (Join-Path $directory 'summary.json')
    Write-Host "Experimental baseline: $directory"
}
if(@($samples | Where-Object {$_.outcome -eq 'completed'}).Count -ne 3*$Rounds){
    throw 'One or more baseline attempts did not complete correctly; inspect samples.json.'
}
