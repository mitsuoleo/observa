#requires -Version 7.0
$ErrorActionPreference='Stop'
$fixture=Join-Path ([IO.Path]::GetTempPath()) ('observa-baseline-test-'+[guid]::NewGuid().ToString('N')+'.json')
try {
    @(
        @{scenario='happy';outcome='completed';duration_seconds=1.2},
        @{scenario='happy';outcome='timeout';duration_seconds=$null},
        @{scenario='payment_rejected';outcome='completed';duration_seconds=0.6},
        @{scenario='stock_unavailable';outcome='completed';duration_seconds=5.5}
    ) | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $fixture -Encoding utf8
    $summary=& "$PSScriptRoot/../../scripts/operations-baseline.ps1" -AnalyzeOnly $fixture | ConvertFrom-Json
    if($summary.attempted -ne 4 -or $summary.completed -ne 3 -or $summary.completion_ratio -ne 0.75){throw 'Failed attempt was excluded from completion denominator.'}
    if($summary.trial_target.completed_within_target -ne 2 -or $summary.trial_target.ratio -ne 0.5){throw 'Trial time target counted a slow or failed attempt.'}
    $happy=@($summary.scenarios | Where-Object scenario -eq 'happy')[0]
    if($happy.duration_seconds.p95 -ne 1.2 -or $happy.completion_ratio -ne 0.5){throw 'Scenario percentile or completion ratio is wrong.'}
    Write-Host 'Experimental baseline calculation passed.'
} finally {
    Remove-Item -LiteralPath $fixture -ErrorAction SilentlyContinue
}
