#requires -Version 7.0
$ErrorActionPreference='Stop'
. "$PSScriptRoot/common.ps1"
function Assert-True([bool]$Condition,[string]$Message){if(-not $Condition){throw $Message}}
$echo=Invoke-Checked pwsh @('-NoProfile','-Command','[Console]::Write("verified")') 10
Assert-True ($echo -eq 'verified') 'Native stdout was not preserved.'
$failed=$false
try {Invoke-Checked pwsh @('-NoProfile','-Command','[Console]::Error.Write("intentional"); exit 7') 10 | Out-Null}
catch {$failed=$_.Exception.Message -match 'exited 7.*intentional'}
Assert-True $failed 'Nonzero exit did not fail with stderr evidence.'
$timedOut=$false
try {Invoke-Checked pwsh @('-NoProfile','-Command','Start-Sleep -Seconds 20') 1 | Out-Null}
catch {$timedOut=$_.Exception.Message -match 'timed out'}
Assert-True $timedOut 'Timeout did not terminate the child process.'
$listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
$listener.Start()
try {
    $busy=$false
    try {Start-LocalForward 'unused' $listener.LocalEndpoint.Port 3000 | Out-Null}
    catch {$busy=$_.Exception.Message -match 'occupied'}
    Assert-True $busy 'Port-forward did not reject an occupied port.'
} finally {$listener.Stop()}
$original=(Get-Command Invoke-Checked).ScriptBlock
try {
    function Invoke-Checked {param($File,$Arguments,$Seconds); $script:Captured=@($File)+$Arguments; 'mocked'}
    Invoke-Kube @('get','pods') | Out-Null
    Assert-True ($script:Captured -contains '--kubeconfig') 'Kube command omitted isolated kubeconfig.'
    Assert-True ($script:Captured -contains (Join-Path $script:Local 'kubeconfig')) 'Kube command used ambient kubeconfig.'
    Assert-True ($script:Captured -contains 'observa-spike0') 'Kube command omitted dedicated context.'
} finally {Set-Item Function:Invoke-Checked $original}
Write-Host 'Automation checks passed: stdout, errors, timeout, occupied port and explicit kubeconfig.'
