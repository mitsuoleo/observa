#requires -Version 7.0
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/common.ps1"

foreach ($case in @(
    @{desired=1;updated=1;ready=1;pass=$true},
    @{desired=2;updated=2;ready=2;pass=$true},
    @{desired=2;updated=1;ready=1;pass=$false},
    @{desired=0;updated=0;ready=0;pass=$false}
)) {
    $script:MockDeployment = @{
        spec = @{replicas = $case.desired}
        status = @{updatedReplicas = $case.updated; readyReplicas = $case.ready}
    } | ConvertTo-Json -Depth 4
    $originalKube = (Get-Command Invoke-Kube).ScriptBlock
    try {
        function Invoke-Kube { param($Arguments, $Seconds); $script:MockDeployment }
        $passed = $true
        try { Assert-DeploymentReady 'order' } catch { $passed = $false }
        if ($passed -ne $case.pass) {
            throw "Deployment readiness check failed for desired=$($case.desired) updated=$($case.updated) ready=$($case.ready)."
        }
    } finally { Set-Item Function:Invoke-Kube $originalKube }
}
Write-Host 'Scaling readiness checks passed.'
