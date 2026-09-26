#requires -Version 7.0
[CmdletBinding()]
param([ValidateRange(60,900)][int]$TimeoutSeconds=600)
. "$PSScriptRoot/common.ps1"

$version='2.21.0'
$checksum='B43C89FFEEF81722D7E2DD2C079D74789767A0F89CAE1336CFF784994814F6D7'
$directory=Join-Path $script:Local 'keda'
$manifest=Join-Path $directory "keda-$version.yaml"
New-Item -ItemType Directory -Force $directory | Out-Null

# The Kubernetes context is fixed by Invoke-Kube. Check it before any download or install.
$null=Invoke-Kube @('get','namespace',$script:Namespace,'-o','name') 30
if(-not (Test-Path -LiteralPath $manifest) -or
    (Get-FileHash -LiteralPath $manifest -Algorithm SHA256).Hash -ne $checksum){
    $download="$manifest.download"
    try {
        Invoke-Checked 'curl.exe' @('-fL','--retry','3','--max-time','120','-o',$download,
            "https://github.com/kedacore/keda/releases/download/v$version/keda-$version.yaml") 150 | Out-Null
        if((Get-FileHash -LiteralPath $download -Algorithm SHA256).Hash -ne $checksum){
            throw 'KEDA release manifest checksum mismatch; refusing installation.'
        }
        Move-Item -LiteralPath $download -Destination $manifest -Force
    } finally {
        if(Test-Path -LiteralPath $download){Remove-Item -LiteralPath $download -Force}
    }
}

Invoke-Checked 'kubectl' @('--kubeconfig',(Join-Path $script:Local 'kubeconfig'),
    '--context',$script:Profile,'apply','--server-side','-f',$manifest) $TimeoutSeconds | Write-Host
foreach($deployment in @('keda-operator','keda-metrics-apiserver','keda-admission')){
    Invoke-Checked 'kubectl' @('--kubeconfig',(Join-Path $script:Local 'kubeconfig'),
        '--context',$script:Profile,'-n','keda','rollout','status',"deployment/$deployment",
        "--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
}
Write-Host "KEDA v$version ready in the dedicated minikube profile."
