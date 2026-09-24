#requires -Version 7.0
[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$directory=Join-Path $root '.tools'
New-Item -ItemType Directory -Force $directory | Out-Null
$version='v1.39.0'
$checksum='776386465ded2cf610ae397fe302de32c44d12134b5ce7ce98ab05bd7713b360'
$binary=Join-Path $directory 'minikube.exe'
if(-not(Test-Path $binary) -or (Get-FileHash $binary -Algorithm SHA256).Hash -ne $checksum){
    $download=Join-Path $directory 'minikube-download.exe'
    Invoke-WebRequest "https://github.com/kubernetes/minikube/releases/download/$version/minikube-windows-amd64.exe" -OutFile $download -TimeoutSec 180
    if((Get-FileHash $download -Algorithm SHA256).Hash -ne $checksum){throw 'Official minikube binary checksum mismatch; refusing execution.'}
    Move-Item -LiteralPath $download -Destination $binary -Force
}
Write-Host "Verified local minikube $version. PATH unchanged."
