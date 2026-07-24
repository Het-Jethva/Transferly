[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Executable,

    [Parameter(Mandatory = $true)]
    [string]$PfxPath,

    [Parameter(Mandatory = $true)]
    [string]$PfxPassword,

    [string]$TimestampUrl = 'http://timestamp.digicert.com'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$executablePath = (Resolve-Path $Executable).Path
$certificatePath = (Resolve-Path $PfxPath).Path
if ([string]::IsNullOrWhiteSpace($PfxPassword)) { throw 'the protected signing password is required' }

$signtool = Get-Command signtool.exe -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -eq $signtool) {
    $kits = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\bin'
    $candidate = Get-ChildItem -Path $kits -Filter signtool.exe -Recurse -ErrorAction SilentlyContinue |
        Where-Object FullName -Match '\\x64\\signtool\.exe$' |
        Sort-Object FullName -Descending |
        Select-Object -First 1
    if ($null -eq $candidate) { throw 'signtool.exe was not found in PATH or the Windows SDK' }
    $signtoolPath = $candidate.FullName
}
else {
    $signtoolPath = $signtool.Source
}

& $signtoolPath sign /fd SHA256 /td SHA256 /tr $TimestampUrl /f $certificatePath /p $PfxPassword $executablePath
if ($LASTEXITCODE -ne 0) { throw 'Authenticode signing failed; refusing to publish' }
& $signtoolPath verify /pa /all $executablePath
if ($LASTEXITCODE -ne 0) { throw 'Authenticode verification failed; refusing to publish' }

$signature = Get-AuthenticodeSignature $executablePath
if ($signature.Status -ne 'Valid' -or $null -eq $signature.SignerCertificate) {
    throw "Authenticode status is $($signature.Status); refusing to publish an unsigned or invalid artifact"
}
Write-Host "Verified Authenticode signer: $($signature.SignerCertificate.Subject)"
