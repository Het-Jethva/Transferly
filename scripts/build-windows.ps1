[CmdletBinding()]
param(
    [ValidateNotNullOrEmpty()]
    [string]$Version = 'dev',

    [string]$OutputDirectory = (Join-Path $PSScriptRoot '..\dist')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-Sha256Hex([string]$Path) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $stream = [System.IO.File]::OpenRead($Path)
    try {
        return ([System.BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-', '')
    }
    finally {
        $stream.Dispose()
        $sha.Dispose()
    }
}

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$output = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $output | Out-Null
$artifact = Join-Path $output 'transferly.exe'
$first = Join-Path $output '.transferly-build-1.exe'
$second = Join-Path $output '.transferly-build-2.exe'

try {
    $env:CGO_ENABLED = '0'
    $env:GOOS = 'windows'
    $env:GOARCH = 'amd64'
    $ldflags = "-s -w -X main.buildVersion=$Version"

    Push-Location $repositoryRoot
    try {
        & go build -buildvcs=false -trimpath -ldflags $ldflags -o $first ./cmd/transferly
        if ($LASTEXITCODE -ne 0) { throw 'first Windows build failed' }
        & go build -buildvcs=false -trimpath -ldflags $ldflags -o $second ./cmd/transferly
        if ($LASTEXITCODE -ne 0) { throw 'second Windows build failed' }
    }
    finally {
        Pop-Location
    }

    $firstHash = Get-Sha256Hex $first
    $secondHash = Get-Sha256Hex $second
    if ($firstHash -ne $secondHash) {
        throw "Windows build is not reproducible: $firstHash differs from $secondHash"
    }

    # Fault injection is compiled out by the absence of the transferly_faults
    # build tag. Assert it rather than trusting the build line: the marker below
    # is reachable only from the injectable variant.
    $bytes = [System.IO.File]::ReadAllBytes($first)
    $marker = [System.Text.Encoding]::ASCII.GetBytes('advance-time')
    $limit = $bytes.Length - $marker.Length
    for ($i = 0; $i -le $limit; $i++) {
        $matched = $true
        for ($j = 0; $j -lt $marker.Length; $j++) {
            if ($bytes[$i + $j] -ne $marker[$j]) { $matched = $false; break }
        }
        if ($matched) {
            throw 'Windows build contains fault-injection code'
        }
    }

    Move-Item -Force $first $artifact
    Remove-Item -Force $second

    Write-Host "Reproducible portable Windows executable: $artifact"
    Write-Host "SHA-256: $firstHash"
}
finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $first, $second
}
