[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Executable,

    [Parameter(Mandatory = $true)]
    [string]$ExpectedVersion
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$executablePath = (Resolve-Path $Executable).Path
if ([System.IO.Path]::GetExtension($executablePath) -ne '.exe') {
    throw 'the release payload must be a portable .exe, not an installer or archive'
}

$stream = [System.IO.File]::OpenRead($executablePath)
try {
    if ($stream.ReadByte() -ne 0x4d -or $stream.ReadByte() -ne 0x5a) {
        throw 'release payload is not a Windows PE executable'
    }
    $stream.Position = 0x3c
    $reader = New-Object System.IO.BinaryReader($stream)
    $peOffset = $reader.ReadInt32()
    $stream.Position = $peOffset
    if ($reader.ReadUInt32() -ne 0x00004550) { throw 'release payload has an invalid PE header' }
    if ($reader.ReadUInt16() -ne 0x8664) { throw 'release payload is not Windows x64 (AMD64)' }
}
finally {
    $stream.Dispose()
}

$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("transferly-portable-check-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $sandbox | Out-Null
try {
    $sandboxExecutable = Join-Path $sandbox 'transferly.exe'
    Copy-Item $executablePath $sandboxExecutable
    $before = @(Get-ChildItem -Force $sandbox | ForEach-Object Name)
    $versionOutput = (& $sandboxExecutable --version | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'portable executable did not run without an installer or separate runtime' }
    if ($versionOutput -notmatch [regex]::Escape("Transferly $ExpectedVersion")) {
        throw "release reports the wrong product version: $versionOutput"
    }
    if ($versionOutput -notmatch 'Wire protocol [0-9]+\.[0-9]+') {
        throw "release does not report its independently versioned wire protocol: $versionOutput"
    }
    $after = @(Get-ChildItem -Force $sandbox | ForEach-Object Name)
    if (@(Compare-Object $before $after).Count -ne 0) {
        throw "--version created unplanned persistent files: $($after -join ', ')"
    }
}
finally {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $sandbox
}

Write-Host "Portable Windows x64 check passed for $executablePath"
