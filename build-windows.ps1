$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

python -m pip install --upgrade pip
python -m pip install -r requirements-build.txt
python -m PyInstaller --noconfirm --clean lyrics_server.spec

$iscc = Get-Command ISCC.exe -ErrorAction SilentlyContinue
if (-not $iscc) {
    $candidates = @(
        "$env:ProgramFiles\Inno Setup 6\ISCC.exe",
        "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
    )
    $isccPath = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $isccPath) { throw 'Inno Setup 6 is required. Install it from https://jrsoftware.org/isinfo.php' }
} else { $isccPath = $iscc.Source }

& $isccPath 'packaging/installer.iss'
Write-Host "Installer created in dist/installer/NowPlaying-Setup-x64.exe"
