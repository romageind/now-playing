# PyInstaller spec shared by Windows x64 and Linux x64 native builds.
from PyInstaller.utils.hooks import collect_all
import os
import re
import subprocess

packages = [
    "aiohttp",
    "aiohttp_retry",
    "numpy",
    "sounddevice",
    "syncedlyrics",
    "shazamio",
    "imageio_ffmpeg",
    "certifi",
]
datas = [("index.html", ".")]
binaries = []
hiddenimports = ["_sounddevice_data", "sounddevice"]
for package in packages:
    package_datas, package_binaries, package_hidden = collect_all(package)
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hidden

# Bundle PortAudio on Linux so a separate libportaudio2 install is not needed.
if os.name != "nt":
    try:
        cache = subprocess.check_output(["ldconfig", "-p"], text=True)
        match = re.search(r"\s(libportaudio\.so\.2)\s+\([^)]*\)\s+=>\s+(\S+)", cache)
        if match:
            binaries.append((match.group(2), "."))
    except Exception:
        pass

a = Analysis(
    ["lyrics_server.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=["packaging/runtime_hook.py"],
    excludes=["tkinter", "matplotlib", "pytest"],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="NowPlaying",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon="assets/icon.ico" if __import__("os").name == "nt" else None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="NowPlaying",
)
