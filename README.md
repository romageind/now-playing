# Now Playing — Windows and Linux x64 packaging

This project packages the existing `index` and `lyrics_server` app as native, self-contained distributions. The source application is included; the package bundles Python and Python dependencies, plus an FFmpeg binary supplied by `imageio-ffmpeg`.

## Build both installers automatically (recommended)

GitHub Actions must build each target on its native operating system. This repository includes `.github/workflows/build-installers.yml` with separate Windows x64 and Ubuntu 22.04 x64 jobs.

1. Create a new GitHub repository and upload the contents of this folder.
2. Open the repository's **Actions** tab.
3. Select **Build Windows and Linux installers** and click **Run workflow**.
4. When both jobs finish, download the two artifacts:
   - `NowPlaying-Windows-x64` contains `NowPlaying-Setup-x64.exe`.
   - `NowPlaying-Linux-x86_64` contains `NowPlaying-x86_64.AppImage`.

The workflow also runs when a `v*` tag is pushed.

## Build on Windows yourself

Use 64-bit Windows with Python 3.11 x64 and Inno Setup 6 installed. In PowerShell, run:

```powershell
.\packaging\build-windows.ps1
```

Output: `dist/installer/NowPlaying-Setup-x64.exe`.

## Build on Linux yourself

Use Ubuntu 22.04 x64, Python 3.11, `libportaudio2`, `portaudio19-dev`, `libfuse2`, `patchelf`, and `wget`. Download the x86_64 `appimagetool` to `tools/appimagetool-x86_64.AppImage`, make it executable, then run:

```bash
./packaging/build-linux.sh
```

Output: `dist/NowPlaying-x86_64.AppImage`.

## Runtime notes

- These are separate native x64 builds; an installer is not cross-platform.
- Shazam recognition and lyric lookup require an internet connection.
- The operating system must allow microphone access. Linux audio backends (ALSA/PulseAudio/PipeWire compatibility) are provided by the host system; this AppImage bundles PortAudio but cannot bundle every distribution's audio stack.
- The app opens its local browser interface at `http://localhost:8090`.
- Microphone selection is stored in the user's application config directory when packaged, not in the installation directory.
- The app remains bound to `127.0.0.1`; it is intended for use on the computer running it.
