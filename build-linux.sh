#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m pip install --upgrade pip
python3 -m pip install -r requirements-build.txt
python3 -m PyInstaller --noconfirm --clean lyrics_server.spec

APPDIR="$PWD/dist/NowPlaying.AppDir"
rm -rf "$APPDIR"
mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/lib"
cp -a dist/NowPlaying/. "$APPDIR/usr/bin/"
cp assets/icon.png "$APPDIR/nowplaying.png"
cp assets/nowplaying.desktop "$APPDIR/nowplaying.desktop"

# PortAudio is included as a PyInstaller binary in the _internal directory.

cat > "$APPDIR/AppRun" <<'APPRUN'
#!/usr/bin/env bash
HERE="$(dirname "$(readlink -f "$0")")"
export LD_LIBRARY_PATH="$HERE/usr/lib:$HERE/usr/bin/_internal:${LD_LIBRARY_PATH:-}"
export PATH="$HERE/usr/bin/_internal:$PATH"
export PORT="${PORT:-8090}"
export NOWPLAYING_OPEN_BROWSER=1
exec "$HERE/usr/bin/NowPlaying" "$@"
APPRUN
chmod +x "$APPDIR/AppRun"
chmod +x "$APPDIR/usr/bin/NowPlaying"

if [[ -z "${APPIMAGETOOL:-}" ]]; then
  APPIMAGETOOL="$PWD/tools/appimagetool-x86_64.AppImage"
fi
if [[ ! -x "$APPIMAGETOOL" ]]; then
  echo 'Set APPIMAGETOOL to an executable appimagetool-x86_64.AppImage.' >&2
  exit 1
fi
ARCH=x86_64 "$APPIMAGETOOL" "$APPDIR" "$PWD/dist/NowPlaying-x86_64.AppImage"
echo "Created dist/NowPlaying-x86_64.AppImage"
