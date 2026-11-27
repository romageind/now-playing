"""Runtime setup for bundled FFmpeg and Linux PortAudio."""
import ctypes.util
import os
import sys
from pathlib import Path

bundle_root = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))

try:
    import imageio_ffmpeg
    ffmpeg_dir = os.path.dirname(imageio_ffmpeg.get_ffmpeg_exe())
    os.environ["PATH"] = ffmpeg_dir + os.pathsep + os.environ.get("PATH", "")
except Exception:
    pass

# sounddevice asks ctypes.util.find_library() for PortAudio at import time.
# Return the copy bundled into the Linux AppImage instead of requiring a host
# libportaudio2 package.
_bundled_portaudio = bundle_root / "libportaudio.so.2"
if _bundled_portaudio.exists():
    _original_find_library = ctypes.util.find_library

    def _find_library(name):
        if name == "portaudio":
            return str(_bundled_portaudio)
        return _original_find_library(name)

    ctypes.util.find_library = _find_library
