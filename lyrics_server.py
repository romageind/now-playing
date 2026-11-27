#!/usr/bin/env python3
"""
Local live-microphone music recognition and synchronized lyrics server.

- Continuous microphone capture via sounddevice with auto-reconnect and device switching.
- Constant Shazam recognition loop using a single persistent client (no session leak).
- New song switching with immediate lyrics/BPM/history reset and position sync.
- Reliable manual re-identify (/refresh) that never stays stuck on 'refreshing'.
"""
import asyncio
import collections
import datetime
import io
import json
import math
import os
import re
import sys
import threading
import time
import urllib.parse
import wave
import webbrowser
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import numpy as np
import syncedlyrics
from aiohttp import web
from aiohttp_retry import ExponentialRetry
from shazamio import HTTPClient, Shazam

# PyInstaller places bundled data in its extraction/application directory.
HERE = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))
RATE = 44100
BLOCK = 1024

SAMPLE_SECONDS = min(14.0, max(5.0, float(os.getenv("SAMPLE_SECONDS", "8.0"))))
FAST_SAMPLE_SECONDS = min(14.0, max(5.0, float(os.getenv("FAST_SAMPLE_SECONDS", "8.0"))))
CONSTANT_SHAZAM_SECONDS = max(8.0, float(os.getenv("CONSTANT_SHAZAM_SECONDS", os.getenv("RECHECK_SECONDS", "10.0"))))
ERROR_RETRY_INTERVAL = max(2.5, float(os.getenv("ERROR_RETRY_INTERVAL", "3.0")))
RATE_LIMIT_COOLDOWN_SECONDS = max(10.0, float(os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "12.0")))
RATE_LIMIT_MAX_COOLDOWN_SECONDS = 20.0
SHAZAM_MIN_INTERVAL_SECONDS = max(6.0, float(os.getenv("SHAZAM_MIN_INTERVAL_SECONDS", "9.0")))
END_SILENCE_SECONDS = max(2.8, float(os.getenv("END_SILENCE_SECONDS", "3.6")))
SHAZAM_TIMEOUT_SECONDS = max(5.0, float(os.getenv("SHAZAM_TIMEOUT_SECONDS", "12.0")))
PORT = int(os.getenv("PORT", "8090"))

DEVICE = (os.getenv("AUDIO_DEVICE") or "").strip() or None
if DEVICE is not None and str(DEVICE).isdigit():
    DEVICE = int(DEVICE)

if getattr(sys, "frozen", False):
    if os.name == "nt":
        _config_root = Path(os.getenv("APPDATA", str(Path.home() / "AppData" / "Roaming"))) / "NowPlaying"
    else:
        _config_root = Path(os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "nowplaying"
    _config_root.mkdir(parents=True, exist_ok=True)
    MIC_PREF_FILE = _config_root / ".selected_microphone.json"
else:
    MIC_PREF_FILE = HERE / ".selected_microphone.json"
MAX_HISTORY_ENTRIES = 25
BPM_CACHE_MAX = 128

state = {
    "status": "listening",
    "key": None,
    "title": None,
    "artist": None,
    "album": None,
    "bpm": None,
    "art": None,
    "lyrics": [],
    "synced": False,
    "lyrics_searched": False,
    "song_ended": False,
    "song_end_seq": 0,
    "start_epoch": None,
    "version": 0,
    "last_reidentify": None,
}
state_lock = threading.RLock()
play_history = collections.deque(maxlen=MAX_HISTORY_ENTRIES)
history_version = 0
generation_counter = 0
lyrics_refresh_counter = 0
_bpm_cache = collections.OrderedDict()
_bpm_in_flight = set()

_scan_lock = asyncio.Lock()
_shazam_client = None
rate_limit_state = {"until": 0.0, "strikes": 0, "last_request": 0.0}
wake_event = None
_loop = None

_ws_event_queue = collections.deque(maxlen=32)
_ws_event_lock = threading.Lock()

mic_lock = threading.Lock()
mic_pref = {
    "mode": "default",
    "name": None,
    "hostapi": None,
    "index": None,
}
mic_status_state = {
    "status": "initializing",
    "message": "Initializing live microphone...",
    "device_name": None,
    "mode": "default",
    "audio_detected": False,
    "music_state": "none",
    "music_confidence": 0.0,
    "level": 0.0,
    "version": 1,
}
_mic_stop_event = threading.Event()
_mic_wake_event = threading.Event()
_last_logged_candidates_sig = None


def log(tag, msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{tag:<9}] {msg}", flush=True)


def _format_ts(epoch_sec):
    try:
        return datetime.datetime.fromtimestamp(float(epoch_sec)).strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return "n/a"


def _emit_ws_event(event_name, payload=None):
    item = {"ev": event_name, "ts": time.time()}
    if isinstance(payload, dict):
        item.update(payload)
    with _ws_event_lock:
        _ws_event_queue.append(item)


def _drain_ws_events():
    with _ws_event_lock:
        if not _ws_event_queue:
            return []
        items = list(_ws_event_queue)
        _ws_event_queue.clear()
        return items


def _trigger_song_ended_signal(reason="silence timeout"):
    global _song_ended, _paused_position
    _song_ended = True
    with state_lock:
        prev_ended = bool(state.get("song_ended"))
        seq = int(state.get("song_end_seq") or 0) + 1
        state["song_ended"] = True
        if state.get("start_epoch") is not None:
            _paused_position = max(0.0, time.time() - state["start_epoch"])
        state["song_end_seq"] = seq
        state["status"] = "idle"
        state["version"] += 1
        cur_key = state.get("key")
        cur_title = state.get("title")
    if not prev_ended:
        log("PAUSED", f"Music stopped/paused -> status=idle, song_ended=True (seq={seq}, reason={reason}, track='{cur_title or ''}', key={cur_key})")
    _emit_ws_event("song_ended", {"ended": 1, "end_seq": seq, "reason": reason, "key": cur_key})
    # No useful recognition request can be made against silence. Keep the
    # listener armed; the first sustained-music transition wakes it immediately
    # and its next scan is for the next track rather than the ended one.
    log("READY", "Track ended; waiting for the next sustained music onset to identify a new song")


def _bump_mic_version_locked():
    mic_status_state["version"] = int(mic_status_state.get("version", 0)) + 1


def _set_mic_status(
    status,
    message,
    device_name=None,
    mode=None,
    audio_detected=None,
    level=None,
    music_state=None,
    music_confidence=None,
):
    logged_summary = None
    with mic_lock:
        changed = False
        if status is not None and mic_status_state.get("status") != status:
            mic_status_state["status"] = status
            changed = True
        if message is not None and mic_status_state.get("message") != message:
            mic_status_state["message"] = message
            changed = True
        if device_name is not None or status in ("unavailable", "disconnected"):
            if mic_status_state.get("device_name") != device_name:
                mic_status_state["device_name"] = device_name
                changed = True
        if mode is not None and mic_status_state.get("mode") != mode:
            mic_status_state["mode"] = mode
            changed = True
        if audio_detected is not None and mic_status_state.get("audio_detected") != bool(audio_detected):
            mic_status_state["audio_detected"] = bool(audio_detected)
            changed = True
        if music_state is not None and mic_status_state.get("music_state") != music_state:
            mic_status_state["music_state"] = music_state
            changed = True
        if music_confidence is not None:
            mc = round(max(0.0, min(1.0, float(music_confidence))), 2)
            if abs(mic_status_state.get("music_confidence", 0.0) - mc) >= 0.05:
                mic_status_state["music_confidence"] = mc
        if level is not None:
            lvl = round(max(0.0, min(1.0, float(level))), 3)
            if abs(mic_status_state.get("level", 0.0) - lvl) >= 0.03:
                mic_status_state["level"] = lvl
        if changed:
            _bump_mic_version_locked()
            logged_summary = (
                f"status={mic_status_state.get('status')} dev={mic_status_state.get('device_name') or 'none'} "
                f"mode={mic_status_state.get('mode')} msg={mic_status_state.get('message')}"
            )
    if logged_summary:
        log("MIC", logged_summary)


def _update_mic_signal_level(rms_val, music_active, music_state, music_conf):
    norm_level = max(0.0, min(1.0, math.sqrt(max(0.0, float(rms_val)) / 0.06)))
    log_state_msg = None
    with mic_lock:
        prev_aud = bool(mic_status_state.get("audio_detected"))
        prev_ms = mic_status_state.get("music_state")
        cur_st = mic_status_state.get("status")
        mc_rounded = round(max(0.0, min(1.0, float(music_conf))), 2)
        if cur_st == "active":
            mic_status_state["audio_detected"] = bool(music_active)
            mic_status_state["music_state"] = music_state
            mic_status_state["music_confidence"] = mc_rounded
            if prev_aud != bool(music_active) or prev_ms != music_state:
                _bump_mic_version_locked()
                log_state_msg = (
                    f"state={music_state} music_present={bool(music_active)} conf={mc_rounded:.2f} "
                    f"rms={float(rms_val):.5f}"
                )
        else:
            mic_status_state["audio_detected"] = False
            mic_status_state["music_state"] = "none"
            mic_status_state["music_confidence"] = 0.0
            if prev_aud or prev_ms != "none":
                _bump_mic_version_locked()
        lvl = round(norm_level, 3)
        if abs(mic_status_state.get("level", 0.0) - lvl) >= 0.03:
            mic_status_state["level"] = lvl
    if log_state_msg:
        log("LOCAL_AUD", log_state_msg)


def load_saved_mic_pref():
    global mic_pref
    data = None
    if MIC_PREF_FILE.exists():
        try:
            raw = json.loads(MIC_PREF_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                data = raw
        except Exception as exc:
            log("WARN", f"Could not read saved microphone preference: {exc!r}")
    with mic_lock:
        saved_name = str(data.get("name")) if (data and data.get("name")) else None
        saved_idx = data.get("index") if (data and isinstance(data.get("index"), int)) else None
        if data and data.get("mode") == "manual" and (saved_name or saved_idx is not None):
            mic_pref = {
                "mode": "manual",
                "name": saved_name,
                "hostapi": data.get("hostapi"),
                "index": saved_idx,
            }
        elif DEVICE is not None:
            mic_pref = {
                "mode": "manual",
                "name": str(DEVICE) if not isinstance(DEVICE, int) else None,
                "hostapi": None,
                "index": DEVICE if isinstance(DEVICE, int) else None,
            }
        else:
            mic_pref = {
                "mode": "default",
                "name": None,
                "hostapi": None,
                "index": None,
            }
        mic_status_state["mode"] = mic_pref["mode"]
        _bump_mic_version_locked()
    log("MIC", f"Loaded mic preference: mode={mic_pref['mode']} name={mic_pref['name']} index={mic_pref['index']}")


def save_mic_pref_locked():
    payload = {
        "mode": mic_pref.get("mode", "default"),
        "name": mic_pref.get("name"),
        "hostapi": mic_pref.get("hostapi"),
        "index": mic_pref.get("index"),
    }
    try:
        if payload["mode"] == "manual" and (payload["name"] or payload["index"] is not None):
            MIC_PREF_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        elif MIC_PREF_FILE.exists():
            MIC_PREF_FILE.unlink()
    except Exception as exc:
        log("WARN", f"Could not persist microphone preference: {exc!r}")


def _query_input_devices():
    global _last_logged_candidates_sig
    import sounddevice as sd

    devices = sd.query_devices()
    hostapis = sd.query_hostapis()

    default_in_idx = None
    try:
        d_in, _ = sd.default.device
        if d_in is not None and int(d_in) >= 0:
            default_in_idx = int(d_in)
    except Exception:
        default_in_idx = None

    wasapi_default_idx = None
    for ha in hostapis:
        ha_name = str((ha or {}).get("name") or "").lower()
        if "wasapi" in ha_name and int((ha or {}).get("default_input", -1)) >= 0:
            wasapi_default_idx = int(ha["default_input"])
            break

    inputs = []
    for idx, dev in enumerate(devices):
        max_in = int((dev or {}).get("max_input_channels") or 0)
        if max_in <= 0:
            continue
        name = str((dev or {}).get("name") or f"Input {idx}").strip()
        ha_idx = int((dev or {}).get("hostapi") if (dev or {}).get("hostapi") is not None else -1)
        ha_name = str((hostapis[ha_idx] if 0 <= ha_idx < len(hostapis) else {}).get("name") or "").strip()
        def_sr = float((dev or {}).get("default_samplerate") or RATE)
        if not math.isfinite(def_sr) or def_sr < 8000:
            def_sr = float(RATE)

        inputs.append({
            "index": idx,
            "name": name,
            "hostapi": ha_name,
            "hostapi_index": ha_idx,
            "max_input_channels": max_in,
            "default_samplerate": def_sr,
            "is_windows_default": False,
        })

    effective_default_idx = None
    valid_indices = {d["index"] for d in inputs}
    for candidate_idx in (wasapi_default_idx, default_in_idx):
        if candidate_idx is not None and candidate_idx in valid_indices:
            effective_default_idx = candidate_idx
            break
    if effective_default_idx is None and inputs:
        effective_default_idx = inputs[0]["index"]

    for d in inputs:
        d["is_windows_default"] = (effective_default_idx is not None and d["index"] == effective_default_idx)

    cand_sig = tuple(
        (d["index"], d["name"], d["hostapi"], d["max_input_channels"], d["is_windows_default"])
        for d in inputs
    )
    if cand_sig != _last_logged_candidates_sig:
        _last_logged_candidates_sig = cand_sig
        summary = ", ".join(
            f"[{d['index']}] {d['name']} ({d['hostapi']}, {d['max_input_channels']}ch{'*' if d['is_windows_default'] else ''})"
            for d in inputs
        ) or "none"
        log("MIC_CAND", f"Input devices (max_input_channels>0): {summary}")

    return inputs, effective_default_idx


def _resolve_target_device(inputs, default_idx):
    if not inputs:
        return None, "No input device with max_input_channels > 0 found."

    with mic_lock:
        mode = mic_pref.get("mode", "default")
        target_name = mic_pref.get("name")
        target_hostapi = mic_pref.get("hostapi")
        target_index = mic_pref.get("index")

    if mode != "manual":
        if default_idx is not None:
            for item in inputs:
                if item["index"] == default_idx:
                    return item, None
        return inputs[0], None

    if isinstance(target_index, int):
        for item in inputs:
            if item["index"] == target_index:
                return item, None

    if target_name:
        if target_hostapi:
            for item in inputs:
                if item["name"] == target_name and item.get("hostapi") == target_hostapi:
                    return item, None
        for item in inputs:
            if item["name"] == target_name:
                return item, None

    return None, f"Selected input '{target_name or target_index}' is not available."


def get_devices_payload():
    try:
        inputs, default_idx = _query_input_devices()
    except Exception as exc:
        log("WARN", f"Device enumeration failed: {exc!r}")
        inputs, default_idx = [], None

    with mic_lock:
        mode = mic_pref.get("mode", "default")
        pref_name = mic_pref.get("name")
        pref_idx = mic_pref.get("index")
        st_now = mic_status_state.get("status", "initializing")
        msg_now = mic_status_state.get("message", "")
        dev_now = mic_status_state.get("device_name")
        aud_now = bool(mic_status_state.get("audio_detected"))
        ms_now = mic_status_state.get("music_state", "none")
        mc_now = float(mic_status_state.get("music_confidence", 0.0))
        lvl_now = float(mic_status_state.get("level", 0.0))
        m_ver = int(mic_status_state.get("version", 1))
        mic_snap = dict(mic_status_state)

    win_def_name = None
    for d in inputs:
        if d.get("is_windows_default") or (default_idx is not None and d.get("index") == default_idx):
            win_def_name = d.get("name")
            break

    clean_devices = [
        {
            "index": d["index"],
            "name": d["name"],
            "hostapi": d.get("hostapi", ""),
            "max_input_channels": d.get("max_input_channels", 1),
            "is_windows_default": bool(d.get("is_windows_default")),
        }
        for d in inputs
    ]

    return {
        "ok": True,
        "mode": mode,
        "preferred_name": pref_name,
        "preferred_index": pref_idx,
        "windows_default_name": win_def_name,
        "active_device_name": dev_now,
        "mic_status": st_now,
        "mic_message": msg_now,
        "audio_detected": aud_now,
        "music_state": ms_now,
        "music_confidence": mc_now,
        "mic_level": lvl_now,
        "mic_version": m_ver,
        "mic": mic_snap,
        "devices": clean_devices,
    }


def set_microphone_selection(mode, device_name=None, hostapi=None, device_index=None):
    mode_clean = (mode or "default").strip().lower()
    if mode_clean not in ("default", "manual"):
        raise ValueError("Invalid selection mode.")
    with mic_lock:
        if mode_clean == "default":
            mic_pref["mode"] = "default"
            mic_pref["name"] = None
            mic_pref["hostapi"] = None
            mic_pref["index"] = None
            mic_status_state["mode"] = "default"
            mic_status_state["status"] = "initializing"
            mic_status_state["message"] = "Switching to default input..."
        else:
            name_clean = (device_name or "").strip() or None
            idx_clean = int(device_index) if device_index is not None and str(device_index).isdigit() else None
            if not name_clean and idx_clean is None:
                raise ValueError("Device name or index is required.")
            mic_pref["mode"] = "manual"
            mic_pref["name"] = name_clean
            mic_pref["hostapi"] = (hostapi or "").strip() or None
            mic_pref["index"] = idx_clean
            mic_status_state["mode"] = "manual"
            mic_status_state["status"] = "initializing"
            mic_status_state["message"] = f"Switching to {name_clean or f'index {idx_clean}'}..."
        mic_status_state["audio_detected"] = False
        mic_status_state["music_state"] = "none"
        mic_status_state["music_confidence"] = 0.0
        _bump_mic_version_locked()
        save_mic_pref_locked()

    log("MIC_SEL", f"Selection -> mode={mic_pref['mode']} idx={mic_pref['index']} name={mic_pref['name']} hostapi={mic_pref['hostapi']}")
    with audio_lock:
        audio_buffer.clear()
    _reset_music_detector_state()
    _mic_wake_event.set()
    return get_devices_payload()


def extract_album_name(track):
    if not isinstance(track, dict):
        return None
    sections = track.get("sections")
    if isinstance(sections, list):
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            if sec.get("type") == "SONG":
                meta = sec.get("metadata")
                if isinstance(meta, list):
                    for item in meta:
                        if not isinstance(item, dict):
                            continue
                        title_lbl = str(item.get("title") or "").strip().lower()
                        if title_lbl in ("album", "album title", "record"):
                            val = str(item.get("text") or "").strip()
                            if val:
                                return val
    hub = track.get("hub")
    if isinstance(hub, dict):
        for key in ("album", "albumname", "collection"):
            val = hub.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
            if isinstance(val, dict) and isinstance(val.get("name"), str) and val["name"].strip():
                return val["name"].strip()
    for key in ("album", "album_name", "collectionName"):
        val = track.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _normalize_bpm_value(raw_val):
    if raw_val is None or isinstance(raw_val, bool):
        return None
    try:
        num = float(str(raw_val).strip())
    except (ValueError, TypeError):
        m = re.search(r"(\d+(?:\.\d+)?)", str(raw_val))
        if not m:
            return None
        try:
            num = float(m.group(1))
        except ValueError:
            return None
    if not math.isfinite(num) or num < 40.0 or num > 260.0:
        return None
    rounded = int(round(num))
    return rounded if 40 <= rounded <= 260 else None


def extract_bpm_from_track(track):
    if not isinstance(track, dict):
        return None
    for k in ("bpm", "tempo", "beats_per_minute"):
        val = _normalize_bpm_value(track.get(k))
        if val is not None:
            return val
    sections = track.get("sections")
    if isinstance(sections, list):
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            meta = sec.get("metadata")
            if isinstance(meta, list):
                for item in meta:
                    if not isinstance(item, dict):
                        continue
                    lbl = str(item.get("title") or "").strip().lower()
                    if lbl in ("bpm", "tempo", "beats per minute"):
                        val = _normalize_bpm_value(item.get("text"))
                        if val is not None:
                            return val
    return None


def _clean_match_str(s):
    s = re.sub(r"\s*\([^)]*\)|\s*\[[^\]]*\]", "", str(s or "")).strip().lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _titles_compatible(target_title, candidate_title, target_artist, candidate_artist):
    t1 = _clean_match_str(target_title)
    t2 = _clean_match_str(candidate_title)
    a1 = _clean_match_str(target_artist)
    a2 = _clean_match_str(candidate_artist)
    if not t1 or not t2 or not a1 or not a2:
        return False
    title_ok = (t1 == t2) or (t1 in t2) or (t2 in t1)
    primary_a1 = re.split(r" (?:feat|ft|featuring|with|and|x) |&|,", a1)[0].strip()
    primary_a2 = re.split(r" (?:feat|ft|featuring|with|and|x) |&|,", a2)[0].strip()
    artist_ok = (
        (a1 == a2)
        or (a1 in a2)
        or (a2 in a1)
        or (primary_a1 and primary_a2 and (primary_a1 == primary_a2 or primary_a1 in primary_a2 or primary_a2 in primary_a1))
    )
    return bool(title_ok and artist_ok)


async def _lookup_bpm_online(title, artist):
    clean_title = re.sub(r"\s*\([^)]*\)|\s*\[[^\]]*\]", "", str(title or "")).strip() or str(title or "").strip()
    clean_artist = str(artist or "").strip()
    if not clean_title or not clean_artist:
        return None

    headers = {"User-Agent": "NowPlayingLyricsDisplay/1.0"}
    timeout = aiohttp.ClientTimeout(total=8)
    bpm_candidates = []

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
            q_strict = urllib.parse.quote(f'artist:"{clean_artist}" track:"{clean_title}"')
            url_search = f"https://api.deezer.com/search?q={q_strict}&limit=5"
            async with sess.get(url_search) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    items = data.get("data") if isinstance(data, dict) else None
                    if not items:
                        q_loose = urllib.parse.quote(f"{clean_artist} {clean_title}")
                        async with sess.get(f"https://api.deezer.com/search?q={q_loose}&limit=5") as resp2:
                            if resp2.status == 200:
                                data2 = await resp2.json(content_type=None)
                                items = data2.get("data") if isinstance(data2, dict) else None
                    if isinstance(items, list):
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            cand_title = item.get("title") or item.get("title_short") or ""
                            cand_artist = (item.get("artist") or {}).get("name") or ""
                            if not _titles_compatible(clean_title, cand_title, clean_artist, cand_artist):
                                continue
                            inline_bpm = _normalize_bpm_value(item.get("bpm"))
                            if inline_bpm is not None:
                                bpm_candidates.append(inline_bpm)
                                break
                            track_id = item.get("id")
                            if track_id:
                                async with sess.get(f"https://api.deezer.com/track/{track_id}") as t_resp:
                                    if t_resp.status == 200:
                                        t_data = await t_resp.json(content_type=None)
                                        det_bpm = _normalize_bpm_value((t_data or {}).get("bpm"))
                                        if det_bpm is not None:
                                            bpm_candidates.append(det_bpm)
                                            break
    except Exception as exc:
        log("META", f"BPM lookup error for '{clean_artist} - {clean_title}': {exc!r}")

    if bpm_candidates:
        if len(bpm_candidates) < 2 or abs(bpm_candidates[0] - bpm_candidates[1]) <= 4:
            return bpm_candidates[0]
    return None


async def update_track_bpm_async(req_gen, key, title, artist, embedded_bpm=None):
    if not key:
        return
    cache_key = str(key)
    if embedded_bpm is not None:
        _bpm_cache[cache_key] = embedded_bpm
        while len(_bpm_cache) > BPM_CACHE_MAX:
            _bpm_cache.popitem(last=False)

    cached_bpm = _bpm_cache.get(cache_key)
    if cached_bpm is not None:
        _bpm_cache.move_to_end(cache_key)
        with state_lock:
            if state["key"] == key and state.get("bpm") != cached_bpm:
                state["bpm"] = cached_bpm
                state["version"] += 1
        return

    if cache_key in _bpm_in_flight:
        return
    _bpm_in_flight.add(cache_key)
    try:
        final_bpm = await _lookup_bpm_online(title, artist)
        if final_bpm is not None:
            _bpm_cache[cache_key] = final_bpm
            while len(_bpm_cache) > BPM_CACHE_MAX:
                _bpm_cache.popitem(last=False)
        with state_lock:
            if state["key"] != key or generation_counter != req_gen:
                return
            if state.get("bpm") != final_bpm:
                state["bpm"] = final_bpm
                state["version"] += 1
        log("META", f"BPM for '{artist} - {title}': {final_bpm if final_bpm is not None else 'n/a'}")
    finally:
        _bpm_in_flight.discard(cache_key)


def record_history_locked(key, title, artist, art_url, album=None, played_at=None):
    global history_version
    if not key or not title:
        return False
    ts = float(played_at if played_at is not None else time.time())
    iso_ts = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()
    existing = list(play_history)
    if existing and existing[0].get("key") == key:
        if album and not existing[0].get("album"):
            existing[0]["album"] = str(album)
            play_history.clear()
            play_history.extend(existing)
            history_version += 1
        return False
    filtered = [item for item in existing if item.get("key") != key]
    entry = {
        "key": str(key),
        "title": str(title),
        "artist": str(artist or ""),
        "album": str(album).strip() if album and str(album).strip() else None,
        "art": str(art_url) if art_url else None,
        "played_at": ts,
        "played_at_iso": iso_ts,
    }
    play_history.clear()
    play_history.append(entry)
    for item in filtered[: MAX_HISTORY_ENTRIES - 1]:
        play_history.append(item)
    history_version += 1
    return True


def notify_listener():
    if wake_event is not None and _loop is not None:
        try:
            _loop.call_soon_threadsafe(wake_event.set)
        except Exception:
            pass


N_BANDS = 48
FFT_SIZE = 2048
_edges = np.geomspace(40, 12000, N_BANDS + 1)
_freqs = np.fft.rfftfreq(FFT_SIZE, 1 / RATE)
_band_slices = []
for _i in range(N_BANDS):
    _a = int(np.searchsorted(_freqs, _edges[_i]))
    _b = max(int(np.searchsorted(_freqs, _edges[_i + 1])), _a + 1)
    _band_slices.append((_a, _b))
_window = np.hanning(FFT_SIZE).astype(np.float32)
_tilt = np.linspace(0, 14, N_BANDS)

audio_lock = threading.Lock()
audio_buffer = collections.deque(maxlen=int((SAMPLE_SECONDS + 8) * RATE / BLOCK) + 4)
_tail = np.zeros(FFT_SIZE, dtype=np.float32)
_smooth = np.zeros(N_BANDS)
_last_push = 0.0
_last_loud = 0.0
_last_rms = 0.0
_rms_avg = 0.0
_quiet_blocks = 0
_silence_since = None
_song_ended = False
_last_buf_log_at = 0.0
_last_skip_reason_log = {}
viz = {"bands": [0.0] * N_BANDS}

_INACTIVITY_SECONDS = 30.0
_SUSTAINED_WINDOW_SECONDS = 2.0
_MIN_SUSTAINED_ACTIVE_RATIO = 0.68
_MUSIC_QUIET_HOLD_SECONDS = 0.65
_MUSIC_RMS_FLOOR = 0.0020
_MUSIC_MIN_PERIODICITY = 0.20
_MUSIC_MAX_SPECTRAL_FLATNESS = 0.68
_LYRICS_RECHECK_SYNCED_SECONDS = 900.0
_LYRICS_RECHECK_UNSYNCED_SECONDS = 240.0
_lyrics_recheck_at = {}
_last_scan_at = 0.0
_paused_position = None

_music_window = np.hanning(FFT_SIZE).astype(np.float64)
_inactivity_lock = threading.Lock()
_boot_now = time.time()
_last_meaningful_music_at = _boot_now - _INACTIVITY_SECONDS
_music_last_confident_at = 0.0
_music_state = "none"
_music_last_score = 0.0
_music_feature_history = collections.deque(maxlen=140)
_music_prev_mag = None
_manual_dismiss_cooldown_until = 0.0


def _log_throttled(key, tag, msg, interval_sec=10.0):
    now = time.time()
    prev = _last_skip_reason_log.get(key, 0.0)
    if (now - prev) >= interval_sec:
        _last_skip_reason_log[key] = now
        log(tag, msg)
        return True
    return False


def _reset_music_detector_state():
    global _music_last_confident_at, _music_state, _music_last_score
    global _music_feature_history, _music_prev_mag
    with _inactivity_lock:
        _music_last_confident_at = 0.0
        _music_state = "none"
        _music_last_score = 0.0
        _music_feature_history.clear()
        _music_prev_mag = None


def _evaluate_sustained_audio(mono, now):
    global _music_prev_mag

    x = np.asarray(mono, dtype=np.float64)
    if x.size < 128:
        return 0.0, "none", False

    x = x - float(np.mean(x))
    rms = float(np.sqrt(np.mean(x * x)))
    peak = float(np.max(np.abs(x)))

    if x.size >= FFT_SIZE:
        seg = x[-FFT_SIZE:] * _music_window
    else:
        pad = np.zeros(FFT_SIZE, dtype=np.float64)
        pad[-x.size:] = x * np.hanning(x.size)
        seg = pad

    mag = np.abs(np.fft.rfft(seg))
    # Keyboard taps and handling noise are broadband, short-lived impulses.
    # Music is more likely to contain stable tonal/harmonic structure.
    band = mag[2:min(len(mag), int(8000 * FFT_SIZE / RATE))]
    if band.size:
        arithmetic = float(np.mean(band)) + 1e-12
        geometric = float(np.exp(np.mean(np.log(band + 1e-12))))
        spectral_flatness = geometric / arithmetic
    else:
        spectral_flatness = 1.0
    centered = seg - float(np.mean(seg))
    corr = np.correlate(centered, centered, mode="full")[len(centered)-1:]
    zero = float(corr[0]) + 1e-12
    periodicity = float(np.max(corr[40:401]) / zero) if len(corr) > 401 else 0.0
    structured_audio = (spectral_flatness <= _MUSIC_MAX_SPECTRAL_FLATNESS) or (periodicity >= _MUSIC_MIN_PERIODICITY)
    if _music_prev_mag is not None and len(_music_prev_mag) == len(mag):
        pos_diff = np.maximum(0.0, mag - _music_prev_mag)
        flux = float(np.sum(pos_diff)) / (float(np.sum(mag)) + 1e-12)
    else:
        flux = 0.0
    _music_prev_mag = mag

    above_floor = math.isfinite(rms) and (rms >= _MUSIC_RMS_FLOOR)
    crest = peak / max(rms, 1e-9)
    is_transient = (not above_floor) or (crest > 10.0) or (flux > 0.82 and rms < 0.012) or not structured_audio

    _music_feature_history.append({
        "t": float(now),
        "rms": rms if math.isfinite(rms) else 0.0,
        "sustained_sample": bool(above_floor and not is_transient),
        "flatness": spectral_flatness,
        "periodicity": periodicity,
    })

    window_items = [f for f in _music_feature_history if (now - f["t"]) <= _SUSTAINED_WINDOW_SECONDS]
    if len(window_items) < 6 or (window_items[-1]["t"] - window_items[0]["t"]) < 1.25:
        return 0.0, "none", False

    sustained_ratio = sum(1 for f in window_items if f["sustained_sample"]) / len(window_items)
    is_sustained = sustained_ratio >= _MIN_SUSTAINED_ACTIVE_RATIO
    score = round(min(1.0, sustained_ratio), 2)
    if is_sustained:
        return score, "confident", True
    if sustained_ratio >= 0.35:
        return score, "transient", False
    return score, "none", False


def update_music_activity(mono, now, mic_is_active):
    global _last_meaningful_music_at, _music_last_confident_at, _music_state, _music_last_score

    t = float(now)
    if not mic_is_active:
        with _inactivity_lock:
            _music_state = "none"
            _music_last_score = 0.0
        return False

    score, m_state, is_sustained = _evaluate_sustained_audio(mono, t)

    became_confident = False
    with _inactivity_lock:
        _music_last_score = score
        if is_sustained:
            became_confident = (_music_state != "confident") or (t - _music_last_confident_at > _MUSIC_QUIET_HOLD_SECONDS)
            _music_state = "confident"
            _music_last_confident_at = t
            if t >= _manual_dismiss_cooldown_until:
                _last_meaningful_music_at = t
        elif (t - _music_last_confident_at) <= _MUSIC_QUIET_HOLD_SECONDS and _music_last_confident_at > 0:
            _music_state = "confident"
            if t >= _manual_dismiss_cooldown_until:
                _last_meaningful_music_at = max(_last_meaningful_music_at, _music_last_confident_at)
        else:
            _music_state = m_state

    # Wake recognition as soon as sustained music first appears instead of waiting
    # for the listener's next periodic tick. The listener still enforces Shazam's
    # minimum request spacing and automatic rate-limit cooldown.
    if became_confident:
        notify_listener()
    return bool(is_sustained or (t - _music_last_confident_at <= _MUSIC_QUIET_HOLD_SECONDS and _music_last_confident_at > 0))


def _record_meaningful_music_confirmed(now):
    global _last_meaningful_music_at, _music_last_confident_at, _music_state
    t = float(now)
    with _inactivity_lock:
        if t < _manual_dismiss_cooldown_until:
            return
        _last_meaningful_music_at = t
        _music_last_confident_at = t
        _music_state = "confident"


def _note_manual_screensaver_dismissed(now=None):
    global _last_meaningful_music_at, _music_state, _manual_dismiss_cooldown_until
    t = float(now if now is not None else time.time())
    with _inactivity_lock:
        _last_meaningful_music_at = t
        _music_state = "none"
        _manual_dismiss_cooldown_until = t + 2.0


def _get_music_detection_snapshot(now):
    t = float(now)
    with _inactivity_lock:
        last_at = _last_meaningful_music_at
        m_state = _music_state
        last_conf_at = _music_last_confident_at
        sc = round(float(_music_last_score), 2)
    idle_for = max(0.0, t - last_at)
    confident_now = (m_state == "confident") and ((t - last_conf_at) <= _MUSIC_QUIET_HOLD_SECONDS)
    return {
        "music_state": m_state,
        "meaningful_audio": confident_now,
        "music_confirmed": confident_now,
        "music_score": sc,
        "idle_seconds": round(idle_for, 2),
        "screensaver_due": (not confident_now) and (idle_for >= _INACTIVITY_SECONDS),
        "threshold_seconds": _INACTIVITY_SECONDS,
    }


def update_spectrum(mono):
    global _tail, _smooth
    _tail = np.concatenate([_tail, mono])[-FFT_SIZE:]
    spec = np.abs(np.fft.rfft(_tail * _window)) / (FFT_SIZE / 4)
    peaks = np.array([spec[a:b].max() for a, b in _band_slices])
    db = 20 * np.log10(peaks + 1e-6) + _tilt
    target = np.clip((db + 78) / 56, 0, 1)
    rate = np.where(target > _smooth, 0.65, 0.14)
    _smooth = _smooth + (target - _smooth) * rate
    viz["bands"] = np.round(_smooth, 2).tolist()


def mic_has_live_audio():
    with mic_lock:
        mic_ok = mic_status_state.get("status") == "active"
    if not mic_ok:
        return False
    if (time.time() - _last_push) > 1.5:
        return False
    return _last_rms >= _MUSIC_RMS_FLOOR


def audio_active():
    with mic_lock:
        mic_ok = mic_status_state.get("status") == "active"
    if not mic_ok or (time.time() - _last_push) > 1.5:
        return False
    with _inactivity_lock:
        sustained_now = (_music_state == "confident") or (
            _music_last_confident_at > 0 and (time.time() - _music_last_confident_at) <= _MUSIC_QUIET_HOLD_SECONDS
        )
    return sustained_now


def current_status():
    with state_lock:
        st = state["status"]
        has_track = state["key"] is not None
        ended = bool(state.get("song_ended"))
    if st in ("playing", "identifying", "refreshing") and not audio_active():
        if has_track and (_song_ended or ended):
            return "idle"
        if has_track and _silence_since is not None and (time.time() - _silence_since) >= 1.5:
            return "idle"
        return "listening" if not has_track else st
    return st


def detect_change(mono, now, gap):
    global _rms_avg, _quiet_blocks
    rms = float(np.sqrt(np.mean(mono * mono))) if len(mono) else 0.0
    if rms < max(_MUSIC_RMS_FLOOR, _rms_avg * 0.14):
        _quiet_blocks += 1
        return
    _quiet_blocks = 0
    _rms_avg = rms if _rms_avg == 0 else _rms_avg + (rms - _rms_avg) * 0.03


def push_block(mono):
    global _last_push, _last_loud, _last_rms, _silence_since, _song_ended, _last_buf_log_at, _paused_position
    mono = np.asarray(mono, dtype=np.float32)
    now = time.time()
    if _last_push > 0:
        detect_change(mono, now, now - _last_push)
    _last_push = now
    rms = float(np.sqrt(np.mean(mono * mono))) if len(mono) else 0.0
    _last_rms = rms
    with mic_lock:
        _mic_is_active = (mic_status_state.get("status") == "active")

    sustained_music = update_music_activity(mono, now, _mic_is_active)
    snap = _get_music_detection_snapshot(now)
    _update_mic_signal_level(rms, sustained_music, snap["music_state"], snap["music_score"])

    if sustained_music:
        _last_loud = now
        if _song_ended:
            _song_ended = False
            _silence_since = None
            with state_lock:
                has_track = state["key"] is not None
                state["song_ended"] = False
                if has_track and _paused_position is not None:
                    state["start_epoch"] = time.time() - _paused_position
                    _paused_position = None
                state["status"] = "playing" if has_track else "listening"
                state["version"] += 1
            log("RESUME", "Sustained audio resumed after pause/stop; waking constant Shazam loop")
            notify_listener()
        else:
            _silence_since = None
    else:
        if _silence_since is None:
            _silence_since = now
        with state_lock:
            has_track = state["key"] is not None
            cur_st = state["status"]
        if has_track and not _song_ended and (now - _silence_since) >= END_SILENCE_SECONDS:
            _trigger_song_ended_signal(f"no sustained audio for {END_SILENCE_SECONDS:.1f}s (rms={rms:.5f})")
        elif not has_track and cur_st not in ("listening", "refreshing"):
            with state_lock:
                state["status"] = "listening"
                state["version"] += 1

    with audio_lock:
        audio_buffer.append((now - len(mono) / RATE, mono))
        buf_samples = sum(len(b) for _, b in audio_buffer)
        buf_sec = buf_samples / RATE

    if (now - _last_buf_log_at) >= 12.0:
        _last_buf_log_at = now
        log(
            "BUFFER",
            f"mic_rx=ok buffered={buf_sec:.1f}s rms={rms:.5f} sustained={sustained_music} "
            f"mstate={snap['music_state']} score={snap['music_score']:.2f}"
        )

    update_spectrum(mono)


def resample(x, src_rate):
    if len(x) == 0:
        return np.zeros(0, dtype=np.float32)
    src_i = int(round(float(src_rate)))
    if src_i == RATE or src_i <= 0:
        return np.asarray(x, dtype=np.float32)
    n = max(1, int(round(len(x) * RATE / src_i)))
    return np.interp(
        np.linspace(0, len(x) - 1, n),
        np.arange(len(x)),
        np.asarray(x, dtype=np.float32),
    ).astype(np.float32)


def _to_mono_normalized(raw_arr, channels):
    arr = np.asarray(raw_arr, dtype=np.float32)
    if arr.size == 0:
        return np.zeros(0, dtype=np.float32)
    if arr.ndim == 2:
        if arr.shape[1] > 1:
            mono = np.mean(arr, axis=1, dtype=np.float32)
        else:
            mono = arr[:, 0]
    else:
        ch = max(1, int(channels or 1))
        if ch > 1 and (len(arr) // ch) > 0:
            mono = arr[: (len(arr) // ch) * ch].reshape(-1, ch).mean(axis=1)
        else:
            mono = arr.reshape(-1)
    mono = np.clip(mono - float(np.mean(mono)), -1.0, 1.0).astype(np.float32)
    return mono


def _wait_or_wake(timeout_sec):
    _mic_wake_event.wait(timeout=max(0.01, float(timeout_sec)))
    _mic_wake_event.clear()


def _run_single_mic_session(target_dev, mode, had_previous_active):
    import sounddevice as sd

    dev_idx = target_dev["index"]
    dev_name = target_dev["name"]
    ch = max(1, min(2, int(target_dev.get("max_input_channels") or 1)))
    native_rate = int(round(float(target_dev.get("default_samplerate") or RATE)))
    if native_rate < 8000:
        native_rate = RATE

    sample_rates_to_try = [native_rate] if native_rate != RATE else [RATE]
    if RATE not in sample_rates_to_try:
        sample_rates_to_try.append(RATE)

    stream = None
    active_sr = None
    last_err = None
    for sr in sample_rates_to_try:
        for try_ch in ([ch, 1] if ch > 1 else [1]):
            try:
                blk = max(256, int(round(BLOCK * sr / RATE)))
                s = sd.InputStream(
                    samplerate=sr,
                    channels=try_ch,
                    dtype="float32",
                    blocksize=blk,
                    device=dev_idx,
                )
                s.start()
                stream = s
                active_sr = sr
                ch = try_ch
                break
            except Exception as exc:
                last_err = exc
        if stream is not None:
            break

    if stream is None:
        raise RuntimeError(f"Failed to open input '{dev_name}' (idx={dev_idx}): {last_err!r}")

    _set_mic_status(
        status="active",
        message=f"Input active: {dev_name}",
        device_name=dev_name,
        mode=mode,
        audio_detected=False,
        music_state="none",
        music_confidence=0.0,
        level=0.0,
    )
    log("MIC", f"Stream started: {dev_name} (idx={dev_idx}, mode={mode}, {active_sr} Hz -> {RATE} Hz, {ch} ch)")

    last_default_check = time.time()
    last_default_idx = None
    first_block_logged = False
    try:
        _, last_default_idx = _query_input_devices()
    except Exception:
        pass

    blk_to_read = max(256, int(round(BLOCK * active_sr / RATE)))
    try:
        while not _mic_stop_event.is_set():
            if _mic_wake_event.is_set():
                _mic_wake_event.clear()
                break

            now = time.time()
            if now - last_default_check >= 2.5:
                last_default_check = now
                try:
                    cur_inputs, cur_def_idx = _query_input_devices()
                    if mode == "default" and last_default_idx is not None and cur_def_idx is not None and cur_def_idx != dev_idx:
                        log("MIC", f"Default input device changed ({dev_idx} -> {cur_def_idx}); switching")
                        break
                    if mode == "manual":
                        resolved_now, _ = _resolve_target_device(cur_inputs, cur_def_idx)
                        if resolved_now is None or resolved_now["index"] != dev_idx:
                            log("MIC", f"Selected input '{dev_name}' changed or disconnected")
                            break
                    last_default_idx = cur_def_idx
                except Exception:
                    pass

            if not stream.active:
                raise RuntimeError(f"Input stream stopped ({dev_name})")

            data, overflow = stream.read(blk_to_read)
            if overflow:
                _log_throttled("mic_ovf", "WARN", f"Input ring-buffer overflow on '{dev_name}'", interval_sec=10.0)
            mono = _to_mono_normalized(data, ch)
            if len(mono):
                if not first_block_logged:
                    first_block_logged = True
                    log("MIC", f"Receiving input audio blocks from '{dev_name}' ({len(mono)} samples/block)")
                push_block(resample(mono, active_sr))

            if (time.time() - _last_push) > 0.15:
                update_spectrum(np.zeros(BLOCK, dtype=np.float32))
    finally:
        try:
            stream.stop()
        except Exception:
            pass
        try:
            stream.close()
        except Exception:
            pass
        _update_mic_signal_level(0.0, False, "none", 0.0)


def capture_mic_pipeline():
    global _last_loud
    was_active_before = False
    consecutive_failures = 0

    while not _mic_stop_event.is_set():
        with mic_lock:
            mode = mic_pref.get("mode", "default")
            pref_name = mic_pref.get("name")

        try:
            inputs, default_idx = _query_input_devices()
        except Exception as exc:
            consecutive_failures += 1
            st_lbl = "reconnecting" if was_active_before else "unavailable"
            _set_mic_status(
                status=st_lbl,
                message=f"Input query failed ({exc!r}); retrying...",
                device_name=None,
                mode=mode,
                audio_detected=False,
                music_state="none",
                music_confidence=0.0,
                level=0.0,
            )
            update_spectrum(np.zeros(BLOCK, dtype=np.float32))
            _wait_or_wake(2.0)
            continue

        target_dev, reason = _resolve_target_device(inputs, default_idx)
        if target_dev is None:
            consecutive_failures += 1
            st_lbl = "disconnected" if was_active_before else "unavailable"
            msg = reason or (f"{pref_name} unavailable" if pref_name else "Input unavailable")
            _set_mic_status(
                status=st_lbl,
                message=msg,
                device_name=None,
                mode=mode,
                audio_detected=False,
                music_state="none",
                music_confidence=0.0,
                level=0.0,
            )
            update_spectrum(np.zeros(BLOCK, dtype=np.float32))
            _wait_or_wake(1.5)
            continue

        log("MIC_SEL", f"Resolved input -> [{target_dev['index']}] {target_dev['name']} ({target_dev.get('hostapi')}, mode={mode})")
        _set_mic_status(
            status="reconnecting" if was_active_before else "initializing",
            message=f"Reconnecting to {target_dev['name']}..." if was_active_before else f"Initializing {target_dev['name']}...",
            device_name=target_dev["name"],
            mode=mode,
            audio_detected=False,
            music_state="none",
            music_confidence=0.0,
            level=0.0,
        )

        try:
            _run_single_mic_session(target_dev, mode, was_active_before)
            was_active_before = True
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            err_st = "reconnecting" if was_active_before else "unavailable"
            err_msg = (
                f"Input interrupted ({target_dev['name']}); reconnecting..."
                if was_active_before
                else f"Input unavailable ({target_dev['name']})"
            )
            _set_mic_status(
                status=err_st,
                message=err_msg,
                device_name=None if err_st == "unavailable" else target_dev["name"],
                mode=mode,
                audio_detected=False,
                music_state="none",
                music_confidence=0.0,
                level=0.0,
            )
            log("ERROR", f"Input capture error on '{target_dev['name']}': {exc!r}")
            update_spectrum(np.zeros(BLOCK, dtype=np.float32))
            retry_delay = min(4.0, 1.5 + 0.5 * min(consecutive_failures, 6))
            _wait_or_wake(retry_delay)


def capture_thread():
    load_saved_mic_pref()
    capture_mic_pipeline()


def snapshot_sample(seconds=None):
    need = int((seconds or SAMPLE_SECONDS) * RATE)
    with audio_lock:
        blocks = list(audio_buffer)
    picked, total, prev_start = [], 0, None
    for start, b in reversed(blocks):
        if prev_start is not None and prev_start - (start + len(b) / RATE) > 0.5:
            break
        picked.append((start, b))
        total += len(b)
        prev_start = start
        if total >= need:
            break
    if total < max(1, int(0.5 * RATE)):
        return None
    picked.reverse()
    samples = np.concatenate([b for _, b in picked])[-need:]
    start = picked[0][0] + max(0.0, (total - len(samples)) / RATE)
    peak = float(np.max(np.abs(samples)))
    rms = float(np.sqrt(np.mean(samples * samples)))
    gain = 1.0
    if 0.0001 < peak < 0.12 and rms > 0.0001:
        gain = min(6.0, 0.30 / max(peak, 1e-4))
    scaled = np.clip(samples.astype(np.float32) * gain, -1.0, 1.0)
    pcm = (scaled * 32767).astype(np.int16)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(RATE)
        w.writeframes(pcm.tobytes())
    return start, buf.getvalue(), samples


LRC_STAMP = re.compile(r"\[(\d+):(\d+(?:\.\d+)?)\]")


def parse_lrc(text):
    lines, synced = [], False
    for raw in text.splitlines():
        stamps = LRC_STAMP.findall(raw)
        content = LRC_STAMP.sub("", raw).strip()
        if stamps:
            synced = True
            for m, s in stamps:
                lines.append({"t": int(m) * 60 + float(s), "text": content})
        elif content and not re.match(r"^\[[a-z]+:", content):
            lines.append({"t": None, "text": content})
    if synced:
        lines = [l for l in lines if l["t"] is not None]
        lines.sort(key=lambda l: l["t"])
    return lines, synced


def fetch_lyrics(title, artist):
    clean_title = re.sub(r"\s*\([^)]*\)|\s*\[[^\]]*\]", "", str(title or "")).strip() or str(title or "")
    queries = [f"{title} {artist}".strip()]
    if clean_title and clean_title.lower() != str(title or "").strip().lower():
        queries.append(f"{clean_title} {artist}".strip())
    for q in queries:
        for synced_only in (True, False):
            mode_lbl = "synced" if synced_only else "plain"
            try:
                result = syncedlyrics.search(q, synced_only=synced_only)
            except Exception as exc:
                log("LYRICS_PRV", f"query='{q}' mode={mode_lbl} -> ERROR: {exc!r}")
                result = None
            if result:
                lines, synced = parse_lrc(result)
                log("LYRICS_PRV", f"query='{q}' mode={mode_lbl} -> HIT ({len(lines)} lines, synced={synced})")
                return lines, synced
            else:
                log("LYRICS_PRV", f"query='{q}' mode={mode_lbl} -> empty")
    log("LYRICS_PRV", f"No provider returned lyrics for title='{title}' artist='{artist}'")
    return [], False


async def update_lyrics_async(loop, req_gen, key, title, artist, reason="new song"):
    log("LYRICS", f"Lookup started for '{artist} - {title}' ({reason}, gen={req_gen})")
    t0 = time.time()
    lyrics, synced = await loop.run_in_executor(None, fetch_lyrics, title, artist)
    elapsed = time.time() - t0
    with state_lock:
        if state["key"] != key or generation_counter != req_gen:
            log("LYRICS", f"Discarded stale lyrics for '{artist} - {title}' (gen={req_gen}, current={generation_counter})")
            return False
        cur_pos = (time.time() - state["start_epoch"]) if state.get("start_epoch") is not None else 0.0
        state.update(
            lyrics=lyrics,
            synced=synced,
            lyrics_searched=True,
            version=state["version"] + 1,
        )
    log("LYRICS", f"Lookup finished in {elapsed:.2f}s: {len(lyrics)} lines (synced={synced}) at pos={cur_pos:.2f}s")
    _lyrics_recheck_at[str(key)] = time.time() + (_LYRICS_RECHECK_SYNCED_SECONDS if synced else _LYRICS_RECHECK_UNSYNCED_SECONDS)
    return True


async def recheck_lyrics_async(loop, req_gen, key, title, artist):
    """Occasionally verify lyric timing; only replace good synced lyrics with better synced data."""
    started = time.time()
    try:
        fresh_lines, fresh_synced = await loop.run_in_executor(None, fetch_lyrics, title, artist)
    except Exception as exc:
        log("LYRICS", f"Periodic verification failed: {exc!r}")
        _lyrics_recheck_at[str(key)] = time.time() + 180.0
        return
    with state_lock:
        if state.get("key") != key or generation_counter != req_gen:
            return
        old_lines = state.get("lyrics") or []
        old_synced = bool(state.get("synced"))
        # Prefer time-coded lyrics; do not replace a useful synced set with plain text.
        replace = bool(fresh_lines) and ((fresh_synced and not old_synced) or (fresh_synced and len(fresh_lines) > len(old_lines) * 1.15))
        if replace:
            state.update(lyrics=fresh_lines, synced=True, lyrics_searched=True, version=state["version"] + 1)
    delay = _LYRICS_RECHECK_SYNCED_SECONDS if (fresh_synced or old_synced) else _LYRICS_RECHECK_UNSYNCED_SECONDS
    _lyrics_recheck_at[str(key)] = time.time() + delay
    log("LYRICS", f"Periodic timing check complete in {time.time()-started:.1f}s: provider_synced={fresh_synced}, replaced={replace}")


def _get_shazam():
    global _shazam_client
    if _shazam_client is None:
        _shazam_client = Shazam(
            http_client=HTTPClient(
                retry_options=ExponentialRetry(
                    attempts=1,
                    max_timeout=3,
                    statuses={500, 502, 503, 504},
                )
            )
        )
    return _shazam_client


async def _close_shazam():
    global _shazam_client
    client = _shazam_client
    _shazam_client = None
    if client is not None:
        for attr in ("http_client", "_http_client", "session", "_session"):
            obj = getattr(client, attr, None)
            if obj is not None and hasattr(obj, "close"):
                try:
                    res = obj.close()
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass


async def execute_shazam_scan(*, forced=False, label="CONSTANT SCAN"):
    """
    Runs a single Shazam recognition scan using a single persistent client
    without ever hanging or leaving state stuck on 'refreshing'.
    """
    global generation_counter, _song_ended, _silence_since

    async with _scan_lock:
        now = time.time()
        cooldown_until = float(rate_limit_state.get("until") or 0.0)
        # Manual searches explicitly bypass automatic cooldown and spacing gates.
        if not forced:
            if now < cooldown_until:
                retry_after = max(1, int(cooldown_until - now + 0.999))
                log("SHAZAM", f"[{label}] Automatic scan suppressed by rate-limit cooldown ({retry_after}s remaining)")
                return {"outcome": "rate_limited", "message": f"Shazam is cooling down. Automatic retry in {retry_after} seconds.", "retry_after": retry_after}
            last_request = float(rate_limit_state.get("last_request") or 0.0)
            since_last = now - last_request
            if last_request and since_last < SHAZAM_MIN_INTERVAL_SECONDS:
                retry_after = max(1, int(SHAZAM_MIN_INTERVAL_SECONDS - since_last + 0.999))
                return {"outcome": "throttled", "message": f"Please wait {retry_after} seconds before scanning again.", "retry_after": retry_after}

        with state_lock:
            has_key_before = state["key"] is not None
            if forced:
                state["status"] = "refreshing"
                state["version"] += 1

        try:
            seconds = FAST_SAMPLE_SECONDS if forced else SAMPLE_SECONDS
            sample = snapshot_sample(seconds)
            if sample is None:
                log("SHAZAM", f"[{label}] Skipped: audio buffer empty/too short")
                return {"outcome": "no_audio", "message": "No mic audio captured yet."}

            started, wav, heard_samples = sample
            max_amp = float(np.abs(heard_samples).max())
            if max_amp <= 0.0 and not forced:
                return {"outcome": "silent", "message": "Silent mic sample."}

            log("SHAZAM", f"[{label}] Scanning {len(heard_samples)/RATE:.1f}s audio (rms={_last_rms:.5f}, peak={max_amp:.4f})")

            shazam = _get_shazam()
            rate_limit_state["last_request"] = time.time()
            try:
                result = await asyncio.wait_for(shazam.recognize(wav), timeout=SHAZAM_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                log("SHAZAM", f"[{label}] Timed out after {SHAZAM_TIMEOUT_SECONDS:.0f}s")
                return {"outcome": "error", "message": "Shazam timed out."}
            except Exception as exc:
                exc_text = str(exc).lower()
                is_rl = (
                    type(exc).__name__ == "RateLimited"
                    or "rate limited" in exc_text
                    or "too many requests" in exc_text
                    or "429" in exc_text
                )
                if is_rl:
                    strikes = int(rate_limit_state.get("strikes") or 0) + 1
                    rate_limit_state["strikes"] = strikes
                    # Start near the minimum safe request spacing, then back off
                    # exponentially only when Shazam continues returning 429s.
                    cooldown = min(
                        RATE_LIMIT_MAX_COOLDOWN_SECONDS,
                        RATE_LIMIT_COOLDOWN_SECONDS * (2 ** min(strikes - 1, 6)),
                    )
                    rate_limit_state["until"] = time.time() + cooldown
                    log("SHAZAM", f"[{label}] Rate-limited; {cooldown:.0f}s automatic cooldown (strike {strikes})")
                    return {"outcome": "rate_limited", "message": f"Shazam rate-limited requests. Automatic scans resume in {int(cooldown)} seconds; manual search remains available.", "retry_after": int(cooldown)}
                log("SHAZAM", f"[{label}] Error: {exc!r}")
                return {"outcome": "error", "message": f"Shazam error: {type(exc).__name__}"}

            if not isinstance(result, dict):
                if hasattr(result, "model_dump"):
                    result = result.model_dump()
                elif hasattr(result, "dict"):
                    result = result.dict()
                else:
                    result = {}

            matches = result.get("matches") or []
            track = result.get("track")
            if not matches or not track:
                log("SHAZAM", f"[{label}] No match")
                # A failed automatic constant scan means the display should sleep.
                # Audio activity alone must never wake it; only a real match does.
                if label == "CONSTANT SCAN":
                    _emit_ws_event("scan_no_match")
                return {"outcome": "no_match", "message": "No song match found."}

            rate_limit_state["until"] = 0.0
            rate_limit_state["strikes"] = 0
            _song_ended = False
            _silence_since = None
            global _paused_position
            _paused_position = None

            offset_ms = matches[0].get("offset", 0)
            offset = float(offset_ms or 0)
            title = track.get("title")
            artist = track.get("subtitle")
            raw_key = track.get("key")
            norm_title_artist = f"{_clean_match_str(artist)}::{_clean_match_str(title)}" if (title or artist) else None
            key = str(raw_key) if raw_key else (norm_title_artist or f"{title or ''}-{artist or ''}")
            # Switch on the first complete Shazam match for a different track.
            # The recognizer has already returned a structured track + match; waiting
            # for a second request delayed legitimate song changes and wasted quota.
            album = extract_album_name(track)
            embedded_bpm = extract_bpm_from_track(track)
            # Shazam's `images.coverarthq` / `images.coverart` are the actual
            # square release covers. `photo_url` can be a portrait/artist image,
            # which was being mistaken for the album jacket in some matches.
            images = track.get("images") if isinstance(track.get("images"), dict) else {}
            art_url = images.get("coverart") or images.get("coverarthq")

            confirmed_at = time.time()
            _record_meaningful_music_confirmed(confirmed_at)
            transit_elapsed = max(0.0, confirmed_at - started)
            live_pos = max(0.0, offset + transit_elapsed)
            computed_start_epoch = confirmed_at - live_pos
            cached_bpm = embedded_bpm if embedded_bpm is not None else _bpm_cache.get(str(key))

            with state_lock:
                prev_key = state["key"]
                prev_norm = f"{_clean_match_str(state.get('artist'))}::{_clean_match_str(state.get('title'))}"
                is_new_song = (key != prev_key) and (
                    not norm_title_artist or norm_title_artist != prev_norm or not prev_key
                )
                state["status"] = "playing"
                state["song_ended"] = False

                if is_new_song or forced:
                    state["start_epoch"] = computed_start_epoch
                    if is_new_song:
                        record_history_locked(key, title, artist, art_url, album=album, played_at=confirmed_at)
                    generation_counter += 1
                    track_gen = generation_counter
                    state.update(
                        key=key,
                        title=title,
                        artist=artist,
                        album=album if album is not None else (state["album"] if not is_new_song else None),
                        bpm=cached_bpm,
                        art=art_url,
                        lyrics=[],
                        synced=False,
                        lyrics_searched=False,
                        version=state["version"] + 1,
                    )
                else:
                    track_gen = generation_counter
                    cur_start = state.get("start_epoch")
                    if cur_start is None:
                        state["start_epoch"] = computed_start_epoch
                    else:
                        # Treat Shazam's match offset as a periodic timing reference, not a hard reset.
                        # Small corrections are blended in to prevent visible lyric jumps.
                        drift = computed_start_epoch - cur_start
                        if abs(drift) >= 1.25:
                            correction = max(-0.45, min(0.45, drift * 0.22))
                            state["start_epoch"] = cur_start + correction
                            log("SYNC", f"Playback clock nudged {correction:+.2f}s (estimated drift {drift:+.2f}s)")
                    if album is not None and not state.get("album"):
                        state["album"] = album
                    if cached_bpm is not None and state.get("bpm") is None:
                        state["bpm"] = cached_bpm
                    state["version"] += 1

            loop = asyncio.get_running_loop()
            if is_new_song:
                log("NEW_SONG", f"New song detected: {artist} - {title} (key={key})")
                _emit_ws_event("song_changed", {"key": key, "title": title, "artist": artist})
                asyncio.create_task(update_lyrics_async(loop, track_gen, key, title, artist, reason="new song"))
                asyncio.create_task(update_track_bpm_async(track_gen, key, title, artist, embedded_bpm=embedded_bpm))
            elif forced:
                log("REIDENT", f"Re-identified track: {artist} - {title} (key={key})")
                asyncio.create_task(update_lyrics_async(loop, track_gen, key, title, artist, reason="manual re-identify"))
                asyncio.create_task(update_track_bpm_async(track_gen, key, title, artist, embedded_bpm=embedded_bpm))
            else:
                log("SHAZAM", f"[{label}] Confirmed playing: {artist} - {title} at {live_pos:.1f}s")
                with state_lock:
                    has_lyrics_now = bool(state["lyrics"])
                    searched_now = bool(state.get("lyrics_searched"))
                    has_bpm_checked = (_bpm_cache.get(str(key)) is not None) or (state.get("bpm") is not None)
                if not has_lyrics_now and not searched_now:
                    asyncio.create_task(update_lyrics_async(loop, track_gen, key, title, artist, reason="initial lyrics lookup"))
                if not has_bpm_checked:
                    asyncio.create_task(update_track_bpm_async(track_gen, key, title, artist, embedded_bpm=embedded_bpm))
                if time.time() >= _lyrics_recheck_at.get(str(key), 0.0):
                    _lyrics_recheck_at[str(key)] = time.time() + 90.0  # reserve the slot while the provider check runs
                    asyncio.create_task(recheck_lyrics_async(loop, track_gen, key, title, artist))

            # Emit last so the websocket's compact event field cannot overwrite
            # the positive identification with the accompanying song_changed event.
            _emit_ws_event("song_identified", {"key": key, "title": title, "artist": artist})
            return {"outcome": "matched", "title": title, "artist": artist, "key": key, "is_new_song": is_new_song}
        finally:
            with state_lock:
                if state["status"] == "refreshing":
                    if not audio_active():
                        state["status"] = "idle" if (has_key_before and (_song_ended or state.get("song_ended"))) else ("playing" if has_key_before else "listening")
                    else:
                        state["status"] = "playing" if has_key_before else "listening"
                    state["version"] += 1


async def listen_loop():
    """
    Constant Shazam loop: wakes every CONSTANT_SHAZAM_SECONDS and scans whenever live
    mic audio is above the noise floor.
    """
    global _loop, wake_event
    _loop = asyncio.get_running_loop()
    wake_event = asyncio.Event()
    log("READY", f"Constant Shazam loop active every {CONSTANT_SHAZAM_SECONDS:.1f}s (sample={SAMPLE_SECONDS:.1f}s)")

    try:
        while True:
            try:
                await asyncio.wait_for(wake_event.wait(), timeout=CONSTANT_SHAZAM_SECONDS)
                wake_event.clear()
            except asyncio.TimeoutError:
                pass

            now = time.time()
            if now < float(rate_limit_state.get("until") or 0.0):
                continue

            # Ignore raw microphone activity until the local detector sees sustained,
            # structured music. This prevents keyboard clicks from triggering Shazam.
            if not audio_active():
                quiet_logged = _log_throttled(
                    "quiet_skip",
                    "SHAZAM",
                    f"Constant scan waiting: mic quiet (rms={_last_rms:.5f} < {_MUSIC_RMS_FLOOR:.5f})",
                    interval_sec=12.0,
                )
                if quiet_logged:
                    _emit_ws_event("scan_quiet")
                continue

            # A sustained-audio scan interrupts any consecutive quiet-log sequence.
            _emit_ws_event("scan_active")
            global _last_scan_at
            # Recheck active playback frequently enough to catch track changes
            # promptly, without violating Shazam's minimum request spacing.
            # Music-onset notifications bypass the idle wait for the first scan.
            with state_lock:
                track_is_ended = bool(state.get("song_ended"))
                track_is_known = bool(state.get("key"))
            # A finished track is treated as a fresh listening session: on the
            # next onset use the normal recognition cadence, not active-track
            # polling. While a track is playing, keep periodic change detection.
            cadence = (
                CONSTANT_SHAZAM_SECONDS
                if (not track_is_known or track_is_ended)
                else max(SHAZAM_MIN_INTERVAL_SECONDS + 1.0, CONSTANT_SHAZAM_SECONDS)
            )
            remaining = cadence - (now - _last_scan_at)
            if remaining > 0.25:
                try:
                    await asyncio.wait_for(wake_event.wait(), timeout=remaining)
                    wake_event.clear()
                except asyncio.TimeoutError:
                    pass
                if not audio_active():
                    continue
            outcome = await execute_shazam_scan(forced=False, label="CONSTANT SCAN")
            if outcome.get("outcome") in ("matched", "candidate", "no_match", "error", "rate_limited"):
                _last_scan_at = time.time()
    finally:
        await _close_shazam()


async def index(_request):
    return web.FileResponse(HERE / "index.html")


async def get_state(_request):
    with state_lock:
        position = None
        if state.get("song_ended") and _paused_position is not None:
            position = _paused_position
        elif state["start_epoch"] is not None:
            position = max(0.0, time.time() - state["start_epoch"])
        payload = {k: v for k, v in state.items() if k != "start_epoch"}
        h_ver = history_version
    with mic_lock:
        mic_snap = dict(mic_status_state)
    st_now = current_status()
    payload["status"] = st_now
    payload["position"] = position
    inact = _get_music_detection_snapshot(time.time())
    payload["history_version"] = h_ver
    payload["mic"] = mic_snap
    payload["inactivity"] = inact
    return web.json_response(payload)


async def post_screensaver_dismiss(request):
    try:
        if request.can_read_body:
            await request.json()
    except Exception:
        pass
    _note_manual_screensaver_dismissed(time.time())
    return web.json_response({"ok": True, "inactivity": _get_music_detection_snapshot(time.time())})


async def get_devices(_request):
    payload = await asyncio.get_running_loop().run_in_executor(None, get_devices_payload)
    return web.json_response(payload)


async def post_select_device(request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid JSON body."}, status=400)
    if not isinstance(body, dict):
        return web.json_response({"ok": False, "error": "Request body must be an object."}, status=400)
    mode = body.get("mode", "default")
    dev_name = body.get("name")
    hostapi = body.get("hostapi")
    dev_index = body.get("index")
    try:
        payload = await asyncio.get_running_loop().run_in_executor(
            None, set_microphone_selection, mode, dev_name, hostapi, dev_index
        )
        return web.json_response(payload)
    except ValueError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        log("ERROR", f"Failed to select input device: {exc!r}")
        return web.json_response({"ok": False, "error": "Failed to switch input device."}, status=500)


async def get_history(_request):
    with state_lock:
        items = list(play_history)
        h_ver = history_version
    return web.json_response({
        "ok": True,
        "history_version": h_ver,
        "max_entries": MAX_HISTORY_ENTRIES,
        "items": items,
    })


async def refresh(_request):
    log("WEB", "Manual re-identify requested")
    res = await execute_shazam_scan(forced=True, label="MANUAL RE-IDENTIFY")
    with state_lock:
        state["last_reidentify"] = {**res, "ts": time.time()}
        state["version"] += 1
        cur_status = current_status()
        title = state.get("title")
        artist = state.get("artist")
    outcome = res.get("outcome")
    ok = outcome == "matched"
    return web.json_response({
        "ok": ok,
        "outcome": outcome,
        "message": res.get("message") or (f"Identified {title} — {artist}." if ok else "No new song was identified."),
        "retry_after": res.get("retry_after"),
        "status": cur_status,
        "title": title,
        "artist": artist,
    }, status=429 if outcome == "rate_limited" else (429 if outcome == "throttled" else 200))


async def refresh_lyrics(_request):
    global lyrics_refresh_counter
    with state_lock:
        title, artist, key = state["title"], state["artist"], state["key"]
        if not title or not artist or not key:
            log("WEB", "Manual lyrics refresh ignored: no active song")
            return web.json_response(
                {"ok": False, "error": "Identify a song before refreshing its lyrics."},
                status=409,
            )
        lyrics_refresh_counter += 1
        request_id = lyrics_refresh_counter
    log("LYRICS", f"Manual refresh requested for '{artist} - {title}' (req={request_id})")
    loop = asyncio.get_running_loop()
    t0 = time.time()
    try:
        lyrics, synced = await loop.run_in_executor(None, fetch_lyrics, title, artist)
    except Exception as exc:
        log("ERROR", f"Manual lyrics refresh failed: {exc!r}")
        return web.json_response(
            {"ok": False, "error": f"Lyrics lookup failed: {type(exc).__name__}"},
            status=502,
        )
    elapsed = time.time() - t0
    with state_lock:
        if state["key"] != key or lyrics_refresh_counter != request_id:
            log("LYRICS", f"Discarded stale manual lyrics refresh for '{artist} - {title}'")
            return web.json_response(
                {"ok": False, "error": "The song changed or a newer refresh started. Try again."},
                status=409,
            )
        state.update(lyrics=lyrics, synced=synced, lyrics_searched=True, version=state["version"] + 1)
    log("LYRICS", f"Manual refresh finished in {elapsed:.2f}s: {len(lyrics)} lines (synced={synced})")
    return web.json_response({
        "ok": True, "count": len(lyrics), "synced": synced,
        "message": f"Found {len(lyrics)} {'synced' if synced else 'plain'} lines." if lyrics else "No lyrics were found for this track."
    })


ART_HOSTS = ("mzstatic.com", "shazam.com")
_art_cache = collections.OrderedDict()


async def art_proxy(request):
    url = request.query.get("u", "").strip()
    if not url or len(url) > 2048:
        raise web.HTTPBadRequest(text="invalid image URL")
    u = urlparse(url)
    host = (u.hostname or "").lower()
    if u.scheme != "https" or not any(host == h or host.endswith("." + h) for h in ART_HOSTS):
        raise web.HTTPBadRequest(text="unsupported image host")
    if url not in _art_cache:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout, headers={
                "User-Agent": "Mozilla/5.0 (compatible; NowPlaying/1.0)",
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            }) as sess:
                # Apple/Shazam artwork URLs can redirect to a CDN image host.
                # Follow redirects, but validate the final destination before
                # accepting bytes so the proxy cannot be used as an open fetcher.
                async with sess.get(url, allow_redirects=True, max_redirects=4) as r:
                    final = urlparse(str(r.url))
                    final_host = (final.hostname or "").lower()
                    if final.scheme != "https" or not any(
                        final_host == h or final_host.endswith("." + h) for h in ART_HOSTS
                    ):
                        raise web.HTTPBadGateway(text="cover art redirected to an unsupported host")
                    if r.status != 200:
                        raise web.HTTPBadGateway(text="image fetch failed")
                    # Read the complete response body, not a single partial
                    # StreamReader.read(n) result. A single read may return as
                    # soon as the first network chunk arrives; caching that
                    # chunk made some progressive JPEGs appear cut off halfway
                    # down the image. Stream to EOF and enforce the cap ourselves.
                    if r.content_length is not None and r.content_length > 5_000_000:
                        raise web.HTTPBadGateway(text="image too large")
                    chunks = []
                    total = 0
                    async for chunk in r.content.iter_chunked(64 * 1024):
                        total += len(chunk)
                        if total > 5_000_000:
                            raise web.HTTPBadGateway(text="image too large")
                        chunks.append(chunk)
                    data = b"".join(chunks)
                    if not data:
                        raise web.HTTPBadGateway(text="empty image response")
                    # Some CDN responses label real JPEG/WebP files as
                    # application/octet-stream. Verify the file signature rather
                    # than rejecting valid jacket art based only on that header.
                    if data.startswith(b"\xff\xd8\xff"):
                        ctype = "image/jpeg"
                    elif data.startswith(b"\x89PNG\r\n\x1a\n"):
                        ctype = "image/png"
                    elif data[:6] in (b"GIF87a", b"GIF89a"):
                        ctype = "image/gif"
                    elif len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
                        ctype = "image/webp"
                    elif len(data) >= 12 and data[4:8] == b"ftyp" and data[8:12] in (b"avif", b"avis"):
                        ctype = "image/avif"
                    else:
                        raise web.HTTPBadGateway(text="cover art provider returned invalid image data")
        except web.HTTPException:
            raise
        except Exception as exc:
            log("ERROR", f"Cover art fetch failed: {exc!r}")
            raise web.HTTPBadGateway(text="image fetch failed")
        if len(data) > 5_000_000:
            raise web.HTTPBadGateway(text="image too large")
        _art_cache[url] = (ctype, data)
        while len(_art_cache) > 20:
            _art_cache.popitem(last=False)
    ctype, data = _art_cache[url]
    return web.Response(
        body=data,
        content_type=ctype,
        headers={
            "Cache-Control": "public, max-age=3600, must-revalidate",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def spectrum_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    last_sent_st = None
    last_sent_ver = None
    last_ws_log_at = 0.0
    try:
        while not ws.closed:
            with state_lock:
                ver = state["version"]
                h_ver = history_version
                ended_flag = bool(state.get("song_ended"))
                end_seq = int(state.get("song_end_seq") or 0)
                pos_now = (_paused_position if state.get("song_ended") and _paused_position is not None else (max(0.0, time.time() - state["start_epoch"]) if state["start_epoch"] is not None else None))
            with mic_lock:
                m_st = mic_status_state.get("status")
                m_aud = mic_status_state.get("audio_detected")
                m_dev = mic_status_state.get("device_name")
                m_ver = mic_status_state.get("version")
            st_now = current_status()
            inact_snap = _get_music_detection_snapshot(time.time())
            pending_events = _drain_ws_events()
            now_ws = time.time()
            if (st_now != last_sent_st or ver != last_sent_ver) and (now_ws - last_ws_log_at) >= 0.4:
                last_sent_st = st_now
                last_sent_ver = ver
                last_ws_log_at = now_ws
                log(
                    "WS",
                    f"State update st={st_now} v={ver} hv={h_ver} ended={ended_flag} mstate={inact_snap['music_state']} "
                    f"pos={(f'{pos_now:.2f}s' if pos_now is not None else 'n/a')}"
                )
            msg_payload = {
                "b": viz["bands"],
                "st": st_now,
                "v": ver,
                "hv": h_ver,
                "pos": pos_now,
                "ms": m_st,
                "ma": m_aud,
                "md": m_dev,
                "mv": m_ver,
                "ended": 1 if ended_flag else 0,
                "end_seq": end_seq,
                "ia": 1 if inact_snap["screensaver_due"] else 0,
                "ma_music": 1 if inact_snap["meaningful_audio"] else 0,
                "mstate": inact_snap["music_state"],
                "mscore": inact_snap["music_score"],
            }
            if pending_events:
                msg_payload["ev"] = pending_events[-1]["ev"]
                msg_payload["evs"] = [item["ev"] for item in pending_events]
            await ws.send_str(json.dumps(msg_payload))
            await asyncio.sleep(1 / 30)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    return ws


async def on_startup(app):
    _mic_stop_event.clear()
    t = threading.Thread(target=capture_thread, daemon=True)
    app["mic_thread"] = t
    t.start()
    app["listener"] = asyncio.create_task(listen_loop())


async def on_cleanup(app):
    _mic_stop_event.set()
    _mic_wake_event.set()

    listener = app.get("listener")
    if listener is not None:
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass

    mic_thread = app.get("mic_thread")
    if mic_thread is not None and mic_thread.is_alive():
        await asyncio.to_thread(mic_thread.join, 3.0)

    await _close_shazam()


def main():
    if "--list-devices" in sys.argv:
        try:
            import sounddevice as sd
            inputs, _ = _query_input_devices()
            print("Input devices (max_input_channels > 0):")
            for item in inputs:
                tag = " [DEFAULT]" if item["is_windows_default"] else ""
                print(f"  [{item['index']}] {item['name']} ({item['hostapi']}, {item['max_input_channels']} ch){tag}")
            if not inputs:
                print("  No input devices with max_input_channels > 0 found.")
            print("\nRaw sounddevice query:")
            print(sd.query_devices())
        except Exception as exc:
            print("sounddevice not available:", exc)
        return
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/state", get_state)
    app.router.add_post("/screensaver/dismiss", post_screensaver_dismiss)
    app.router.add_get("/devices", get_devices)
    app.router.add_post("/devices/select", post_select_device)
    app.router.add_get("/history", get_history)
    app.router.add_get("/ws", spectrum_ws)
    app.router.add_get("/art", art_proxy)
    app.router.add_post("/refresh", refresh)
    app.router.add_post("/refresh-lyrics", refresh_lyrics)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    log("SERVER", f"Starting web server at http://localhost:{PORT}")
    if os.getenv("NOWPLAYING_OPEN_BROWSER", "1" if getattr(sys, "frozen", False) else "0") == "1":
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    web.run_app(app, host="127.0.0.1", port=PORT, print=None)


if __name__ == "__main__":
    main()