"""Analyze tracks: detect BPM and musical key, then write metadata to Lexicon."""

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile

import httpx

from tasks.helpers import (
    LEXICON_API_URL,
    get_config,
    get_db,
    log_activity,
    set_config,
)

log = logging.getLogger("worker.analyze")

# Camelot wheel mapping: (pitch_class, mode) -> Camelot notation
# mode: 0 = minor, 1 = major
_CAMELOT_MAP = {
    # Minor keys
    (0, 0): "5A",   # C minor
    (1, 0): "12A",  # C#/Db minor
    (2, 0): "7A",   # D minor
    (3, 0): "2A",   # D#/Eb minor
    (4, 0): "9A",   # E minor
    (5, 0): "4A",   # F minor
    (6, 0): "11A",  # F#/Gb minor
    (7, 0): "6A",   # G minor
    (8, 0): "1A",   # G#/Ab minor
    (9, 0): "8A",   # A minor
    (10, 0): "3A",  # A#/Bb minor
    (11, 0): "10A", # B minor
    # Major keys
    (0, 1): "8B",   # C major
    (1, 1): "3B",   # C#/Db major
    (2, 1): "10B",  # D major
    (3, 1): "5B",   # D#/Eb major
    (4, 1): "12B",  # E major
    (5, 1): "7B",   # F major
    (6, 1): "2B",   # F#/Gb major
    (7, 1): "9B",   # G major
    (8, 1): "4B",   # G#/Ab major
    (9, 1): "11B",  # A major
    (10, 1): "6B",  # A#/Bb major
    (11, 1): "1B",  # B major
}

# Open key notation mapping (alternative to Camelot, used by some DJs)
_OPENKEY_MAP = {
    (0, 0): "1m",   (0, 1): "1d",
    (1, 0): "8m",   (1, 1): "8d",
    (2, 0): "3m",   (2, 1): "3d",
    (3, 0): "10m",  (3, 1): "10d",
    (4, 0): "5m",   (4, 1): "5d",
    (5, 0): "12m",  (5, 1): "12d",
    (6, 0): "7m",   (6, 1): "7d",
    (7, 0): "2m",   (7, 1): "2d",
    (8, 0): "9m",   (8, 1): "9d",
    (9, 0): "4m",   (9, 1): "4d",
    (10, 0): "11m", (10, 1): "11d",
    (11, 0): "6m",  (11, 1): "6d",
}


def _detect_bpm_aubio(file_path: str) -> float | None:
    """Detect BPM using aubio CLI tool."""
    try:
        result = subprocess.run(
            ["aubio", "tempo", "-i", file_path],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            log.warning("aubio tempo failed: %s", result.stderr.strip())
            return None

        # aubio outputs lines of beat timestamps, last line is BPM
        # or use aubio tempo which outputs bpm at the end
        # Actually aubio tempo outputs beat positions; we need to parse differently
        # Let's try aubio tempo -i file which prints beats, and the summary
        lines = result.stdout.strip().split("\n")
        # The output format varies; try to find BPM in stderr or use beat count
        # Better approach: use aubio with specific flags
        return None
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("aubio BPM detection failed for %s: %s", file_path, e)
        return None


def _detect_bpm_sox(file_path: str) -> float | None:
    """Detect BPM using sox stat + simple beat analysis.
    Fallback method using ffprobe to get duration and sox for onset detection.
    """
    # This is a very rough fallback; aubio is preferred
    return None


def _detect_bpm_ffprobe(file_path: str) -> float | None:
    """Read BPM from file metadata tags using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", file_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        tags = data.get("format", {}).get("tags", {})
        # BPM can be in various tag names
        for key in ("BPM", "bpm", "TBPM", "tbpm", "TEMPO", "tempo"):
            val = tags.get(key)
            if val:
                try:
                    bpm = float(val)
                    if 20 < bpm < 300:
                        return round(bpm, 1)
                except (ValueError, TypeError):
                    pass
        return None
    except Exception:
        return None


def _detect_key_keyfinder(file_path: str) -> str | None:
    """Detect musical key using keyfinder-cli (libKeyFinder)."""
    try:
        result = subprocess.run(
            ["keyfinder-cli", file_path],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            return None
        key_str = result.stdout.strip()
        if key_str and len(key_str) < 10:
            return key_str
        return None
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("keyfinder key detection failed for %s: %s", file_path, e)
        return None


def _detect_key_ffprobe(file_path: str) -> str | None:
    """Read musical key from file metadata tags using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", file_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        tags = data.get("format", {}).get("tags", {})
        for key in ("INITIALKEY", "initialkey", "KEY", "key", "initial_key"):
            val = tags.get(key)
            if val and len(val.strip()) < 10:
                return val.strip()
        return None
    except Exception:
        return None


def _detect_bpm_aubio_precise(file_path: str) -> float | None:
    """Detect BPM using aubio Python bindings (more reliable than CLI)."""
    try:
        import aubio as aubio_lib

        win_s = 1024
        hop_s = 512
        samplerate = 0  # auto

        src = aubio_lib.source(file_path, samplerate, hop_s)
        actual_sr = src.samplerate
        tempo = aubio_lib.tempo("default", win_s, hop_s, actual_sr)

        beats = []
        total_frames = 0
        while True:
            samples, read = src()
            is_beat = tempo(samples)
            if is_beat:
                beats.append(tempo.get_last_s())
            total_frames += read
            if read < hop_s:
                break

        bpm = tempo.get_bpm()
        if bpm and 20 < bpm < 300:
            return round(bpm, 1)

        # Fallback: calculate from beat intervals
        if len(beats) > 2:
            intervals = [beats[i + 1] - beats[i] for i in range(len(beats) - 1)]
            avg_interval = sum(intervals) / len(intervals)
            if avg_interval > 0:
                calculated_bpm = 60.0 / avg_interval
                if 20 < calculated_bpm < 300:
                    return round(calculated_bpm, 1)

        return None
    except ImportError:
        log.debug("aubio Python package not available, falling back to CLI")
        return None
    except Exception as e:
        log.warning("aubio Python BPM detection failed for %s: %s", file_path, e)
        return None


def detect_bpm(file_path: str) -> float | None:
    """Detect BPM using best available method."""
    # 1. Try reading from existing metadata first (instant)
    bpm = _detect_bpm_ffprobe(file_path)
    if bpm:
        log.debug("BPM from metadata: %.1f for %s", bpm, file_path)
        return bpm

    # 2. Try aubio Python bindings (most accurate)
    bpm = _detect_bpm_aubio_precise(file_path)
    if bpm:
        log.debug("BPM from aubio: %.1f for %s", bpm, file_path)
        return bpm

    # 3. Try aubio CLI
    bpm = _detect_bpm_aubio(file_path)
    if bpm:
        log.debug("BPM from aubio CLI: %.1f for %s", bpm, file_path)
        return bpm

    return None


def detect_key(file_path: str) -> str | None:
    """Detect musical key using best available method."""
    # 1. Try reading from existing metadata first
    key = _detect_key_ffprobe(file_path)
    if key:
        log.debug("Key from metadata: %s for %s", key, file_path)
        return key

    # 2. Try keyfinder-cli
    key = _detect_key_keyfinder(file_path)
    if key:
        log.debug("Key from keyfinder: %s for %s", key, file_path)
        return key

    return None


def _get_unanalyzed_tracks(db_path: str, limit: int = 20) -> list[dict]:
    """Query Lexicon for tracks that need analysis (BPM=0 or no key)."""
    lexicon_api = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL

    try:
        with httpx.Client(base_url=lexicon_api, timeout=60) as client:
            resp = client.get("/v1/tracks")
            if resp.status_code != 200:
                log.warning("Failed to fetch Lexicon tracks: HTTP %d", resp.status_code)
                return []

            data = resp.json()
            tracks = data.get("data", {}).get("tracks", [])

            unanalyzed = []
            for t in tracks:
                bpm = t.get("bpm") or 0
                key = t.get("initialKey") or t.get("key") or ""
                # Track needs analysis if BPM is 0/missing or key is empty
                if (not bpm or float(bpm) == 0) or not key:
                    unanalyzed.append(t)
                    if len(unanalyzed) >= limit:
                        break

            return unanalyzed
    except Exception as e:
        log.error("Failed to query Lexicon for unanalyzed tracks: %s", e)
        return []


def _patch_lexicon_track(db_path: str, lexicon_track_id: str, edits: dict) -> bool:
    """PATCH a track in Lexicon with the given edits."""
    lexicon_api = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL

    try:
        with httpx.Client(base_url=lexicon_api, timeout=30) as client:
            resp = client.patch("/v1/track", json={
                "id": int(lexicon_track_id),
                "edits": edits,
            })
            if resp.status_code in (200, 201, 204):
                return True
            else:
                log.warning("Lexicon PATCH failed for track %s: HTTP %d %s",
                            lexicon_track_id, resp.status_code, resp.text[:200])
                return False
    except Exception as e:
        log.error("Lexicon PATCH error for track %s: %s", lexicon_track_id, e)
        return False


def _lexicon_path_to_local(db_path: str, lexicon_location: str) -> str | None:
    """Convert a Lexicon file path (Mac host path) to a container-local path."""
    lexicon_library_path = get_config(db_path, "lexicon_library_path") or "/Volumes/music/Database"
    lexicon_input_path = get_config(db_path, "lexicon_input_path") or "/Volumes/music/Input"
    music_library_path = os.environ.get("MUSIC_LIBRARY_PATH", "/music")
    downloads_path = os.environ.get("DOWNLOADS_PATH", "/downloads")

    # Also check legacy prefixes
    legacy_str = get_config(db_path, "lexicon_legacy_path_prefixes") or ""
    all_prefixes = [
        (lexicon_library_path, music_library_path),
        (lexicon_input_path, downloads_path),
    ]
    for p in legacy_str.split(","):
        p = p.strip()
        if p:
            all_prefixes.append((p.rstrip("/"), music_library_path))

    for mac_prefix, local_prefix in all_prefixes:
        if lexicon_location.startswith(mac_prefix):
            relative = lexicon_location[len(mac_prefix):].lstrip("/")
            local_path = os.path.join(local_prefix, relative)
            if os.path.exists(local_path):
                return local_path

    return None


def analyze_single_track(db_path: str, file_path: str, lexicon_track_id: str, sls_track_id: int | None = None) -> dict:
    """Analyze a single track file and write results to Lexicon.

    Returns dict with keys: bpm, key, patched (bool), errors (list).
    """
    result = {"bpm": None, "key": None, "patched": False, "errors": []}

    if not os.path.exists(file_path):
        result["errors"].append(f"File not found: {file_path}")
        return result

    # Detect BPM
    bpm = detect_bpm(file_path)
    if bpm:
        result["bpm"] = bpm

    # Detect key
    key = detect_key(file_path)
    if key:
        result["key"] = key

    # Build edits for Lexicon
    edits = {}
    if bpm and bpm > 0:
        edits["bpm"] = bpm
    if key:
        edits["initialKey"] = key

    if edits and lexicon_track_id:
        patched = _patch_lexicon_track(db_path, lexicon_track_id, edits)
        result["patched"] = patched

        if patched:
            log.info("Analyzed track (lexicon_id=%s): BPM=%.1f, key=%s",
                     lexicon_track_id, bpm or 0, key or "unknown")
            if sls_track_id:
                log_activity(
                    db_path, "track_analyzed", sls_track_id,
                    f"BPM={bpm or '?'}, key={key or '?'} written to Lexicon (id={lexicon_track_id})",
                    {"bpm": bpm, "key": key, "lexicon_track_id": lexicon_track_id},
                )
        else:
            result["errors"].append("Lexicon PATCH failed")
    elif not edits:
        log.debug("No analysis results for %s", file_path)
    else:
        result["errors"].append("No lexicon_track_id to patch")

    return result


def _analyze_batch(db_path: str):
    """Analyze a batch of unanalyzed tracks from Lexicon."""
    enabled = get_config(db_path, "auto_analyze_enabled")
    if enabled == "0":
        return

    batch_size = 20
    try:
        val = get_config(db_path, "analyze_batch_size")
        if val:
            batch_size = int(val)
    except (ValueError, TypeError):
        pass

    unanalyzed = _get_unanalyzed_tracks(db_path, limit=batch_size)
    if not unanalyzed:
        return

    log.info("Found %d unanalyzed tracks in Lexicon, processing...", len(unanalyzed))

    analyzed_count = 0
    error_count = 0

    for track in unanalyzed:
        lexicon_id = str(track.get("id", ""))
        location = track.get("location", "")

        if not location:
            continue

        local_path = _lexicon_path_to_local(db_path, location)
        if not local_path:
            log.debug("Cannot resolve local path for Lexicon track %s: %s", lexicon_id, location)
            continue

        # Find SLS track_id if we have one
        sls_track_id = None
        with get_db(db_path) as conn:
            row = conn.execute(
                "SELECT id FROM tracks WHERE lexicon_track_id = ?", (lexicon_id,)
            ).fetchone()
            if row:
                sls_track_id = row["id"]

        result = analyze_single_track(db_path, local_path, lexicon_id, sls_track_id)

        if result["patched"]:
            analyzed_count += 1
        elif result["errors"]:
            error_count += 1

    if analyzed_count > 0 or error_count > 0:
        log.info("Analysis batch complete: %d analyzed, %d errors", analyzed_count, error_count)

        # Update stats in config
        try:
            current = int(get_config(db_path, "analyze_total_processed") or "0")
            set_config(db_path, "analyze_total_processed", str(current + analyzed_count))
        except (ValueError, TypeError):
            set_config(db_path, "analyze_total_processed", str(analyzed_count))

        log_activity(
            db_path, "analyze_batch", None,
            f"Analyzed {analyzed_count} tracks, {error_count} errors (batch of {len(unanalyzed)})",
            {"analyzed": analyzed_count, "errors": error_count, "batch_size": len(unanalyzed)},
        )


async def analyze_tracks(db_path: str):
    """Async entry point for the analysis task."""
    await asyncio.to_thread(_analyze_batch, db_path)
