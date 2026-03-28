"""Process tracks through the pipeline stages: match -> download -> verify -> organize."""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import httpx

from tasks.helpers import (
    LEXICON_API_URL,
    MUSIC_LIBRARY_PATH,
    TIDARR_URL,
    get_config,
    get_db,
    get_tracks_by_stage,
    log_activity,
    sanitize_filename,
    set_config,
    update_track,
)

log = logging.getLogger("worker.pipeline")

# How many tracks to process per cycle in each stage
BATCH_SIZE = 10


async def process_pipeline(db_path: str):
    """Run one cycle of the pipeline processor."""
    await asyncio.to_thread(_process_new, db_path)
    await asyncio.to_thread(_process_matching, db_path)
    await asyncio.to_thread(_process_downloading, db_path)
    await asyncio.to_thread(_process_verifying, db_path)
    await asyncio.to_thread(_process_organizing, db_path)


# ---------------------------------------------------------------------------
# Stage: new -> matching
# ---------------------------------------------------------------------------

def _process_new(db_path: str):
    """Move 'new' tracks into the matching stage."""
    tracks = get_tracks_by_stage(db_path, "new", limit=BATCH_SIZE)
    for track in tracks:
        try:
            update_track(db_path, track["id"], pipeline_stage="matching")
            log.info("Track %d (%s - %s) -> matching", track["id"], track["artist"], track["title"])
        except Exception as e:
            log.error("Failed to advance track %d to matching: %s", track["id"], e)


# ---------------------------------------------------------------------------
# Stage: matching -> downloading (or error)
# ---------------------------------------------------------------------------

def _process_matching(db_path: str):
    """Attempt to match 'matching' tracks via Tidarr/Tidal."""
    tracks = get_tracks_by_stage(db_path, "matching", limit=BATCH_SIZE)
    for track in tracks:
        try:
            _match_track(db_path, track)
        except Exception as e:
            log.error("Match error for track %d: %s", track["id"], e, exc_info=True)
            update_track(
                db_path, track["id"],
                pipeline_stage="error",
                pipeline_error=f"Match error: {e}",
                match_status="failed",
            )
            log_activity(db_path, "match_error", track["id"], f"Match failed: {e}")


def _match_track(db_path: str, track: dict):
    """Try to find a Tidal match for a track via Tidarr."""
    track_id = track["id"]
    isrc = track.get("isrc")
    artist = track.get("artist", "")
    title = track.get("title", "")
    duration_ms = track.get("duration_ms") or 0

    matched = False
    tidal_id = None
    confidence = 0.0
    match_source = None

    # Strategy 1: Search by ISRC
    if isrc:
        try:
            result = _tidarr_search(isrc)
            if result:
                for item in result:
                    item_isrc = item.get("isrc", "")
                    if item_isrc and item_isrc.upper() == isrc.upper():
                        tidal_id = str(item.get("id", ""))
                        confidence = 1.0
                        match_source = "isrc"
                        matched = True
                        break
        except Exception as e:
            log.warning("ISRC search failed for track %d: %s", track_id, e)

    # Strategy 2: Search by artist + title
    if not matched:
        query = f"{artist} {title}".strip()
        if query:
            try:
                result = _tidarr_search(query)
                if result:
                    for item in result:
                        item_title = (item.get("title") or "").lower()
                        item_artist = (item.get("artist", {}).get("name") or
                                       item.get("artists", [{}])[0].get("name", "") if item.get("artists") else "").lower()
                        item_duration = item.get("duration", 0) * 1000  # Tidal returns seconds

                        title_match = title.lower() in item_title or item_title in title.lower()
                        artist_match = artist.lower().split(",")[0].strip() in item_artist
                        duration_close = abs(item_duration - duration_ms) < 5000  # within 5 seconds

                        if title_match and artist_match and duration_close:
                            tidal_id = str(item.get("id", ""))
                            confidence = 0.95
                            match_source = "search"
                            matched = True
                            break
            except Exception as e:
                log.warning("Title/artist search failed for track %d: %s", track_id, e)

    if matched and tidal_id:
        update_track(
            db_path, track_id,
            match_status="matched",
            match_source=match_source,
            match_confidence=confidence,
            tidal_id=tidal_id,
            pipeline_stage="downloading",
            download_status="pending",
        )
        log_activity(
            db_path, "match_found", track_id,
            f"Matched via {match_source} (confidence {confidence}): tidal_id={tidal_id}",
            {"tidal_id": tidal_id, "match_source": match_source, "confidence": confidence},
        )
        log.info("Track %d matched via %s -> tidal_id=%s", track_id, match_source, tidal_id)
    else:
        # Record fallback attempt
        with get_db(db_path) as conn:
            conn.execute(
                """INSERT INTO fallback_attempts
                (track_id, source, status, search_query, result_count)
                VALUES (?, 'tidarr', 'no_match', ?, 0)""",
                (track_id, f"{artist} {title}"),
            )

        update_track(
            db_path, track_id,
            match_status="failed",
            pipeline_stage="error",
            pipeline_error="No Tidal match found",
        )
        log_activity(db_path, "match_failed", track_id, f"No match found for: {artist} - {title}")
        log.warning("Track %d: no match for '%s - %s'", track_id, artist, title)


def _tidarr_search(query: str) -> list[dict]:
    """Search Tidarr for tracks. Returns list of result items."""
    with httpx.Client(timeout=30) as client:
        resp = client.get(f"{TIDARR_URL}/api/search/track", params={"query": query})
        resp.raise_for_status()
        data = resp.json()
        # Tidarr returns items in various structures; normalize
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("items", data.get("tracks", data.get("results", [])))
        return []


# ---------------------------------------------------------------------------
# Stage: downloading -> verifying (or error)
# ---------------------------------------------------------------------------

def _process_downloading(db_path: str):
    """Download matched tracks via Tidarr."""
    tracks = get_tracks_by_stage(db_path, "downloading", limit=BATCH_SIZE)
    # Only process tracks that haven't started downloading yet
    tracks = [t for t in tracks if t.get("download_status") in ("pending", "failed")]

    max_concurrent = int(get_config(db_path, "max_concurrent_downloads") or 2)
    # Only take up to max_concurrent at a time
    tracks = tracks[:max_concurrent]

    for track in tracks:
        try:
            _download_track(db_path, track)
        except Exception as e:
            attempts = (track.get("download_attempts") or 0) + 1
            log.error("Download error for track %d (attempt %d): %s", track["id"], attempts, e)
            update_track(
                db_path, track["id"],
                download_status="failed" if attempts >= 3 else "pending",
                download_attempts=attempts,
                download_error=str(e),
                pipeline_stage="error" if attempts >= 3 else "downloading",
                pipeline_error=f"Download failed after {attempts} attempts: {e}" if attempts >= 3 else None,
            )
            log_activity(db_path, "download_error", track["id"], f"Download attempt {attempts} failed: {e}")


def _download_track(db_path: str, track: dict):
    """Queue and retrieve a track download from Tidarr."""
    track_id = track["id"]
    tidal_id = track.get("tidal_id")
    artist = track.get("artist", "Unknown Artist")
    album = track.get("album", "Unknown Album")
    title = track.get("title", "Unknown Track")
    attempts = (track.get("download_attempts") or 0) + 1

    if not tidal_id:
        raise ValueError("No tidal_id set for track")

    update_track(db_path, track_id, download_status="downloading", download_attempts=attempts)

    # Insert into download_queue for tracking
    with get_db(db_path) as conn:
        existing_queue = conn.execute(
            "SELECT id FROM download_queue WHERE track_id = ? AND status NOT IN ('complete', 'failed')",
            (track_id,),
        ).fetchone()
        if not existing_queue:
            conn.execute(
                """INSERT INTO download_queue (track_id, source, status, attempts)
                VALUES (?, 'tidarr', 'downloading', ?)""",
                (track_id, attempts),
            )

    # Request download from Tidarr
    with httpx.Client(timeout=120) as client:
        resp = client.post(
            f"{TIDARR_URL}/api/download/track",
            json={"id": int(tidal_id)},
        )
        resp.raise_for_status()
        download_data = resp.json()

    # Tidarr downloads to its output dir. We need to find and move the file.
    # Tidarr typically outputs to /downloads/{Artist}/{Album}/{track}.flac
    # Check if Tidarr returned a file path
    tidarr_path = download_data.get("path") or download_data.get("file") or download_data.get("filePath")

    if not tidarr_path:
        # Poll Tidarr for completion if it's async
        _wait_for_tidarr_download(tidal_id)
        # Try to find the file in common Tidarr output locations
        tidarr_path = _find_downloaded_file(artist, album, title)

    if not tidarr_path or not os.path.exists(tidarr_path):
        raise FileNotFoundError(f"Downloaded file not found for tidal_id={tidal_id}")

    # Move file to organized library path: /music/{Artist}/{Album}/{track}.flac
    safe_artist = sanitize_filename(artist.split(",")[0].strip())
    safe_album = sanitize_filename(album)
    safe_title = sanitize_filename(title)

    dest_dir = os.path.join(MUSIC_LIBRARY_PATH, safe_artist, safe_album)
    os.makedirs(dest_dir, exist_ok=True)

    ext = os.path.splitext(tidarr_path)[1] or ".flac"
    dest_path = os.path.join(dest_dir, f"{safe_title}{ext}")

    # Avoid overwriting existing files
    if os.path.exists(dest_path) and dest_path != tidarr_path:
        base, extension = os.path.splitext(dest_path)
        dest_path = f"{base}_{track_id}{extension}"

    if tidarr_path != dest_path:
        shutil.move(tidarr_path, dest_path)

    # Calculate file hash
    file_hash = _sha256(dest_path)

    # Update track and download queue
    update_track(
        db_path, track_id,
        download_status="complete",
        download_source="tidarr",
        file_path=dest_path,
        file_hash_sha256=file_hash,
        pipeline_stage="verifying",
        verify_status="pending",
    )

    with get_db(db_path) as conn:
        conn.execute(
            """UPDATE download_queue SET status='complete', completed_at=datetime('now')
            WHERE track_id = ? AND status = 'downloading'""",
            (track_id,),
        )

    log_activity(
        db_path, "download_complete", track_id,
        f"Downloaded: {artist} - {title}",
        {"file_path": dest_path, "file_hash": file_hash, "source": "tidarr"},
    )
    log.info("Track %d downloaded -> %s", track_id, dest_path)


def _wait_for_tidarr_download(tidal_id: str, max_wait: int = 300, poll_interval: int = 5):
    """Poll Tidarr queue until download completes or timeout."""
    import time
    start = time.time()
    while time.time() - start < max_wait:
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(f"{TIDARR_URL}/api/queue")
                if resp.status_code == 200:
                    queue = resp.json()
                    # Check if our track is still in the queue
                    active = [
                        item for item in (queue if isinstance(queue, list) else queue.get("items", []))
                        if str(item.get("id", "")) == str(tidal_id)
                    ]
                    if not active:
                        return  # download finished (no longer in queue)
        except Exception:
            pass
        time.sleep(poll_interval)


def _find_downloaded_file(artist: str, album: str, title: str) -> str | None:
    """Try to locate a downloaded file in common Tidarr output directories."""
    search_dirs = ["/downloads", "/app/downloads", "/music/downloads"]
    for base_dir in search_dirs:
        if not os.path.exists(base_dir):
            continue
        for root, dirs, files in os.walk(base_dir):
            for f in files:
                if f.endswith((".flac", ".mp3", ".m4a", ".ogg")):
                    name_lower = f.lower()
                    if title.lower()[:20] in name_lower or (artist.split(",")[0].strip().lower() in root.lower()):
                        return os.path.join(root, f)
    return None


def _sha256(filepath: str) -> str:
    """Calculate SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Stage: verifying -> organizing (or error)
# ---------------------------------------------------------------------------

def _process_verifying(db_path: str):
    """Verify downloaded files: codec, sample rate, bit depth, chromaprint."""
    tracks = get_tracks_by_stage(db_path, "verifying", limit=BATCH_SIZE)
    min_fp_score = float(get_config(db_path, "fingerprint_min_score") or 0.85)

    for track in tracks:
        try:
            _verify_track(db_path, track, min_fp_score)
        except Exception as e:
            log.error("Verify error for track %d: %s", track["id"], e, exc_info=True)
            update_track(
                db_path, track["id"],
                verify_status="fail",
                pipeline_stage="error",
                pipeline_error=f"Verification error: {e}",
            )
            log_activity(db_path, "verify_error", track["id"], f"Verification failed: {e}")


def _verify_track(db_path: str, track: dict, min_fp_score: float):
    """Run ffprobe and fpcalc on a downloaded file."""
    track_id = track["id"]
    file_path = track.get("file_path")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # --- ffprobe: check codec, sample rate, bit depth ---
    probe_result = subprocess.run(
        [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", file_path,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    if probe_result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {probe_result.stderr}")

    probe_data = json.loads(probe_result.stdout)
    audio_stream = None
    for stream in probe_data.get("streams", []):
        if stream.get("codec_type") == "audio":
            audio_stream = stream
            break

    if not audio_stream:
        raise RuntimeError("No audio stream found in file")

    codec = audio_stream.get("codec_name", "unknown")
    sample_rate = int(audio_stream.get("sample_rate", 0))
    bit_depth = int(audio_stream.get("bits_per_raw_sample") or audio_stream.get("bits_per_sample") or 0)

    # --- fpcalc: chromaprint fingerprint ---
    chromaprint = None
    fp_duration = None
    try:
        fpcalc_result = subprocess.run(
            ["fpcalc", "-json", file_path],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if fpcalc_result.returncode == 0:
            fp_data = json.loads(fpcalc_result.stdout)
            chromaprint = fp_data.get("fingerprint")
            fp_duration = fp_data.get("duration")
    except Exception as e:
        log.warning("fpcalc failed for track %d: %s", track_id, e)

    # --- Determine if genuine lossless ---
    is_lossless = codec in ("flac", "alac", "wav", "aiff") and sample_rate >= 44100

    # --- Simple fingerprint duration check ---
    # Compare fpcalc duration with Spotify duration to detect truncated files
    fp_match_score = None
    spotify_duration_ms = track.get("duration_ms") or 0
    if fp_duration and spotify_duration_ms:
        spotify_duration_s = spotify_duration_ms / 1000.0
        duration_diff = abs(fp_duration - spotify_duration_s)
        # Score: 1.0 if durations match within 2s, degrading after
        if duration_diff <= 2:
            fp_match_score = 1.0
        elif duration_diff <= 10:
            fp_match_score = max(0, 1.0 - (duration_diff - 2) / 20)
        else:
            fp_match_score = max(0, 0.6 - duration_diff / 100)

    # --- Pass/fail decision ---
    verify_pass = True
    reasons = []

    if codec not in ("flac", "alac", "wav", "aiff", "mp3", "aac", "opus", "vorbis"):
        verify_pass = False
        reasons.append(f"unexpected codec: {codec}")

    if sample_rate < 44100:
        verify_pass = False
        reasons.append(f"low sample rate: {sample_rate}")

    if fp_match_score is not None and fp_match_score < min_fp_score:
        verify_pass = False
        reasons.append(f"fingerprint score too low: {fp_match_score:.2f}")

    verify_status = "pass" if verify_pass else "fail"
    next_stage = "organizing" if verify_pass else "error"

    update_track(
        db_path, track_id,
        verify_status=verify_status,
        verify_codec=codec,
        verify_sample_rate=sample_rate,
        verify_bit_depth=bit_depth,
        verify_is_genuine_lossless=1 if is_lossless else 0,
        chromaprint=chromaprint,
        fingerprint_match_score=fp_match_score,
        pipeline_stage=next_stage,
        pipeline_error="; ".join(reasons) if reasons else None,
    )

    log_activity(
        db_path, f"verify_{verify_status}", track_id,
        f"Verification {verify_status}: codec={codec}, sr={sample_rate}, bits={bit_depth}",
        {
            "codec": codec,
            "sample_rate": sample_rate,
            "bit_depth": bit_depth,
            "is_lossless": is_lossless,
            "fp_match_score": fp_match_score,
            "reasons": reasons,
        },
    )
    log.info("Track %d verified: %s (codec=%s, sr=%s, bits=%s)", track_id, verify_status, codec, sample_rate, bit_depth)


# ---------------------------------------------------------------------------
# Stage: organizing -> complete (Lexicon sync)
# ---------------------------------------------------------------------------

def _process_organizing(db_path: str):
    """Sync verified tracks to Lexicon: create playlists, import files, tag."""
    tracks = get_tracks_by_stage(db_path, "organizing", limit=BATCH_SIZE)

    for track in tracks:
        try:
            _organize_track(db_path, track)
        except Exception as e:
            log.error("Organize error for track %d: %s", track["id"], e, exc_info=True)
            update_track(
                db_path, track["id"],
                lexicon_status="error",
                pipeline_stage="error",
                pipeline_error=f"Lexicon sync error: {e}",
            )
            log_activity(db_path, "organize_error", track["id"], f"Lexicon sync failed: {e}")


def _organize_track(db_path: str, track: dict):
    """Import a track into Lexicon and add it to the appropriate monthly playlist."""
    track_id = track["id"]
    spotify_id = track["spotify_id"]
    file_path = track.get("file_path")
    added_at = track.get("spotify_added_at", "")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Determine target year/month from spotify_added_at
    if added_at:
        try:
            dt = datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            year = dt.year
            month = dt.month
        except (ValueError, TypeError):
            year = datetime.utcnow().year
            month = datetime.utcnow().month
    else:
        year = datetime.utcnow().year
        month = datetime.utcnow().month

    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    folder_name = str(year)
    playlist_name = f"{month_names[month]} {year}"

    # Ensure playlist record exists in our DB
    playlist_row = _ensure_playlist(db_path, year, month, folder_name, playlist_name)

    # --- Lexicon operations ---
    with httpx.Client(base_url=LEXICON_API_URL, timeout=60) as client:
        # 1. Import the track file into Lexicon
        lexicon_track_id = _lexicon_import_track(client, file_path, spotify_id)

        # 2. Ensure the folder and playlist exist in Lexicon
        lexicon_folder_id = playlist_row.get("lexicon_folder_id")
        lexicon_playlist_id = playlist_row.get("lexicon_playlist_id")

        if not lexicon_folder_id:
            lexicon_folder_id = _lexicon_ensure_folder(client, folder_name)
            if lexicon_folder_id:
                with get_db(db_path) as conn:
                    conn.execute(
                        "UPDATE playlists SET lexicon_folder_id = ? WHERE id = ?",
                        (lexicon_folder_id, playlist_row["id"]),
                    )

        if not lexicon_playlist_id:
            lexicon_playlist_id = _lexicon_ensure_playlist(client, playlist_name, lexicon_folder_id)
            if lexicon_playlist_id:
                with get_db(db_path) as conn:
                    conn.execute(
                        "UPDATE playlists SET lexicon_playlist_id = ? WHERE id = ?",
                        (lexicon_playlist_id, playlist_row["id"]),
                    )

        # 3. Add track to playlist
        if lexicon_playlist_id and lexicon_track_id:
            _lexicon_add_to_playlist(client, lexicon_playlist_id, lexicon_track_id)

        # 4. Tag track comment with [sls:{spotify_id}]
        if lexicon_track_id:
            _lexicon_tag_track(client, lexicon_track_id, spotify_id)

    # Update track record
    update_track(
        db_path, track_id,
        lexicon_status="synced",
        lexicon_track_id=lexicon_track_id,
        lexicon_playlist_id=lexicon_playlist_id,
        pipeline_stage="complete",
    )

    # Link track to playlist in our DB
    if playlist_row:
        with get_db(db_path) as conn:
            conn.execute(
                """INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id)
                VALUES (?, ?)""",
                (playlist_row["id"], track_id),
            )
            conn.execute(
                "UPDATE playlists SET track_count = track_count + 1 WHERE id = ?",
                (playlist_row["id"],),
            )

    log_activity(
        db_path, "lexicon_synced", track_id,
        f"Synced to Lexicon: {track.get('artist')} - {track.get('title')} -> {playlist_name}",
        {"lexicon_track_id": lexicon_track_id, "playlist": playlist_name},
    )
    log.info("Track %d synced to Lexicon -> %s", track_id, playlist_name)


def _ensure_playlist(db_path: str, year: int, month: int, folder_name: str, playlist_name: str) -> dict:
    """Ensure a playlist record exists in the DB for this year/month. Returns the row as dict."""
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM playlists WHERE year = ? AND month = ?",
            (year, month),
        ).fetchone()

        if row:
            return dict(row)

        conn.execute(
            """INSERT INTO playlists (folder_name, playlist_name, year, month)
            VALUES (?, ?, ?, ?)""",
            (folder_name, playlist_name, year, month),
        )
        new_row = conn.execute(
            "SELECT * FROM playlists WHERE year = ? AND month = ?",
            (year, month),
        ).fetchone()
        return dict(new_row)


# ---------------------------------------------------------------------------
# Lexicon API helpers
# ---------------------------------------------------------------------------

def _lexicon_import_track(client: httpx.Client, file_path: str, spotify_id: str) -> str | None:
    """Import a track file into Lexicon. Returns lexicon track ID."""
    try:
        # Lexicon's import endpoint — try common patterns
        # Option 1: POST /api/tracks/import with file path
        resp = client.post("/api/tracks/import", json={"filePath": file_path})
        if resp.status_code in (200, 201):
            data = resp.json()
            return str(data.get("id") or data.get("trackId") or "")
    except httpx.HTTPError:
        pass

    try:
        # Option 2: POST /api/library/import
        resp = client.post("/api/library/import", json={"paths": [file_path]})
        if resp.status_code in (200, 201):
            data = resp.json()
            tracks = data.get("tracks", [data]) if isinstance(data, dict) else data
            if tracks:
                return str(tracks[0].get("id") or tracks[0].get("trackId") or "")
    except httpx.HTTPError:
        pass

    try:
        # Option 3: PUT /api/tracks with file info
        resp = client.put("/api/tracks", json={"filePath": file_path})
        if resp.status_code in (200, 201):
            data = resp.json()
            return str(data.get("id") or data.get("trackId") or "")
    except httpx.HTTPError:
        pass

    log.warning("Could not import track to Lexicon via any known endpoint for: %s", file_path)
    return None


def _lexicon_ensure_folder(client: httpx.Client, folder_name: str) -> str | None:
    """Ensure a playlist folder exists in Lexicon. Returns folder ID."""
    try:
        # List existing folders
        resp = client.get("/api/playlists/folders")
        if resp.status_code == 200:
            folders = resp.json()
            if isinstance(folders, list):
                for f in folders:
                    if f.get("name") == folder_name:
                        return str(f.get("id", ""))

        # Create folder
        resp = client.post("/api/playlists/folders", json={"name": folder_name})
        if resp.status_code in (200, 201):
            data = resp.json()
            return str(data.get("id", ""))
    except httpx.HTTPError as e:
        log.warning("Lexicon folder operation failed: %s", e)
    return None


def _lexicon_ensure_playlist(client: httpx.Client, playlist_name: str, folder_id: str | None) -> str | None:
    """Ensure a playlist exists in Lexicon. Returns playlist ID."""
    try:
        # List existing playlists
        resp = client.get("/api/playlists")
        if resp.status_code == 200:
            playlists = resp.json()
            if isinstance(playlists, list):
                for p in playlists:
                    if p.get("name") == playlist_name:
                        return str(p.get("id", ""))

        # Create playlist
        payload = {"name": playlist_name}
        if folder_id:
            payload["folderId"] = folder_id
        resp = client.post("/api/playlists", json=payload)
        if resp.status_code in (200, 201):
            data = resp.json()
            return str(data.get("id", ""))
    except httpx.HTTPError as e:
        log.warning("Lexicon playlist operation failed: %s", e)
    return None


def _lexicon_add_to_playlist(client: httpx.Client, playlist_id: str, track_id: str):
    """Add a track to a Lexicon playlist."""
    try:
        resp = client.post(
            f"/api/playlists/{playlist_id}/tracks",
            json={"trackIds": [track_id]},
        )
        if resp.status_code not in (200, 201, 204):
            log.warning("Failed to add track %s to playlist %s: %s", track_id, playlist_id, resp.status_code)
    except httpx.HTTPError as e:
        log.warning("Lexicon add-to-playlist failed: %s", e)


def _lexicon_tag_track(client: httpx.Client, lexicon_track_id: str, spotify_id: str):
    """Tag a track's comment field with [sls:{spotify_id}] for traceability."""
    tag = f"[sls:{spotify_id}]"
    try:
        # Get current track data
        resp = client.get(f"/api/tracks/{lexicon_track_id}")
        if resp.status_code == 200:
            track_data = resp.json()
            current_comment = track_data.get("comment") or ""
            if tag not in current_comment:
                new_comment = f"{current_comment} {tag}".strip()
                client.patch(
                    f"/api/tracks/{lexicon_track_id}",
                    json={"comment": new_comment},
                )
    except httpx.HTTPError as e:
        log.warning("Lexicon tag operation failed: %s", e)
