"""Process tracks through the pipeline stages: match -> download -> verify -> organize."""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import re

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

BATCH_SIZE = 10  # legacy default

# Per-stage batch sizes
BATCH_NEW = 50       # Lexicon/file checks are fast
BATCH_MATCH = 20     # Tidal API calls
BATCH_DOWNLOAD = 2   # Tidarr one at a time
BATCH_VERIFY = 20    # ffprobe is fast
BATCH_ORGANIZE = 20  # Lexicon API


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

def _check_existing_by_isrc(db_path: str, track: dict) -> dict | None:
    """Check if a track already exists in the file_index by ISRC or fuzzy title+artist."""
    isrc = (track.get("isrc") or "").strip()
    title = (track.get("title") or "").strip()
    artist = (track.get("artist") or "").strip()

    with get_db(db_path) as conn:
        # Check if the file_index table exists
        tbl = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='file_index'"
        ).fetchone()
        if not tbl:
            return None

        # Primary: exact ISRC match (guaranteed same recording)
        if isrc:
            row = conn.execute(
                "SELECT file_path, title, artist FROM file_index WHERE isrc = ?", (isrc,)
            ).fetchone()
            if row:
                return {"file_path": row[0], "match_type": "isrc"}

        # Secondary: fuzzy title + artist prefix match
        if title and artist:
            title_prefix = title[:15]
            artist_first = artist.split(",")[0].strip()
            row = conn.execute(
                "SELECT file_path FROM file_index WHERE title LIKE ? AND artist LIKE ?",
                (f"{title_prefix}%", f"{artist_first}%"),
            ).fetchone()
            if row:
                return {"file_path": row[0], "match_type": "title_artist"}

    return None


def _process_new(db_path: str):
    # One-time reset: move downloading tracks back to new for Lexicon re-check
    if get_config(db_path, "_lexicon_recheck_done") != "1":
        with get_db(db_path) as conn:
            r = conn.execute(
                """UPDATE tracks SET pipeline_stage = 'new', updated_at = datetime('now')
                   WHERE pipeline_stage = 'downloading' AND download_status = 'pending'"""
            )
            log.info("One-time Lexicon recheck: reset %d downloading tracks to new", r.rowcount)
        set_config(db_path, "_lexicon_recheck_done", "1")

    tracks = get_tracks_by_stage(db_path, "new", limit=BATCH_NEW)
    for track in tracks:
        # Safety: skip tracks that are already complete (prevents reprocessing)
        if track.get("pipeline_stage") == "complete":
            log.debug("Track %d already complete, skipping", track["id"])
            continue
        try:
            # Check ISRC index first (fastest, most reliable)
            isrc_match = _check_existing_by_isrc(db_path, track)
            if isrc_match:
                log.info(
                    "Track %d (%s - %s) found via %s in file index: %s",
                    track["id"], track["artist"], track["title"],
                    isrc_match["match_type"], isrc_match["file_path"],
                )
                update_track(
                    db_path, track["id"],
                    pipeline_stage="verifying",
                    match_status="matched",
                    match_source=f"file_index_{isrc_match['match_type']}",
                    match_confidence=1.0 if isrc_match["match_type"] == "isrc" else 0.85,
                    download_status="complete",
                    download_source="existing",
                    file_path=isrc_match["file_path"],
                )
                log_activity(
                    db_path, "isrc_index_match", track["id"],
                    f"Found via {isrc_match['match_type']} in file index: {isrc_match['file_path']}",
                    {"file_path": isrc_match["file_path"], "match_type": isrc_match["match_type"]},
                )
                continue

            # Check if track already exists in Lexicon's database
            lexicon_existing = _check_existing_in_lexicon(track)
            if lexicon_existing:
                log.info(
                    "Track %d (%s - %s) already in Lexicon (track_id=%s): %s",
                    track["id"], track["artist"], track["title"],
                    lexicon_existing["lexicon_track_id"], lexicon_existing["file_path"],
                )
                update_track(
                    db_path, track["id"],
                    pipeline_stage="organizing",
                    match_status="matched",
                    match_source="lexicon_existing",
                    download_status="skipped",
                    download_source="lexicon_existing",
                    file_path=lexicon_existing["file_path"],
                    lexicon_track_id=lexicon_existing["lexicon_track_id"],
                )
                log_activity(
                    db_path, "lexicon_existing_found", track["id"],
                    f"Found in Lexicon (track_id={lexicon_existing['lexicon_track_id']}): {lexicon_existing['file_path']}",
                    {"lexicon_track_id": lexicon_existing["lexicon_track_id"], "file_path": lexicon_existing["file_path"]},
                )
                continue

            # Check if track already exists in the music library on disk
            existing = _check_existing_in_library(track, db_path)
            if existing:
                log.info("Track %d (%s - %s) already exists: %s", track["id"], track["artist"], track["title"], existing["file_path"])
                update_track(
                    db_path, track["id"],
                    pipeline_stage="verifying",
                    match_status="matched",
                    match_source="library_existing",
                    match_confidence=0.9,
                    download_status="complete",
                    download_source="existing",
                    file_path=existing["file_path"],
                )
                log_activity(
                    db_path, "existing_found", track["id"],
                    f"Found existing file: {existing['file_path']}",
                    {"file_path": existing["file_path"]},
                )
                continue

            update_track(db_path, track["id"], pipeline_stage="matching")
            log.info("Track %d (%s - %s) -> matching", track["id"], track["artist"], track["title"])
        except Exception as e:
            log.error("Failed to advance track %d: %s", track["id"], e)


def _check_existing_in_lexicon(track: dict) -> dict | None:
    """Check if a track already exists in Lexicon's database."""
    artist = track.get("artist", "")
    title = track.get("title", "")
    if not artist or not title:
        return None

    lexicon_url = os.environ.get("LEXICON_API_URL", LEXICON_API_URL)
    spotify_title = title.lower().strip()
    spotify_artists = [a.strip().lower() for a in artist.split(",")]

    # Clean title for matching
    title_base = spotify_title
    for suffix in [" - original mix", " (original mix)", " - extended mix", " (extended mix)",
                   " - radio edit", " (radio edit)", " - vip", " (vip)"]:
        title_base = title_base.replace(suffix, "")
    title_base = title_base.split(" - ")[0].strip()  # Before remix indicator

    try:
        with httpx.Client(base_url=lexicon_url, timeout=15) as client:
            # Search by TITLE ONLY — more reliable than combined filter
            resp = client.get("/v1/search/tracks", params={"filter[title]": title})
            if resp.status_code != 200:
                return None

            data = resp.json()
            results = data.get("data", {}).get("tracks", [])

            for t in results:
                lex_title = (t.get("title") or "").lower().strip()
                lex_artist = (t.get("artist") or "").lower().strip()

                # Check if ANY spotify artist appears in the lexicon artist
                artist_match = any(a in lex_artist for a in spotify_artists)
                if not artist_match:
                    # Also check reverse: lexicon artist words in spotify artists
                    lex_artists = [a.strip() for a in lex_artist.split(",")]
                    artist_match = any(la in " ".join(spotify_artists) for la in lex_artists if len(la) > 2)

                if not artist_match:
                    continue

                # Title matching: fuzzy bidirectional
                title_match = (
                    spotify_title in lex_title or
                    lex_title in spotify_title or
                    title_base in lex_title or
                    lex_title.split(" (")[0].strip() in spotify_title
                )

                if title_match:
                    return {
                        "lexicon_track_id": str(t["id"]),
                        "file_path": t.get("location", ""),
                    }
    except Exception as e:
        log.warning("Lexicon check failed: %s", e)

    return None


def _check_existing_in_library(track: dict, db_path: str | None = None) -> dict | None:
    """Check if a track already exists in the music library or downloads directory."""
    artist = track.get("artist", "")
    title = track.get("title", "")
    if not artist or not title:
        return None

    # Check file_index for title+artist fuzzy match (fast, indexed)
    # NOTE: ISRC lookup is handled by _check_existing_by_isrc, so only do title+artist here
    if db_path:
        try:
            with get_db(db_path) as conn:
                table_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='file_index'"
                ).fetchone()
                if table_exists:
                    row = conn.execute(
                        "SELECT file_path FROM file_index WHERE title LIKE ? AND artist LIKE ? LIMIT 1",
                        (f"%{title[:20]}%", f"%{artist.split(',')[0].strip()[:15]}%"),
                    ).fetchone()
                    if row and os.path.exists(row[0]):
                        return {"file_path": row[0]}
        except Exception:
            pass  # file_index may not exist yet

    downloads_dir = os.environ.get("DOWNLOADS_PATH", "/downloads")

    # Build list of artist name variations to check
    artist_names = set()
    artist_names.add(artist)                              # Full: "G Jones, Eprom"
    artist_names.add(artist.split(",")[0].strip())         # First: "G Jones"
    artist_names.add(artist.replace(", ", " "))            # No comma: "G Jones Eprom"
    # Also add each individual artist
    for a in artist.split(","):
        artist_names.add(a.strip())                        # Each: "G Jones", "Eprom"

    # Title variations for matching
    title_lower = title.lower()
    # Strip common suffixes for broader matching
    title_clean = title_lower.replace(" - original mix", "").replace(" (original mix)", "")
    title_words = title_clean.split(" - ")[0].strip()  # Before any " - " suffix

    # Search both music library and downloads directory
    search_dirs = [MUSIC_LIBRARY_PATH, downloads_dir]

    for base_dir in search_dirs:
        if not os.path.isdir(base_dir):
            continue
        for artist_name in artist_names:
            # Check both root level and tracks/ subdirectory
            for prefix in ["", "tracks"]:
                artist_dir = os.path.join(base_dir, prefix, artist_name) if prefix else os.path.join(base_dir, artist_name)
                if not os.path.isdir(artist_dir):
                    continue

                for root, dirs, files in os.walk(artist_dir):
                    dirs[:] = [d for d in dirs if not d.startswith("@") and not d.endswith(".old")]
                    for f in files:
                        if not f.endswith((".flac", ".aiff", ".m4a", ".wav")):
                            continue
                        fname_lower = f.lower()
                        # Match if title (or cleaned title) appears in filename
                        if (title_words[:15] in fname_lower or
                                title_lower[:20] in fname_lower or
                                title_clean[:20] in fname_lower):
                            return {"file_path": os.path.join(root, f)}

    return None


# ---------------------------------------------------------------------------
# Stage: matching -> downloading (or error)
# ---------------------------------------------------------------------------

def _process_matching(db_path: str):
    sync_mode = get_config(db_path, "sync_mode") or "scan"
    tracks = get_tracks_by_stage(db_path, "matching", limit=BATCH_MATCH)
    for track in tracks:
        try:
            if sync_mode == "scan":
                # Scan mode: skip Tidal search, mark as waiting for download mode
                update_track(
                    db_path, track["id"],
                    pipeline_stage="waiting",
                    match_status="pending",
                    pipeline_error=None,
                )
                log.info(
                    "Scan mode: track %d (%s - %s) not in library, waiting for download mode",
                    track["id"], track.get("artist", ""), track.get("title", ""),
                )
                continue
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


def _normalize_title(title: str) -> str:
    """Normalize a track title for fuzzy comparison."""
    t = title.lower().strip()
    # Remove common suffixes
    for suffix in [" - original mix", " (original mix)", " - extended mix", " (extended mix)",
                   " - radio edit", " (radio edit)", " - vip", " (vip)",
                   " - original version", " (original version)"]:
        t = t.replace(suffix, "")
    # Remove featured artist tags: [feat. ...], (feat. ...), [ft. ...], (ft. ...)
    t = re.sub(r"\s*[\(\[]\s*(?:feat|ft)\.?\s+[^\)\]]+[\)\]]", "", t)
    # Normalize parentheses vs dashes for remixes
    # "Title - Remix Name" -> "title remix name"
    # "Title (Remix Name)" -> "title remix name"
    t = t.replace(" - ", " ").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    # Remove extra whitespace
    return " ".join(t.split())


def _match_track(db_path: str, track: dict):
    """Try to find a Tidal match. Uses Tidarr's Tidal session for search via tiddl."""
    track_id = track["id"]
    isrc = track.get("isrc")
    artist = track.get("artist", "")
    title = track.get("title", "")
    duration_ms = track.get("duration_ms") or 0

    matched = False
    tidal_id = None
    confidence = 0.0
    match_source = None

    # Strategy 1: Search Tidal by ISRC
    if isrc:
        try:
            results = _tidal_search_via_tidarr(isrc)
            # Check ALL results for matching ISRC (not just the first)
            for item in results:
                if item.get("isrc", "").upper() == isrc.upper():
                    tidal_id = str(item["id"])
                    confidence = 1.0
                    match_source = "isrc"
                    matched = True
                    log.debug("Track %d: ISRC match found at result index %d", track_id,
                              results.index(item))
                    break
        except Exception as e:
            log.warning("ISRC search failed for track %d: %s", track_id, e)

    # Strategy 2: Search by artist + title with smarter matching
    if not matched:
        query = f"{artist} {title}".strip()
        if query:
            try:
                results = _tidal_search_via_tidarr(query)
                spotify_norm = _normalize_title(title)
                spotify_artists = [a.strip().lower() for a in artist.split(",")]

                best_candidate = None
                best_confidence = 0.0

                for item in results:
                    item_title = (item.get("title") or "")
                    item_norm = _normalize_title(item_title)
                    item_title_lower = item_title.lower()

                    # Build full artist string from Tidal response
                    item_artist = ""
                    if item.get("artist", {}).get("name"):
                        item_artist = item["artist"]["name"].lower()
                    elif item.get("artists"):
                        item_artist = " ".join(
                            a.get("name", "") for a in item["artists"]
                        ).lower()

                    # Check if ANY Spotify artist appears in the Tidal artist name
                    artist_match = any(sa in item_artist for sa in spotify_artists)
                    if not artist_match:
                        continue

                    item_duration = (item.get("duration") or 0) * 1000
                    duration_diff = abs(item_duration - duration_ms)

                    # Title matching tiers
                    norm_exact = (spotify_norm == item_norm)
                    partial_match = (
                        spotify_norm in item_norm
                        or item_norm in spotify_norm
                        or title.lower() in item_title_lower
                        or item_title_lower in title.lower()
                    )

                    # Compute confidence based on match quality
                    candidate_confidence = 0.0
                    if norm_exact and duration_diff <= 5000:
                        candidate_confidence = 0.95
                    elif norm_exact and duration_diff <= 15000:
                        candidate_confidence = 0.90
                    elif partial_match and duration_diff <= 5000:
                        candidate_confidence = 0.85
                    elif partial_match and duration_diff <= 15000:
                        candidate_confidence = 0.80

                    if candidate_confidence > best_confidence:
                        best_confidence = candidate_confidence
                        best_candidate = item

                if best_candidate and best_confidence >= 0.80:
                    tidal_id = str(best_candidate["id"])
                    confidence = best_confidence
                    match_source = "search"
                    matched = True
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


def _tidal_search_via_tidarr(query: str) -> list[dict]:
    """Search Tidal using tiddl CLI inside Tidarr container via Docker exec on NAS.

    Since Tidarr doesn't have a search API, and tiddl is the CLI tool that handles
    Tidal auth, we use a simple HTTP search against Tidal's public-ish API.
    Tidarr's tiddl config has the auth token.

    Alternatively, we use the Tidal API directly with a device code flow.
    For now, we'll try a direct Tidal API search (which may work without auth for basic search).
    """
    with httpx.Client(timeout=30) as client:
        # Tidal API search (public endpoint — may work without auth for basic results)
        headers = {
            "x-tidal-token": "CzET4vdadNUFQ5JU",  # Common public Tidal token
        }
        resp = client.get(
            "https://api.tidal.com/v1/search/tracks",
            params={"query": query, "limit": 10, "countryCode": "US"},
            headers=headers,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("items", [])

        # Fallback: try without token
        resp2 = client.get(
            "https://api.tidal.com/v1/search/tracks",
            params={"query": query, "limit": 10, "countryCode": "US"},
        )
        if resp2.status_code == 200:
            return resp2.json().get("items", [])

    return []


# ---------------------------------------------------------------------------
# Stage: downloading -> verifying (or error)
# ---------------------------------------------------------------------------

def _process_downloading(db_path: str):
    sync_mode = get_config(db_path, "sync_mode") or "scan"
    if sync_mode == "scan":
        return  # Scan mode: no downloads
    tracks = get_tracks_by_stage(db_path, "downloading", limit=BATCH_DOWNLOAD)
    tracks = [t for t in tracks if t.get("download_status") in ("pending", "failed")]

    max_concurrent = int(get_config(db_path, "max_concurrent_downloads") or 2)
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
    """Queue a track download via Tidarr's POST /api/save endpoint."""
    track_id = track["id"]
    tidal_id = track.get("tidal_id")
    artist = track.get("artist", "Unknown Artist")
    album = track.get("album", "Unknown Album")
    title = track.get("title", "Unknown Track")
    attempts = (track.get("download_attempts") or 0) + 1

    if not tidal_id:
        raise ValueError("No tidal_id set for track")

    # Dedup: skip if file_path is already set and file exists on disk
    existing_path = track.get("file_path")
    if existing_path and os.path.exists(existing_path):
        log.info("Track %d already has file at %s, skipping download", track_id, existing_path)
        update_track(
            db_path, track_id,
            download_status="complete",
            download_source="existing",
            pipeline_stage="verifying",
            verify_status="pending",
        )
        log_activity(db_path, "download_skipped", track_id,
                     f"File already exists: {existing_path}")
        return

    # Dedup: skip if this track_id already has a completed download in the queue
    with get_db(db_path) as conn:
        already_done = conn.execute(
            "SELECT id FROM download_queue WHERE track_id = ? AND status = 'complete'",
            (track_id,),
        ).fetchone()
    if already_done:
        log.info("Track %d already has a completed download in queue, skipping", track_id)
        update_track(
            db_path, track_id,
            download_status="complete",
            download_source="existing",
            pipeline_stage="verifying",
            verify_status="pending",
        )
        log_activity(db_path, "download_skipped", track_id,
                     "Already completed in download_queue")
        return

    update_track(db_path, track_id, download_status="downloading", download_attempts=attempts)

    with get_db(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM download_queue WHERE track_id = ? AND status NOT IN ('complete', 'failed')",
            (track_id,),
        ).fetchone()
        if not existing:
            conn.execute(
                """INSERT INTO download_queue (track_id, source, status, attempts)
                VALUES (?, 'tidarr', 'downloading', ?)""",
                (track_id, attempts),
            )

    # Queue download in Tidarr using its actual API
    tidal_url = f"https://tidal.com/browse/track/{tidal_id}"
    with httpx.Client(timeout=120) as client:
        resp = client.post(
            f"{TIDARR_URL}/api/save",
            json={"item": {
                "id": int(tidal_id),
                "artist": artist,
                "title": title,
                "type": "track",
                "quality": "max",
                "status": "queue",
                "loading": False,
                "error": False,
                "url": f"/track/{tidal_id}",
            }},
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Tidarr save failed: HTTP {resp.status_code} - {resp.text}")

    # Wait for Tidarr to complete the download
    _wait_for_tidarr_download(tidal_id)

    # Find the downloaded file in the music library
    # Tidarr downloads to the configured output directory (/shared/downloads or similar)
    dest_path = _find_and_move_downloaded_file(db_path, track_id, artist, album, title)

    if not dest_path:
        raise FileNotFoundError(f"Downloaded file not found for tidal_id={tidal_id}")

    file_hash = _sha256(dest_path)

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


def _wait_for_tidarr_download(tidal_id: str, max_wait: int = 60, poll_interval: int = 5):
    """Wait for Tidarr to finish downloading a track by polling the item-specific output."""
    start = time.time()
    time.sleep(8)  # Give Tidarr time to start and potentially finish the download

    while time.time() - start < max_wait:
        try:
            with httpx.Client(timeout=15) as client:
                # Check the item-specific output stream
                resp = client.get(f"{TIDARR_URL}/api/stream-item-output/{tidal_id}", timeout=10)
                if resp.status_code == 200:
                    text = resp.text
                    if "Move complete" in text or "Post processing complete" in text:
                        time.sleep(2)
                        return
                    if "Download succeed" in text:
                        time.sleep(3)
                        return
                    if "No file to process" in text:
                        return  # skip_existing or auth issue
        except Exception:
            pass
        time.sleep(poll_interval)
    # Timeout — proceed anyway


def _find_and_move_downloaded_file(db_path: str, track_id: int, artist: str, album: str, title: str) -> str | None:
    """Find a recently downloaded file.

    Tidarr downloads to /music (= /volume1/music/Database) using the template:
    tracks/{artist}/{artist} - {title}.flac

    So files should already be in the right place — no moving needed.
    """
    artist_first = artist.split(",")[0].strip()
    title_lower = title.lower()

    # Tidarr downloads to /downloads (= /volume1/music/Input) with template:
    # tracks/{artist}/{artist} - {title}.flac
    # Also check the main music library for existing files
    downloads_dir = os.environ.get("DOWNLOADS_PATH", "/downloads")
    downloads_tracks = os.path.join(downloads_dir, "tracks")

    search_bases = [downloads_tracks, downloads_dir, MUSIC_LIBRARY_PATH]

    for base in search_bases:
        if not os.path.isdir(base):
            continue
        # Check artist directories
        for artist_variant in [artist, artist_first, artist.replace(", ", " ")]:
            artist_dir = os.path.join(base, artist_variant)
            if not os.path.isdir(artist_dir):
                continue
            for root, dirs, files in os.walk(artist_dir):
                dirs[:] = [d for d in dirs if not d.startswith("@") and not d.endswith(".old")]
                for f in files:
                    if not f.endswith((".flac", ".m4a", ".aiff", ".wav")):
                        continue
                    if title_lower[:15] in f.lower():
                        fpath = os.path.join(root, f)
                        # If in downloads dir, move to music library
                        if fpath.startswith(downloads_dir):
                            return _move_to_library(fpath, artist, album, title, track_id)
                        return fpath

    # Fallback: search entire downloads dir for recent files matching title
    cutoff = time.time() - 600  # last 10 minutes
    for search_dir in [downloads_dir, MUSIC_LIBRARY_PATH]:
        if not os.path.isdir(search_dir):
            continue
        for root, dirs, files in os.walk(search_dir):
            dirs[:] = [d for d in dirs if not d.startswith("@") and not d.endswith(".old")]
            for f in files:
                if f.endswith((".flac",)) and title_lower[:15] in f.lower():
                    fpath = os.path.join(root, f)
                    try:
                        if os.path.getmtime(fpath) > cutoff:
                            # If found in downloads, move to music library
                            if fpath.startswith(downloads_dir):
                                return _move_to_library(fpath, artist, album, title, track_id)
                            return fpath
                    except OSError:
                        continue

    return None


def _move_to_library(src_path: str, artist: str, album: str, title: str, track_id: int) -> str:
    """Move a file to the organized library structure."""
    safe_artist = sanitize_filename(artist.split(",")[0].strip())
    safe_album = sanitize_filename(album) if album else "Singles"
    safe_title = sanitize_filename(title)

    dest_dir = os.path.join(MUSIC_LIBRARY_PATH, safe_artist, safe_album)
    os.makedirs(dest_dir, exist_ok=True)

    ext = os.path.splitext(src_path)[1] or ".flac"
    dest_path = os.path.join(dest_dir, f"{safe_title}{ext}")

    if os.path.exists(dest_path) and dest_path != src_path:
        base, extension = os.path.splitext(dest_path)
        dest_path = f"{base}_{track_id}{extension}"

    if src_path != dest_path:
        shutil.move(src_path, dest_path)

    return dest_path


def _sha256(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Stage: verifying -> organizing (or error)
# ---------------------------------------------------------------------------

def _process_verifying(db_path: str):
    tracks = get_tracks_by_stage(db_path, "verifying", limit=BATCH_VERIFY)
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
    track_id = track["id"]
    file_path = track.get("file_path")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # ffprobe
    probe_result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", file_path],
        capture_output=True, text=True, timeout=30,
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
        raise RuntimeError("No audio stream found")

    codec = audio_stream.get("codec_name", "unknown")
    sample_rate = int(audio_stream.get("sample_rate", 0))
    bit_depth = int(audio_stream.get("bits_per_raw_sample") or audio_stream.get("bits_per_sample") or 0)

    # Chromaprint
    chromaprint = None
    fp_duration = None
    try:
        fpcalc_result = subprocess.run(
            ["fpcalc", "-json", file_path], capture_output=True, text=True, timeout=60,
        )
        if fpcalc_result.returncode == 0:
            fp_data = json.loads(fpcalc_result.stdout)
            chromaprint = fp_data.get("fingerprint")
            fp_duration = fp_data.get("duration")
    except Exception as e:
        log.warning("fpcalc failed for track %d: %s", track_id, e)

    lossless_codecs = ("flac", "alac", "wav", "aiff", "pcm_s16be", "pcm_s24be", "pcm_s32be",
                       "pcm_s16le", "pcm_s24le", "pcm_s32le", "pcm_f32le", "pcm_f64le")
    is_lossless = codec in lossless_codecs and sample_rate >= 44100

    # Duration match score
    fp_match_score = None
    duration_diff = None
    spotify_duration_ms = track.get("duration_ms") or 0
    if fp_duration and spotify_duration_ms:
        spotify_duration_s = spotify_duration_ms / 1000.0
        duration_diff = abs(fp_duration - spotify_duration_s)
        if duration_diff <= 2:
            fp_match_score = 1.0
        elif duration_diff <= 10:
            fp_match_score = max(0, 1.0 - (duration_diff - 2) / 20)
        else:
            fp_match_score = max(0, 0.6 - duration_diff / 100)

    verify_pass = True
    reasons = []
    is_mismatched = False

    if not is_lossless:
        verify_pass = False
        reasons.append(f"not lossless: codec={codec}, sr={sample_rate}")

    if fp_match_score is not None and fp_match_score < min_fp_score:
        verify_pass = False
        reasons.append(f"fingerprint score low: {fp_match_score:.2f}")

    # Stricter duration-based mismatch detection
    if duration_diff is not None and duration_diff > 10:
        is_mismatched = True
        verify_pass = False
        reasons.append(f"duration mismatch ({duration_diff:.1f}s diff): likely wrong track")
    elif duration_diff is not None and duration_diff > 5:
        is_mismatched = True
        verify_pass = False
        reasons.append(f"duration suspicious ({duration_diff:.1f}s diff): possible wrong track")

    verify_status = "pass" if verify_pass else "fail"
    next_stage = "organizing" if verify_pass else "error"

    extra_updates = {}
    if is_mismatched:
        extra_updates["match_status"] = "mismatched"

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
        **extra_updates,
    )

    log_activity(
        db_path, f"verify_{verify_status}", track_id,
        f"Verification {verify_status}: codec={codec}, sr={sample_rate}, bits={bit_depth}",
        {"codec": codec, "sample_rate": sample_rate, "bit_depth": bit_depth, "is_lossless": is_lossless,
         "fp_match_score": fp_match_score, "reasons": reasons},
    )
    log.info("Track %d verified: %s (codec=%s)", track_id, verify_status, codec)


# ---------------------------------------------------------------------------
# Stage: organizing -> complete (Lexicon sync)
# ---------------------------------------------------------------------------

def _process_organizing(db_path: str):
    tracks = get_tracks_by_stage(db_path, "organizing", limit=BATCH_ORGANIZE)
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

    # Determine target year/month
    if added_at:
        try:
            dt = datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            year = dt.year
            month = dt.month
        except (ValueError, TypeError):
            year = datetime.now(tz=timezone.utc).year
            month = datetime.now(tz=timezone.utc).month
    else:
        year = datetime.now(tz=timezone.utc).year
        month = datetime.now(tz=timezone.utc).month

    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    folder_name = str(year)
    playlist_name = f"{month:02d}. {month_names[month]} {year}"

    playlist_row = _ensure_playlist(db_path, year, month, folder_name, playlist_name)

    # Lexicon API operations (uses /v1/ endpoints)
    with httpx.Client(base_url=LEXICON_API_URL, timeout=60) as client:
        # 1. Find or import the track in Lexicon
        # The file is already in the music library (/music/ = NAS /volume1/music/Database/)
        # SynologyDrive will sync it to the Mac Mini where Lexicon reads it
        # The Mac Mini path will be: /Users/willcurran/SynologyDrive/Database/{relative_path}
        relative_path = os.path.relpath(file_path, MUSIC_LIBRARY_PATH)
        mac_path = f"/Users/willcurran/SynologyDrive/Database/{relative_path}"

        # Wait for SynologyDrive to sync — only needed for freshly downloaded files
        download_source = track.get("download_source", "")
        match_source = track.get("match_source", "")
        if download_source == "tidarr":
            sync_delay = int(get_config(db_path, "synology_sync_delay_seconds") or 10)
            log.info("Waiting %ds for SynologyDrive sync (new download)...", sync_delay)
            time.sleep(sync_delay)
        else:
            log.debug("Skipping SynologyDrive delay for existing file (source=%s/%s)",
                       match_source, download_source)

        # Search Lexicon for the track by file path
        lexicon_track_id = _lexicon_find_or_import(client, mac_path, track)

        # 2. Ensure folder and playlist exist
        lexicon_folder_id = playlist_row.get("lexicon_folder_id")
        lexicon_playlist_id = playlist_row.get("lexicon_playlist_id")

        if not lexicon_folder_id:
            lexicon_folder_id = _lexicon_ensure_folder(client, folder_name)
            if lexicon_folder_id:
                with get_db(db_path) as conn:
                    conn.execute("UPDATE playlists SET lexicon_folder_id = ? WHERE id = ?",
                                 (lexicon_folder_id, playlist_row["id"]))

        if not lexicon_playlist_id:
            lexicon_playlist_id = _lexicon_ensure_playlist(client, playlist_name, lexicon_folder_id)
            if lexicon_playlist_id:
                with get_db(db_path) as conn:
                    conn.execute("UPDATE playlists SET lexicon_playlist_id = ? WHERE id = ?",
                                 (lexicon_playlist_id, playlist_row["id"]))

        # 3. Add track to playlist (only if not already present)
        if lexicon_playlist_id and lexicon_track_id:
            if _lexicon_track_in_playlist(client, lexicon_playlist_id, lexicon_track_id):
                log.info("Track %d (lexicon_id=%s) already in playlist %s, skipping add",
                         track_id, lexicon_track_id, playlist_name)
            else:
                _lexicon_add_to_playlist(client, lexicon_playlist_id, lexicon_track_id)

        # 4. Tag with [sls:spotify_id]
        if lexicon_track_id:
            _lexicon_tag_track(client, lexicon_track_id, spotify_id)

    update_track(
        db_path, track_id,
        lexicon_status="synced",
        lexicon_track_id=lexicon_track_id,
        lexicon_playlist_id=lexicon_playlist_id,
        pipeline_stage="complete",
    )

    # Auto-trigger Lexicon analysis (BPM/key detection + genre tagging)
    if lexicon_track_id and get_config(db_path, "auto_analyze_enabled") != "0":
        _trigger_lexicon_analysis(db_path, lexicon_track_id, track_id)

    if playlist_row:
        with get_db(db_path) as conn:
            conn.execute("INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id) VALUES (?, ?)",
                         (playlist_row["id"], track_id))
            conn.execute("UPDATE playlists SET track_count = track_count + 1 WHERE id = ?", (playlist_row["id"],))

    log_activity(
        db_path, "lexicon_synced", track_id,
        f"Synced to Lexicon: {track.get('artist')} - {track.get('title')} -> {playlist_name}",
        {"lexicon_track_id": lexicon_track_id, "playlist": playlist_name},
    )
    log.info("Track %d synced to Lexicon -> %s", track_id, playlist_name)


def _trigger_lexicon_analysis(db_path: str, lexicon_track_id: str, track_id: int):
    """Trigger Lexicon's BPM/key analysis and genre tagging for a synced track.

    Non-fatal: logs errors but never fails the pipeline.
    """
    track_ids_payload = [int(lexicon_track_id)]

    with httpx.Client(base_url=LEXICON_API_URL, timeout=60) as client:
        # 1. BPM / key analysis
        try:
            resp = client.post("/v1/analyze", json={"trackIds": track_ids_payload})
            if resp.status_code in (200, 201, 202, 204):
                log.info("Track %d: Lexicon analysis triggered (lexicon_id=%s)", track_id, lexicon_track_id)
            else:
                log.warning("Track %d: Lexicon /v1/analyze returned HTTP %d: %s",
                            track_id, resp.status_code, resp.text[:200])
        except Exception as e:
            log.warning("Track %d: Lexicon /v1/analyze failed: %s", track_id, e)

        # 2. Tag / genre finder
        try:
            resp = client.post("/v1/find-tags", json={"trackIds": track_ids_payload})
            if resp.status_code in (200, 201, 202, 204):
                log.info("Track %d: Lexicon find-tags triggered (lexicon_id=%s)", track_id, lexicon_track_id)
            else:
                log.warning("Track %d: Lexicon /v1/find-tags returned HTTP %d: %s",
                            track_id, resp.status_code, resp.text[:200])
        except Exception as e:
            log.warning("Track %d: Lexicon /v1/find-tags failed: %s", track_id, e)

    log_activity(
        db_path, "lexicon_analysis_triggered", track_id,
        f"Triggered Lexicon analysis for lexicon_track_id={lexicon_track_id}",
    )


def _ensure_playlist(db_path: str, year: int, month: int, folder_name: str, playlist_name: str) -> dict:
    with get_db(db_path) as conn:
        row = conn.execute("SELECT * FROM playlists WHERE year = ? AND month = ?", (year, month)).fetchone()
        if row:
            return dict(row)
        conn.execute(
            "INSERT INTO playlists (folder_name, playlist_name, year, month) VALUES (?, ?, ?, ?)",
            (folder_name, playlist_name, year, month),
        )
        return dict(conn.execute("SELECT * FROM playlists WHERE year = ? AND month = ?", (year, month)).fetchone())


# ---------------------------------------------------------------------------
# Lexicon API helpers (using /v1/ endpoints)
# ---------------------------------------------------------------------------

def _lexicon_find_or_import(client: httpx.Client, mac_path: str, track: dict) -> str | None:
    """Find a track in Lexicon by artist+title, or import it via file path."""
    spotify_title = (track.get("title") or "").lower().strip()
    spotify_artist = (track.get("artist") or "").lower().split(",")[0].strip()

    # Search by artist + title
    try:
        resp = client.get("/v1/search/tracks", params={
            "filter[artist]": track.get("artist", ""),
            "filter[title]": track.get("title", ""),
        })
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("data", {}).get("tracks", [])
            for t in results:
                # Exact path match
                if t.get("location", "") == mac_path:
                    return str(t["id"])
                # Fuzzy title match: Spotify "Hot One" matches Lexicon "Hot One (Original Mix)"
                lex_title = (t.get("title") or "").lower().strip()
                lex_artist = (t.get("artist") or "").lower().strip()
                if spotify_title in lex_title and spotify_artist in lex_artist:
                    return str(t["id"])
            # If any results at all and artist matches, take first
            if results:
                first_artist = (results[0].get("artist") or "").lower()
                if spotify_artist in first_artist:
                    return str(results[0]["id"])
    except Exception as e:
        log.warning("Lexicon search failed: %s", e)

    # Import the track file via POST /v1/tracks (with locations array)
    try:
        resp = client.post("/v1/tracks", json={"locations": [mac_path]})
        if resp.status_code in (200, 201):
            data = resp.json()
            imported = data.get("data", {}).get("tracks", [])
            if imported:
                return str(imported[0]["id"])
    except Exception as e:
        log.warning("Lexicon import via /v1/tracks failed: %s", e)

    return None


def _lexicon_ensure_folder(client: httpx.Client, folder_name: str) -> str | None:
    """Find or create a year folder in Lexicon under ROOT (parentId=1).

    Handles the case where a year folder (e.g. "2026") doesn't exist yet
    by creating it as a folder (type=1, folderType=1) under ROOT.
    """
    try:
        resp = client.get("/v1/playlists")
        if resp.status_code == 200:
            data = resp.json()
            root = data.get("data", {}).get("playlists", [])
            # Navigate past ROOT
            if root and root[0].get("name") == "ROOT":
                items = root[0].get("playlists", [])
            else:
                items = root

            for item in items:
                # type may be int 1 or string "1" depending on Lexicon version
                if item.get("name") == folder_name and str(item.get("type")) == "1":
                    log.info("Found existing Lexicon folder '%s' (id=%s)", folder_name, item["id"])
                    return str(item["id"])

            log.info("Folder '%s' not found in Lexicon, creating under ROOT (parentId=1)", folder_name)
    except Exception as e:
        log.warning("Lexicon folder search failed: %s — will attempt to create folder", e)

    # Create folder under ROOT (parentId=1)
    try:
        resp = client.post("/v1/playlist", json={
            "name": folder_name,
            "type": "1",
            "folderType": "1",
            "parentId": 1,
        })
        if resp.status_code in (200, 201):
            data = resp.json()
            folder_id = str(data.get("data", {}).get("id", data.get("id", "")))
            log.info("Created Lexicon folder '%s' (id=%s)", folder_name, folder_id)
            return folder_id
        else:
            log.warning("Lexicon folder create failed: HTTP %d - %s", resp.status_code, resp.text)
    except Exception as e:
        log.warning("Lexicon folder creation failed: %s", e)
    return None


def _lexicon_ensure_playlist(client: httpx.Client, playlist_name: str, folder_id: str | None) -> str | None:
    """Find or create a monthly playlist in Lexicon folder."""
    try:
        resp = client.get("/v1/playlists")
        if resp.status_code == 200:
            data = resp.json()
            root = data.get("data", {}).get("playlists", [])
            if root and root[0].get("name") == "ROOT":
                items = root[0].get("playlists", [])
            else:
                items = root

            # Search inside the target folder
            for item in items:
                if folder_id and str(item.get("id")) == str(folder_id):
                    for child in item.get("playlists", []):
                        if child.get("name") == playlist_name:
                            return str(child["id"])

        # Create playlist
        body = {"name": playlist_name, "type": "2"}
        if folder_id:
            body["parentId"] = int(folder_id)
        resp = client.post("/v1/playlist", json=body)
        if resp.status_code in (200, 201):
            data = resp.json()
            return str(data.get("data", {}).get("id", data.get("id", "")))
    except Exception as e:
        log.warning("Lexicon playlist operation failed: %s", e)
    return None


def _lexicon_track_in_playlist(client: httpx.Client, playlist_id: str, track_id: str) -> bool:
    """Check if a track is already in a Lexicon playlist."""
    try:
        resp = client.get(f"/v1/playlist", params={"id": int(playlist_id)})
        if resp.status_code == 200:
            data = resp.json()
            playlist_data = data.get("data", data)
            existing_ids = playlist_data.get("trackIds", [])
            return int(track_id) in existing_ids
    except Exception as e:
        log.warning("Lexicon playlist membership check failed: %s", e)
    return False


def _lexicon_add_to_playlist(client: httpx.Client, playlist_id: str, track_id: str):
    """Add a track to a playlist via PATCH /v1/playlist-tracks."""
    try:
        resp = client.patch(
            "/v1/playlist-tracks",
            json={"id": int(playlist_id), "trackIds": [int(track_id)]},
        )
        if resp.status_code not in (200, 201, 204):
            log.warning("Failed to add track %s to playlist %s: HTTP %d - %s",
                        track_id, playlist_id, resp.status_code, resp.text)
    except Exception as e:
        log.warning("Lexicon add-to-playlist failed: %s", e)


def _lexicon_tag_track(client: httpx.Client, lexicon_track_id: str, spotify_id: str):
    """Tag a track's comment with [sls:{spotify_id}] for traceability."""
    tag = f"[sls:{spotify_id}]"
    try:
        # Get current track info to preserve existing comment
        resp = client.get("/v1/tracks")
        # For efficiency, we just set the tag directly via PATCH
        client.patch("/v1/track", json={
            "id": int(lexicon_track_id),
            "edits": {"comment": tag},
        })
    except Exception as e:
        log.warning("Lexicon tag failed: %s", e)
