"""Index music library files by ISRC and other metadata for fast duplicate detection."""
import asyncio
import logging
import os
import sqlite3
import subprocess
import json
import time

log = logging.getLogger("worker.index_library")


def _index(db_path: str):
    """Synchronous library indexer (runs in thread)."""
    from tasks.helpers import get_db, get_config, set_config, log_activity

    music_path = os.environ.get("MUSIC_LIBRARY_PATH", "/music")
    downloads_path = os.environ.get("DOWNLOADS_PATH", "/downloads")

    # Create index table if not exists
    with get_db(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS file_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            isrc TEXT,
            title TEXT,
            artist TEXT,
            album TEXT,
            duration_seconds REAL,
            codec TEXT,
            sample_rate INTEGER,
            file_size INTEGER,
            last_indexed TEXT DEFAULT (datetime('now'))
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_isrc ON file_index(isrc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_title ON file_index(title)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_artist ON file_index(artist)")

    # Get last index time
    last_index = get_config(db_path, "last_library_index") or "1970-01-01"

    indexed = 0
    skipped = 0
    errors = 0

    for base_dir in [music_path, downloads_path]:
        if not os.path.isdir(base_dir):
            continue
        for root, dirs, files in os.walk(base_dir):
            dirs[:] = [d for d in dirs if not d.startswith("@") and not d.endswith(".old")]
            for f in files:
                if not f.lower().endswith((".flac", ".aiff", ".m4a", ".wav", ".mp3")):
                    continue
                fpath = os.path.join(root, f)

                # Skip if already indexed and file hasn't changed
                try:
                    mtime = os.path.getmtime(fpath)
                    fsize = os.path.getsize(fpath)
                except OSError:
                    continue

                with get_db(db_path) as conn:
                    existing = conn.execute(
                        "SELECT id, file_size FROM file_index WHERE file_path = ?", (fpath,)
                    ).fetchone()
                    if existing and existing[1] == fsize:
                        skipped += 1
                        continue

                # Extract metadata using ffprobe
                try:
                    result = subprocess.run(
                        ["ffprobe", "-v", "quiet", "-print_format", "json",
                         "-show_format", "-show_streams", fpath],
                        capture_output=True, text=True, timeout=15
                    )
                    if result.returncode != 0:
                        errors += 1
                        continue

                    probe = json.loads(result.stdout)
                    fmt = probe.get("format", {})
                    tags = fmt.get("tags", {})
                    # Tags can be uppercase or lowercase
                    tags_lower = {k.lower(): v for k, v in tags.items()}

                    audio = None
                    for s in probe.get("streams", []):
                        if s.get("codec_type") == "audio":
                            audio = s
                            break

                    isrc = tags_lower.get("isrc", tags_lower.get("tsrc", "")).strip() or None
                    title = tags_lower.get("title", "").strip() or None
                    artist = tags_lower.get("artist", tags_lower.get("album_artist", "")).strip() or None
                    album = tags_lower.get("album", "").strip() or None
                    duration = float(fmt.get("duration", 0)) if fmt.get("duration") else None
                    codec = audio.get("codec_name") if audio else None
                    sample_rate = int(audio.get("sample_rate", 0)) if audio else None

                    with get_db(db_path) as conn:
                        conn.execute("""INSERT OR REPLACE INTO file_index
                            (file_path, isrc, title, artist, album, duration_seconds, codec, sample_rate, file_size)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (fpath, isrc, title, artist, album, duration, codec, sample_rate, fsize))
                    indexed += 1

                except Exception as e:
                    errors += 1
                    if indexed % 100 == 0:
                        log.warning("Index error for %s: %s", fpath, e)

                # Log progress periodically
                if (indexed + skipped) % 500 == 0 and indexed > 0:
                    log.info("Indexing progress: %d indexed, %d skipped, %d errors", indexed, skipped, errors)

    set_config(db_path, "last_library_index", time.strftime("%Y-%m-%dT%H:%M:%S"))
    log_activity(db_path, "library_indexed", None,
                 f"Library index complete: {indexed} new, {skipped} unchanged, {errors} errors",
                 {"indexed": indexed, "skipped": skipped, "errors": errors})
    log.info("Library index complete: %d indexed, %d skipped, %d errors", indexed, skipped, errors)


async def index_library(db_path: str):
    """Run library indexing (async wrapper)."""
    await asyncio.to_thread(_index, db_path)
