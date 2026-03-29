"""Daily Lexicon backup task: verify Lexicon API health and record backup entry."""

import asyncio
import logging
import time

import httpx

from tasks.helpers import get_db, get_config, log_activity, LEXICON_API_URL

log = logging.getLogger("worker.backup_lexicon")


def _backup(db_path: str):
    """Synchronous backup verification (runs in thread)."""
    lexicon_url = LEXICON_API_URL
    connected = False
    latency_ms = None
    error_msg = None
    track_count = None

    # Verify Lexicon API is reachable
    try:
        t0 = time.monotonic()
        resp = httpx.get(f"{lexicon_url}/v1/tracks", timeout=10.0)
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        if resp.status_code == 200:
            connected = True
            data = resp.json()
            if isinstance(data, list):
                track_count = len(data)
            elif isinstance(data, dict) and "data" in data:
                tracks = data["data"]
                track_count = len(tracks) if isinstance(tracks, list) else None
        else:
            error_msg = f"HTTP {resp.status_code}"
    except Exception as e:
        error_msg = str(e)

    # Record backup entry
    status = "verified" if connected else "error"
    note = (
        f"Lexicon API healthy ({track_count} tracks, {latency_ms}ms)"
        if connected
        else f"Lexicon API unreachable: {error_msg}. Manual backup recommended."
    )

    with get_db(db_path) as conn:
        conn.execute(
            "INSERT INTO lexicon_backups (backup_path, backup_size_bytes, trigger) VALUES (?, ?, ?)",
            (note, track_count or 0, "scheduled"),
        )

    log_activity(
        db_path,
        "lexicon_backup",
        None,
        f"Lexicon backup {status}: {note}",
        {
            "status": status,
            "connected": connected,
            "latency_ms": latency_ms,
            "track_count": track_count,
            "error": error_msg,
        },
    )

    log.info("Lexicon backup %s: %s", status, note)


async def backup_lexicon(db_path: str):
    """Run Lexicon backup verification (async wrapper)."""
    await asyncio.to_thread(_backup, db_path)
