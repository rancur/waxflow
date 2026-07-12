"""Lexicon backup bookkeeping — HONEST edition.

IMPORTANT / HISTORY
-------------------
This task USED to `GET /v1/tracks` and, on a 200, record a row that read
"Lexicon API healthy … backup verified". That was dangerously misleading: an
API ping is NOT a backup. Will's entire Lexicon library DB (tracks, playlists,
cue points, tags, links — ``~/Library/Application Support/lexicon/main.db`` on
the Mac) was never actually copied, yet the dashboard implied it was safe. That
false sense of safety is exactly the failure this repo is now correcting.

THE REAL BACKUP lives in ``scripts/backup-lexicon-db.sh``, which runs on the ops
Mac (the box that can SSH both the Lexicon Mac and the NAS — the worker
container can reach neither the Mac's filesystem nor a `sqlite3 .backup` of it).
It takes a consistent SQLite online backup (no need to quit Lexicon), verifies
``PRAGMA integrity_check`` + a Track row-count, gzips it, and stores rotated
copies on BOTH the Mac and the NAS, writing a heartbeat JSON. It is scheduled
daily via the ``com.openclaw.waxflow-lexicon-backup`` LaunchAgent and should be
run manually before any delicate library operation.

What THIS in-worker task now does (and only this):
  * probes Lexicon API reachability (a liveness signal, useful for the pipeline)
  * records the probe TRUTHFULLY as ``lexicon_api_probe`` — never as a "backup"
  * NEVER writes a row that claims the DB was backed up.

So the dashboard/activity log can no longer mistake a ping for a backup.
"""

import asyncio
import logging
import time

import httpx

from tasks.helpers import get_db, log_activity, LEXICON_API_URL

log = logging.getLogger("worker.backup_lexicon")


def _probe(db_path: str):
    """Synchronous Lexicon API liveness probe (runs in a thread).

    This is a HEALTH PROBE, not a backup. It records reachability only.
    """
    lexicon_url = LEXICON_API_URL
    connected = False
    latency_ms = None
    error_msg = None
    track_count = None

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

    status = "reachable" if connected else "unreachable"
    note = (
        f"Lexicon API reachable ({track_count} tracks, {latency_ms}ms). "
        f"NB: the DB backup is done by scripts/backup-lexicon-db.sh, not here."
        if connected
        else (
            f"Lexicon API unreachable: {error_msg}. "
            f"(This is only a liveness probe; the real DB backup runs externally.)"
        )
    )

    # Record the PROBE truthfully. We intentionally no longer INSERT into
    # lexicon_backups here — that table is for real backup artifacts, and this
    # task produces none. Writing a fake "backup" row is what caused the gap.
    log_activity(
        db_path,
        "lexicon_api_probe",
        None,
        f"Lexicon API {status}: {note}",
        {
            "status": status,
            "connected": connected,
            "latency_ms": latency_ms,
            "track_count": track_count,
            "error": error_msg,
            "backup_performed": False,
            "real_backup": "scripts/backup-lexicon-db.sh (ops Mac, daily)",
        },
    )

    log.info("Lexicon API probe %s: %s", status, note)


async def backup_lexicon(db_path: str):
    """Async wrapper. Despite the historical name, this only PROBES Lexicon API
    liveness and records it truthfully — the real DB backup is external
    (scripts/backup-lexicon-db.sh). Kept under this name so the existing worker
    schedule keeps calling it, but it can never again claim a phantom backup."""
    await asyncio.to_thread(_probe, db_path)
