"""Replication gate — hold a Lexicon import until the file exists on the Mac.

WHY THIS EXISTS (2026-08-08)
    WaxFlow now hands Lexicon a LOCAL Mac path (/Users/willcurran/Music/Database/...)
    instead of an SMB path (/Volumes/music/...). That is what finally makes Engine DJ
    export work — Engine refuses /Volumes paths, which is why 40 tracks carrying a
    '/Volumes/Macintosh HD/' prefix never reached it.

    The cost of local paths is replication lag. The worker writes to the NAS; a
    one-way rsync agent on the Mac (scripts/sync-nas-to-mac.sh, every 120 s) pulls it
    down. Between those two events the local path does not exist yet, and importing
    then is exactly the silent failure mode WaxFlow already fights: Lexicon returns
    HTTP 200 having imported 0 tracks (see lexicon_health.note_empty_import).

    So: before importing, check that the file predates the last completed sync pass.
    The sync agent publishes a heartbeat into the shared Input/ directory, which the
    worker sees at /downloads/.waxflow-sync-heartbeat.

DESIGN NOTES
    * FAIL OPEN. A missing, malformed, or stale heartbeat must never stall the
      pipeline — if the sync agent dies, imports proceed and the existing
      empty-import + import_catchup machinery handles any fallout. A gate that can
      deadlock the pipeline is worse than the lag it prevents.
    * BOUNDED HOLD. A track is held for at most sync_gate_max_hold_seconds; past
      that we import anyway rather than spin forever on a file that will never
      replicate (e.g. it lives in Database/Aktive, which the sync agent's SSH-side
      change detection cannot see).
    * NO NEW STATE. Held tracks simply stay in 'organizing' and are retried on the
      next pipeline cycle (~10 s). No queue rows, no error state, nothing to drain.

Config (all read live from app_config, no redeploy needed):
    sync_gate_enabled                    default 1
    sync_gate_heartbeat_path             default /downloads/.waxflow-sync-heartbeat
    sync_gate_max_hold_seconds           default 3600
    sync_gate_heartbeat_max_age_seconds  default 1800
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time

log = logging.getLogger(__name__)

DEFAULT_HEARTBEAT = "/downloads/.waxflow-sync-heartbeat"
DEFAULT_MAX_HOLD = 3600
DEFAULT_HEARTBEAT_MAX_AGE = 1800


def get_config(db_path: str, key: str) -> str | None:
    """Read one app_config value.

    Deliberately NOT tasks.helpers.get_config: importing helpers pulls in spotipy
    and the whole Spotify client stack, which would make this leaf module
    un-importable in a bare test environment. The read is three lines; the coupling
    is not worth it.
    """
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT value FROM app_config WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def _int_config(db_path: str, key: str, default: int) -> int:
    try:
        return int(str(get_config(db_path, key) or default).strip())
    except (TypeError, ValueError):
        return default


def is_enabled(db_path: str) -> bool:
    return str(get_config(db_path, "sync_gate_enabled") or "1").strip().lower() in (
        "1", "true", "yes", "on",
    )


def read_heartbeat(db_path: str) -> dict | None:
    """Last sync-agent heartbeat, or None if absent/unparseable/stale."""
    path = (get_config(db_path, "sync_gate_heartbeat_path") or DEFAULT_HEARTBEAT).strip()
    try:
        with open(path, encoding="utf-8") as fh:
            hb = json.load(fh)
    except (OSError, ValueError):
        return None
    try:
        completed = int(hb.get("completed_at", 0))
    except (TypeError, ValueError):
        return None
    if completed <= 0:
        return None
    max_age = _int_config(db_path, "sync_gate_heartbeat_max_age_seconds",
                          DEFAULT_HEARTBEAT_MAX_AGE)
    if time.time() - completed > max_age:
        return None  # agent is dead or wedged -> fail open
    hb["completed_at"] = completed
    return hb


def is_replicated(db_path: str, file_path: str | None, track: dict | None = None) -> tuple[bool, str]:
    """Has ``file_path`` had time to reach the Mac?

    Returns (ok_to_import, reason). ``ok_to_import`` is True whenever we cannot
    prove the file is still in flight — see FAIL OPEN above.
    """
    if not is_enabled(db_path):
        return True, "gate disabled"
    if not file_path:
        return True, "no file_path"

    hb = read_heartbeat(db_path)
    if hb is None:
        return True, "no usable sync heartbeat (failing open)"
    if hb.get("status") != "ok":
        return True, f"sync heartbeat status={hb.get('status')} (failing open)"

    try:
        mtime = os.path.getmtime(file_path)
    except OSError:
        # The worker cannot see its own file; that is a different problem and the
        # normal import path will surface it.
        return True, "file not stat-able by worker"

    if mtime <= hb["completed_at"]:
        return True, "replicated"

    # Still in flight — but do not hold forever.
    max_hold = _int_config(db_path, "sync_gate_max_hold_seconds", DEFAULT_MAX_HOLD)
    if time.time() - mtime > max_hold:
        log.warning(
            "sync_gate: %s still not replicated after %ss — importing anyway",
            file_path, max_hold,
        )
        return True, f"max hold {max_hold}s exceeded"

    return False, (
        f"awaiting replication (file {int(time.time() - mtime)}s old, "
        f"last sync {int(time.time() - hb['completed_at'])}s ago)"
    )
