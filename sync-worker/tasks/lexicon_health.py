"""Lexicon import-health canary + shared health recorder.

WaxFlow's import health has two failure modes, both of which used to be silent:

  1. Empty-import (POST /v1/tracks path). When Lexicon's Mac loses access to the
     NAS music share, ``POST /v1/tracks`` for a file under the mount returns
     HTTP 200 with an EMPTY tracks array — it imports NOTHING but looks like
     success. The reactive detector in process_pipeline raises LexiconImportEmpty
     and calls ``note_empty_import`` here.

  2. Delivery path (current architecture, 2026-07-11). The worker writes finished
     audio into container ``/music`` (== NAS /volume1/music). The Lexicon host Mac
     reads that SAME tree over an SMB mount at /Volumes/music (live — no sync lag),
     and WaxFlow imports each file by its Mac-side path. Finished files also keep
     the inherited Synology ACL so they propagate to the Mac's ~/Music replica.
     ROOT-CAUSE NOTE: a chmod on a finished file strips that Synology ACL, turning
     it into "Linux mode" that Synology Drive Server cannot see — the exact reason
     Apr/Jun downloads never reached Lexicon (proven 2026-07-11). The worker now
     copies data only and never chmods, so the ACL survives.
     The proactive ``run_canary`` verifies the NAS-side dependencies of the flow:
       * the delivery dir the worker writes to is writable by WaxFlow, and
       * Lexicon's API is reachable (the link/organize/playlist step still needs it).
     It cannot see the Mac side (SMB mount + Lexicon), so an actual empty import
     (mode 1) remains the authoritative mount-down signal.

``record_import_health`` is the single place that persists the signal to
app_config (read by the API /api/admin/health endpoint and the self-heal
monitors) and LOUDLY pages (activity log + webhook, transition-gated so a
persistent outage isn't re-paged every cycle).
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from tasks.helpers import LEXICON_API_URL, get_config, log_activity, set_config

log = logging.getLogger("worker.lexicon_health")

# app_config keys (also read by sync-api /api/admin/health and monitor-parity.sh)
STATUS_KEY = "lexicon_import_health"          # ok | watch_dir_unwritable | lexicon_unreachable | mount_down | skipped_scan | unknown
MOUNT_OK_KEY = "lexicon_mount_ok"             # "1" | "0" | "" (unknown)
CHECKED_AT_KEY = "lexicon_mount_checked_at"   # ISO8601 UTC
DETAIL_KEY = "lexicon_mount_detail"           # human message
EMPTY_COUNT_KEY = "lexicon_import_empty_count"

# Statuses that mean "new imports will fail" -> page LOUD.
CRITICAL_STATUSES = frozenset({"mount_down", "lexicon_unreachable", "watch_dir_unwritable"})

DEFAULT_WATCH_DIR = "/music"  # container path == NAS /volume1/music == Mac /Volumes/music (SMB). Where downloads land.


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def record_import_health(
    db_path: str,
    status: str,
    detail: str,
    *,
    ok: bool | None,
    source: str,
    notify: bool = True,
) -> None:
    """Persist the import-health signal and page loudly on a bad transition.

    ``ok`` is tri-state: True (healthy), False (broken -> pageable), or None
    (inconclusive/advisory — recorded but never paged). Webhook paging is
    transition-gated: it fires only when we move from a not-critical state into a
    critical one, so a persistently-down chain is not re-paged every cycle.
    """
    prev_status = (get_config(db_path, STATUS_KEY) or "").strip()

    set_config(db_path, STATUS_KEY, status)
    set_config(db_path, DETAIL_KEY, detail)
    set_config(db_path, CHECKED_AT_KEY, _now_iso())
    if ok is True:
        set_config(db_path, MOUNT_OK_KEY, "1")
    elif ok is False:
        set_config(db_path, MOUNT_OK_KEY, "0")
    else:
        set_config(db_path, MOUNT_OK_KEY, "")

    is_critical = status in CRITICAL_STATUSES
    if not is_critical:
        if ok is True and prev_status in CRITICAL_STATUSES:
            log.info("Lexicon import-health RECOVERED (%s): %s", source, detail)
            log_activity(db_path, "lexicon_import_recovered", None,
                         f"Lexicon import health recovered: {detail}", {"source": source})
            _post_webhook(db_path, "lexicon_import_recovered", status, detail, source)
        return

    log.error("LEXICON IMPORT HEALTH CRITICAL (%s): %s — %s", source, status, detail)
    log_activity(db_path, "lexicon_import_critical", None,
                 f"[{status}] {detail}", {"source": source, "status": status})
    if notify and prev_status not in CRITICAL_STATUSES:
        _post_webhook(db_path, "lexicon_import_critical", status, detail, source)


def _post_webhook(db_path: str, event: str, status: str, detail: str, source: str) -> None:
    webhook_url = get_config(db_path, "webhook_url")
    if not webhook_url:
        return
    try:
        payload = {
            "event": event,
            "status": status,
            "detail": detail,
            "source": source,
            "service": "waxflow",
            "checked_at": _now_iso(),
        }
        with httpx.Client(timeout=5) as client:
            client.post(webhook_url, json=payload)
    except Exception:
        pass  # never let a paging failure break the pipeline/canary


def note_empty_import(db_path: str, mac_path: str, source: str = "pipeline") -> None:
    """Record a REAL empty-import failure observed during pipeline processing.

    Authoritative signal (an actual import returned 0 tracks). Bumps the distinct
    counter and drives the shared health recorder to mount_down.
    """
    try:
        prev = int(get_config(db_path, EMPTY_COUNT_KEY) or "0")
    except ValueError:
        prev = 0
    set_config(db_path, EMPTY_COUNT_KEY, str(prev + 1))
    record_import_health(
        db_path,
        "mount_down",
        f"Lexicon returned success but imported 0 tracks for {mac_path} — the NAS "
        f"music mount is likely unreachable on the Lexicon host. New imports are "
        f"silently failing.",
        ok=False,
        source=source,
    )


def _check_watch_dir_writable(watch_dir: str) -> tuple[bool, str]:
    """Verify WaxFlow can write to the staging/watch dir the import flow depends on.

    Writes and immediately deletes a hidden, non-audio dotfile so Lexicon's
    watch-folder importer (audio-only) never touches it and Synology Drive churn
    is negligible.
    """
    if not os.path.isdir(watch_dir):
        return False, f"watch/staging dir {watch_dir} does not exist in the WaxFlow container"
    probe = os.path.join(watch_dir, f".waxflow-canary-{os.getpid()}.tmp")
    try:
        with open(probe, "w") as f:
            f.write(str(time.time()))
        os.remove(probe)
        return True, f"watch/staging dir {watch_dir} is writable"
    except Exception as e:
        try:
            if os.path.exists(probe):
                os.remove(probe)
        except Exception:
            pass
        return False, f"cannot write to watch/staging dir {watch_dir}: {e}"


def _check_lexicon_reachable(api: str) -> tuple[bool, str]:
    """Cheap Lexicon API reachability check (reads Lexicon's DB, not the mount)."""
    try:
        with httpx.Client(base_url=api, timeout=10) as client:
            r = client.get("/v1/playlists")
        if r.status_code == 200:
            return True, f"Lexicon API reachable at {api}"
        return False, f"Lexicon API at {api} returned HTTP {r.status_code}"
    except Exception as e:
        return False, f"Lexicon API unreachable at {api} ({e})"


def run_canary(db_path: str) -> dict:
    """Proactive import-health self-check for the watch-folder flow.

    Persists + pages via record_import_health. Returns a small dict (also used by
    tests)."""
    api = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    watch_dir = get_config(db_path, "lexicon_watch_dir") or os.environ.get("DOWNLOADS_PATH", DEFAULT_WATCH_DIR)

    # 1) Staging/watch dir must be writable — without it, WaxFlow can't stage any
    #    download for Synology Drive to sync into Lexicon's watch folder.
    write_ok, write_detail = _check_watch_dir_writable(watch_dir)
    if not write_ok:
        detail = f"WaxFlow cannot stage downloads — {write_detail}. New imports will fail."
        record_import_health(db_path, "watch_dir_unwritable", detail, ok=False, source="canary")
        return {"status": "watch_dir_unwritable", "detail": detail, "ok": False}

    # 2) Lexicon API reachability — the link/organize/playlist step still needs it,
    #    and an unreachable Lexicon means the whole import surface is down.
    lex_ok, lex_detail = _check_lexicon_reachable(api)
    if not lex_ok:
        detail = f"{lex_detail}. New imports/links will fail."
        record_import_health(db_path, "lexicon_unreachable", detail, ok=False, source="canary")
        return {"status": "lexicon_unreachable", "detail": detail, "ok": False}

    detail = f"{write_detail}; {lex_detail}."
    record_import_health(db_path, "ok", detail, ok=True, source="canary")
    return {"status": "ok", "detail": detail, "ok": True}


async def lexicon_health_check(db_path: str):
    """Async wrapper for the worker task loop."""
    await asyncio.to_thread(run_canary, db_path)
