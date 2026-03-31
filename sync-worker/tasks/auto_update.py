"""Auto-update task: check GitHub for new releases and signal updates."""

import asyncio
import json
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import httpx

from tasks.helpers import get_db, get_config, set_config, log_activity

log = logging.getLogger("worker.auto_update")

GITHUB_REPO = "rancur/spotify-lexicon-sync"
BACKUP_DIR = "/app/data/backups"
SIGNAL_FILE = Path("/app/data/.update-requested")


def _get_current_version() -> str:
    version_path = Path("/app/VERSION")
    try:
        if version_path.exists():
            return version_path.read_text().strip()
    except Exception:
        pass
    return "unknown"


def _check_github_release() -> dict | None:
    """Fetch latest release from GitHub. Returns release info or None."""
    try:
        resp = httpx.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.warning("Failed to check GitHub releases: %s", e)
    return None


def _is_right_time(schedule: str) -> bool:
    """Check if now is the right time to auto-update based on schedule."""
    now = datetime.now()

    if schedule == "daily_3am":
        # Allow within the 3:00-3:59 AM window
        return now.hour == 3
    elif schedule == "weekly_sunday_3am":
        return now.weekday() == 6 and now.hour == 3
    elif schedule == "manual":
        return False

    return False


def _create_backup(db_path: str) -> str | None:
    """Create a pre-update backup. Returns timestamp or None on failure."""
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")

        db_src = os.environ.get("SLS_DB_PATH", db_path)
        db_dst = f"{BACKUP_DIR}/sync_{timestamp}.db"
        shutil.copy2(db_src, db_dst)

        # Backup config
        with get_db(db_path) as conn:
            rows = conn.execute("SELECT key, value FROM app_config").fetchall()
            config = {r["key"]: r["value"] for r in rows}

        config_dst = f"{BACKUP_DIR}/config_{timestamp}.json"
        with open(config_dst, "w") as f:
            json.dump(config, f, indent=2)

        # Prune old backups (keep last 10)
        import glob
        db_backups = sorted(glob.glob(f"{BACKUP_DIR}/sync_*.db"))
        for old in db_backups[:-10]:
            os.remove(old)
            config_pair = old.replace("sync_", "config_").replace(".db", ".json")
            if os.path.exists(config_pair):
                os.remove(config_pair)

        log.info("Pre-update backup created: %s", timestamp)
        return timestamp
    except Exception as e:
        log.error("Failed to create pre-update backup: %s", e)
        return None


def _auto_update(db_path: str):
    """Check for updates and signal if one is available and conditions are met."""
    current = _get_current_version()

    # Check GitHub for latest release
    release = _check_github_release()
    if not release:
        return

    latest = release.get("tag_name", "").lstrip("v")
    update_available = latest != current and latest > current

    # Record the check time
    set_config(db_path, "last_update_check", time.strftime("%Y-%m-%dT%H:%M:%S"))
    set_config(db_path, "latest_available_version", latest)
    set_config(db_path, "latest_release_url", release.get("html_url", ""))
    set_config(db_path, "latest_release_notes", (release.get("body") or "")[:500])
    set_config(db_path, "latest_release_published", release.get("published_at", ""))

    if not update_available:
        log.info("No update available (current: %s, latest: %s)", current, latest)
        return

    log.info("Update available: %s -> %s", current, latest)

    # Check if auto-update is enabled
    enabled = get_config(db_path, "auto_update_enabled")
    if enabled != "1":
        log.info("Auto-update disabled, skipping automatic update")
        return

    # Check schedule
    schedule = get_config(db_path, "auto_update_schedule") or "daily_3am"
    if not _is_right_time(schedule):
        log.info("Not the right time for auto-update (schedule: %s)", schedule)
        return

    # Don't signal if already signaled
    if SIGNAL_FILE.exists():
        log.info("Update already signaled, skipping")
        return

    # Create backup before update if enabled
    backup_before = get_config(db_path, "auto_backup_before_update")
    if backup_before != "0":
        backup_ts = _create_backup(db_path)
        if backup_ts:
            log_activity(
                db_path, "auto_update_backup", None,
                f"Pre-update backup created: {backup_ts}",
                {"version_from": current, "version_to": latest},
            )

    # Write signal file for the deploy script
    try:
        SIGNAL_FILE.write_text(
            json.dumps({
                "requested_at": time.time(),
                "current_version": current,
                "target_version": latest,
                "triggered_by": "auto_update",
            })
        )
        log_activity(
            db_path, "auto_update_triggered", None,
            f"Auto-update triggered: {current} -> {latest}",
            {"release_url": release.get("html_url"), "schedule": schedule},
        )
        log.info("Update signal written for %s -> %s", current, latest)
    except Exception as e:
        log.error("Failed to write update signal: %s", e)


async def auto_update(db_path: str):
    """Run auto-update check (async wrapper)."""
    await asyncio.to_thread(_auto_update, db_path)
