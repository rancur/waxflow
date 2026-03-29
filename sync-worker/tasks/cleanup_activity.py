"""Cleanup old activity_log entries beyond the configured retention period."""

import asyncio
import logging

from tasks.helpers import get_config, get_db

log = logging.getLogger("worker.cleanup_activity")


async def cleanup_activity(db_path: str):
    """Delete activity_log entries older than the configured retention (default 365 days)."""
    await asyncio.to_thread(_cleanup, db_path)


def _cleanup(db_path: str):
    retention_days = 365
    try:
        val = get_config(db_path, "activity_log_retention_days")
        if val:
            retention_days = int(val)
    except Exception:
        pass

    with get_db(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM activity_log WHERE created_at < datetime('now', ?)",
            (f"-{retention_days} days",),
        )
        deleted = cursor.rowcount

    if deleted > 0:
        log.info("Cleaned up %d activity_log entries older than %d days", deleted, retention_days)
    else:
        log.debug("No activity_log entries older than %d days to clean up", retention_days)
