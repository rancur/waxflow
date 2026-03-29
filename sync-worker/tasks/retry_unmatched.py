"""
Periodic retry for tracks that failed matching.
Resets 'No Tidal match found' errors back to the beginning of the pipeline
so the worker will attempt matching again.
"""

import logging

from tasks.helpers import get_db, log_activity

log = logging.getLogger("worker.retry_unmatched")


async def retry_unmatched(db_path: str):
    """Find tracks with 'No Tidal match found' errors and reset them for retry."""
    import asyncio

    count = await asyncio.to_thread(_retry_unmatched_sync, db_path)
    if count > 0:
        log.info(f"Reset {count} unmatched tracks for retry")


def _retry_unmatched_sync(db_path: str) -> int:
    """Synchronous DB work for retry_unmatched."""
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, title, artist FROM tracks
               WHERE match_status = 'failed'
                 AND pipeline_stage = 'error'
                 AND pipeline_error LIKE '%No Tidal match found%'"""
        ).fetchall()

        if not rows:
            return 0

        track_ids = [r["id"] for r in rows]

        conn.execute(
            f"""UPDATE tracks
                SET pipeline_stage = 'new',
                    match_status = 'pending',
                    pipeline_error = NULL,
                    updated_at = datetime('now')
                WHERE id IN ({','.join('?' * len(track_ids))})""",
            track_ids,
        )

        # Log each reset
        for row in rows:
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched",
                    row["id"],
                    f"Retry search: {row['title']} by {row['artist']}",
                    None,
                ),
            )

        # Summary log entry
        conn.execute(
            "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
            (
                "retry_unmatched_batch",
                None,
                f"Reset {len(track_ids)} unmatched tracks for retry",
                None,
            ),
        )

        return len(track_ids)
