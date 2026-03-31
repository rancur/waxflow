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
    MAX_RETRIES = 3

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, title, artist, download_attempts FROM tracks
               WHERE match_status = 'failed'
                 AND pipeline_stage = 'error'
                 AND pipeline_error LIKE '%No Tidal match found%'"""
        ).fetchall()

        if not rows:
            return 0

        # Split into retryable vs permanently failed based on download_attempts
        # (reusing download_attempts as retry counter for unmatched tracks)
        retryable = []
        permanently_failed = []
        for r in rows:
            retry_count = r["download_attempts"] or 0
            if retry_count >= MAX_RETRIES:
                permanently_failed.append(r)
            else:
                retryable.append(r)

        # Mark permanently failed tracks so they stop being retried
        for row in permanently_failed:
            conn.execute(
                """UPDATE tracks
                    SET pipeline_error = 'Permanently unavailable on Tidal (retried 3 times)',
                        updated_at = datetime('now')
                    WHERE id = ?""",
                (row["id"],),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched_exhausted",
                    row["id"],
                    f"Permanently unavailable: {row['title']} by {row['artist']} (retried {MAX_RETRIES} times)",
                    None,
                ),
            )

        if permanently_failed:
            log.info("Marked %d tracks as permanently unavailable (max retries reached)", len(permanently_failed))

        if not retryable:
            return 0

        track_ids = [r["id"] for r in retryable]

        # Increment download_attempts as a retry counter, then reset for re-matching
        for r in retryable:
            new_count = (r["download_attempts"] or 0) + 1
            conn.execute(
                """UPDATE tracks
                    SET pipeline_stage = 'new',
                        match_status = 'pending',
                        pipeline_error = NULL,
                        download_attempts = ?,
                        updated_at = datetime('now')
                    WHERE id = ?""",
                (new_count, r["id"]),
            )

        # Log each reset
        for row in retryable:
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched",
                    row["id"],
                    f"Retry search: {row['title']} by {row['artist']} (attempt {(row['download_attempts'] or 0) + 1}/{MAX_RETRIES})",
                    None,
                ),
            )

        # Summary log entry
        conn.execute(
            "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
            (
                "retry_unmatched_batch",
                None,
                f"Reset {len(track_ids)} unmatched tracks for retry ({len(permanently_failed)} exhausted)",
                None,
            ),
        )

        return len(track_ids)
