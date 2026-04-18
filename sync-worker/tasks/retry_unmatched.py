"""
Periodic retry for tracks that failed Tidal matching.
On each retry, fuzzy_depth is incremented so _match_track uses progressively
broader search queries (feat-stripped, version-stripped, bare title, etc.).
When fuzzy_depth reaches fuzzy_match_max_depth the track is marked permanently
unavailable.
"""

import json
import logging

from tasks.helpers import get_config, get_db, log_activity

log = logging.getLogger("worker.retry_unmatched")


async def retry_unmatched(db_path: str):
    """Find tracks with 'No Tidal match found' errors and retry with deeper fuzzy search."""
    import asyncio

    count = await asyncio.to_thread(_retry_unmatched_sync, db_path)
    if count > 0:
        log.info("Reset %d unmatched tracks for fuzzy retry", count)


def _retry_unmatched_sync(db_path: str) -> int:
    """Synchronous DB work for retry_unmatched."""
    max_depth = int(get_config(db_path, "fuzzy_match_max_depth") or "4")

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, title, artist, fuzzy_depth FROM tracks
               WHERE match_status = 'failed'
                 AND pipeline_stage = 'error'
                 AND pipeline_error LIKE '%No Tidal match found%'"""
        ).fetchall()

        if not rows:
            return 0

        retryable = []
        permanently_failed = []
        for r in rows:
            depth = r["fuzzy_depth"] or 0
            if depth >= max_depth:
                permanently_failed.append(r)
            else:
                retryable.append(r)

        for row in permanently_failed:
            depth = row["fuzzy_depth"] or 0
            conn.execute(
                """UPDATE tracks
                    SET pipeline_error = ?,
                        updated_at = datetime('now')
                    WHERE id = ?""",
                (f"No Tidal match after {depth} fuzzy search passes", row["id"]),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched_exhausted",
                    row["id"],
                    f"Permanently unavailable: {row['title']} by {row['artist']} (exhausted {depth} fuzzy passes)",
                    None,
                ),
            )

        if permanently_failed:
            log.info(
                "Marked %d tracks as permanently unavailable (fuzzy_depth exhausted at %d)",
                len(permanently_failed), max_depth,
            )

        if not retryable:
            return 0

        for r in retryable:
            new_depth = (r["fuzzy_depth"] or 0) + 1
            conn.execute(
                """UPDATE tracks
                    SET pipeline_stage = 'new',
                        match_status = 'pending',
                        pipeline_error = NULL,
                        fuzzy_depth = ?,
                        updated_at = datetime('now')
                    WHERE id = ?""",
                (new_depth, r["id"]),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched",
                    r["id"],
                    f"Fuzzy retry depth {new_depth}/{max_depth}: {r['title']} by {r['artist']}",
                    json.dumps({"fuzzy_depth": new_depth, "max_depth": max_depth}),
                ),
            )

        conn.execute(
            "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
            (
                "retry_unmatched_batch",
                None,
                f"Queued {len(retryable)} tracks for fuzzy retry ({len(permanently_failed)} exhausted)",
                None,
            ),
        )

        return len(retryable)
