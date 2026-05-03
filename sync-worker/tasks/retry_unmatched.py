"""
Periodic retry for tracks that failed matching.
Increments match_retry_depth so each retry uses a progressively fuzzier
search pass, up to the configurable fuzzy_retry_depth limit.
"""

import logging

from tasks.helpers import get_config, get_db, log_activity

log = logging.getLogger("worker.retry_unmatched")

DEFAULT_MAX_DEPTH = 3


async def retry_unmatched(db_path: str):
    """Find tracks with 'No Tidal match found' errors and reset them for retry."""
    import asyncio

    count = await asyncio.to_thread(_retry_unmatched_sync, db_path)
    if count > 0:
        log.info("Reset %d unmatched tracks for fuzzy retry", count)


def _retry_unmatched_sync(db_path: str) -> int:
    """Synchronous DB work for retry_unmatched."""
    max_depth_cfg = get_config(db_path, "fuzzy_retry_depth")
    try:
        max_depth = int(max_depth_cfg) if max_depth_cfg is not None else DEFAULT_MAX_DEPTH
    except (ValueError, TypeError):
        max_depth = DEFAULT_MAX_DEPTH

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, title, artist, match_retry_depth FROM tracks
               WHERE match_status = 'failed'
                 AND pipeline_stage = 'error'
                 AND pipeline_error LIKE '%No Tidal match found%'"""
        ).fetchall()

        if not rows:
            return 0

        retryable = []
        exhausted = []
        for r in rows:
            depth = r["match_retry_depth"] or 0
            if depth >= max_depth:
                exhausted.append(r)
            else:
                retryable.append(r)

        # Mark exhausted tracks so they stop being picked up
        for row in exhausted:
            conn.execute(
                """UPDATE tracks
                      SET pipeline_error = 'Permanently unavailable on Tidal (all fuzzy passes exhausted)',
                          updated_at = datetime('now')
                    WHERE id = ?""",
                (row["id"],),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                (
                    "retry_unmatched_exhausted",
                    row["id"],
                    f"Permanently unavailable: {row['title']} by {row['artist']} "
                    f"(exhausted {max_depth} fuzzy pass(es))",
                ),
            )

        if exhausted:
            log.info(
                "Marked %d tracks as permanently unavailable (max depth %d reached)",
                len(exhausted), max_depth,
            )

        if not retryable:
            return 0

        for r in retryable:
            new_depth = (r["match_retry_depth"] or 0) + 1
            conn.execute(
                """UPDATE tracks
                      SET pipeline_stage = 'matching',
                          match_status = 'pending',
                          pipeline_error = NULL,
                          match_retry_depth = ?,
                          updated_at = datetime('now')
                    WHERE id = ?""",
                (new_depth, r["id"]),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) "
                "VALUES (?, ?, ?, ?)",
                (
                    "retry_unmatched",
                    r["id"],
                    f"Fuzzy retry pass {new_depth}/{max_depth}: "
                    f"{r['title']} by {r['artist']}",
                    f'{{"depth": {new_depth}, "max_depth": {max_depth}}}',
                ),
            )

        conn.execute(
            "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
            (
                "retry_unmatched_batch",
                None,
                f"Reset {len(retryable)} tracks for fuzzy retry "
                f"({len(exhausted)} exhausted, max_depth={max_depth})",
            ),
        )

        return len(retryable)
