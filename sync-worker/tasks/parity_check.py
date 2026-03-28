"""Parity check: compare Spotify total vs Lexicon synced count."""

import asyncio
import logging

from tasks.helpers import get_db, get_spotify_client, log_activity, set_config

log = logging.getLogger("worker.parity")


def _check(db_path: str):
    """Synchronous parity check (runs in thread)."""
    with get_db(db_path) as conn:
        total_row = conn.execute("SELECT COUNT(*) as cnt FROM tracks").fetchone()
        synced_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE lexicon_status = 'synced'"
        ).fetchone()
        error_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE pipeline_stage = 'error'"
        ).fetchone()
        in_progress_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE pipeline_stage NOT IN ('complete', 'error')"
        ).fetchone()

    total = total_row["cnt"] if total_row else 0
    synced = synced_row["cnt"] if synced_row else 0
    errors = error_row["cnt"] if error_row else 0
    in_progress = in_progress_row["cnt"] if in_progress_row else 0

    parity_pct = (synced / total * 100) if total > 0 else 0.0

    # Also check Spotify liked count if auth is available
    spotify_total = None
    sp = get_spotify_client(db_path)
    if sp:
        try:
            results = sp.current_user_saved_tracks(limit=1)
            spotify_total = results.get("total", 0)
        except Exception as e:
            log.warning("Could not fetch Spotify total: %s", e)

    # Store parity stats in config for the dashboard API to read
    set_config(db_path, "parity_total_tracks", str(total))
    set_config(db_path, "parity_synced", str(synced))
    set_config(db_path, "parity_errors", str(errors))
    set_config(db_path, "parity_in_progress", str(in_progress))
    set_config(db_path, "parity_pct", f"{parity_pct:.1f}")
    if spotify_total is not None:
        set_config(db_path, "parity_spotify_total", str(spotify_total))
        missing = spotify_total - total
        set_config(db_path, "parity_missing_from_db", str(max(0, missing)))

    log.info(
        "Parity: %d total, %d synced (%.1f%%), %d errors, %d in-progress%s",
        total, synced, parity_pct, errors, in_progress,
        f", spotify={spotify_total}" if spotify_total is not None else "",
    )

    # Log activity only if there are notable issues
    if errors > 0 or (spotify_total is not None and spotify_total > total):
        log_activity(
            db_path, "parity_check", None,
            f"Parity: {synced}/{total} synced ({parity_pct:.1f}%), {errors} errors",
            {
                "total": total,
                "synced": synced,
                "errors": errors,
                "in_progress": in_progress,
                "parity_pct": parity_pct,
                "spotify_total": spotify_total,
            },
        )


async def parity_check(db_path: str):
    """Run parity check (async wrapper)."""
    await asyncio.to_thread(_check, db_path)
