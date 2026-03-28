"""Poll Spotify Liked Songs and insert new tracks into the database."""

import asyncio
import logging
from datetime import datetime

from tasks.helpers import (
    get_config,
    get_db,
    get_spotify_client,
    log_activity,
    set_config,
)

log = logging.getLogger("worker.poll_spotify")


def _poll(db_path: str):
    """Synchronous poll implementation (runs in thread)."""
    sp = get_spotify_client(db_path)
    if sp is None:
        log.warning("No Spotify tokens configured. Skipping poll. Complete OAuth in the web UI.")
        return

    last_poll = get_config(db_path, "last_spotify_poll")
    log.info("Polling Spotify liked songs (last poll: %s)", last_poll or "never")

    new_count = 0
    offset = 0
    limit = 50
    done = False

    while not done:
        try:
            results = sp.current_user_saved_tracks(limit=limit, offset=offset)
        except Exception as e:
            log.error("Spotify API error at offset %d: %s", offset, e)
            # If it's an auth error, try to note it
            if "401" in str(e) or "token" in str(e).lower():
                log.error("Spotify auth may be expired. Re-authenticate via the web UI.")
            break

        items = results.get("items", [])
        if not items:
            break

        for item in items:
            added_at = item.get("added_at", "")

            # If we have a last_poll timestamp, skip tracks added before it
            if last_poll and added_at and added_at <= last_poll:
                done = True
                break

            track = item.get("track")
            if not track:
                continue

            spotify_id = track.get("id")
            if not spotify_id:
                continue

            # Check if track already exists
            with get_db(db_path) as conn:
                existing = conn.execute(
                    "SELECT id FROM tracks WHERE spotify_id = ?", (spotify_id,)
                ).fetchone()

            if existing:
                continue

            # Extract metadata
            artists = ", ".join(a["name"] for a in track.get("artists", []) if a.get("name"))
            album_info = track.get("album", {})
            album_name = album_info.get("name", "")

            # Get ISRC from external_ids
            external_ids = track.get("external_ids", {})
            isrc = external_ids.get("isrc")

            # Insert new track
            with get_db(db_path) as conn:
                conn.execute(
                    """INSERT INTO tracks
                    (spotify_id, spotify_uri, spotify_added_at, title, artist, album,
                     duration_ms, isrc, spotify_popularity, pipeline_stage, match_status,
                     download_status, verify_status, lexicon_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', 'pending', 'pending', 'pending', 'pending')""",
                    (
                        spotify_id,
                        track.get("uri"),
                        added_at,
                        track.get("name"),
                        artists,
                        album_name,
                        track.get("duration_ms"),
                        isrc,
                        track.get("popularity"),
                    ),
                )
                track_db_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            log_activity(
                db_path,
                "spotify_import",
                track_db_id,
                f"Imported from Spotify: {artists} - {track.get('name')}",
                {"spotify_id": spotify_id, "isrc": isrc, "added_at": added_at},
            )
            new_count += 1

        offset += limit

        # Spotify returns total, check if we've gone past it
        total = results.get("total", 0)
        if offset >= total:
            break

    # Update last poll timestamp
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    set_config(db_path, "last_spotify_poll", now)

    if new_count > 0:
        log.info("Imported %d new tracks from Spotify", new_count)
        log_activity(db_path, "poll_complete", None, f"Spotify poll complete: {new_count} new tracks")
    else:
        log.info("Spotify poll complete: no new tracks")


async def poll_spotify(db_path: str):
    """Poll Spotify liked songs (async wrapper)."""
    await asyncio.to_thread(_poll, db_path)
