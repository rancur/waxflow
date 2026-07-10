"""Poll Spotify Liked Songs and insert new tracks into the database."""

import asyncio
import logging
from datetime import datetime, timezone

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

    # Full-backfill mode: a one-shot pull of the ENTIRE liked-songs history,
    # ignoring the incremental last_poll cutoff. The normal incremental poll stops
    # at the first track added at/before last_poll (liked songs come back newest-
    # first), so once last_poll is set it never reaches older history — that is why
    # the DB was stuck far below the true liked-songs total. When app_config
    # 'backfill_all_liked' == '1' we paginate every page; already-present tracks are
    # skipped by the spotify_id / ISRC / artist+title dedup checks below, and each
    # missing track is inserted with its REAL spotify_added_at (needed for correct
    # monthly playlists). The flag is auto-cleared once the run completes.
    backfill = get_config(db_path, "backfill_all_liked") == "1"
    effective_cutoff = None if backfill else last_poll

    log.info(
        "Polling Spotify liked songs (mode=%s, last poll: %s)",
        "FULL-BACKFILL" if backfill else "incremental", last_poll or "never",
    )

    new_count = 0
    scanned = 0
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
            scanned += 1
            added_at = item.get("added_at", "")

            # Incremental cutoff: stop at the first track added at/before the last
            # poll (liked songs are newest-first). Disabled during full-backfill so
            # the entire history is walked.
            if effective_cutoff and added_at and added_at <= effective_cutoff:
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

            # Extract metadata (needed for dedup checks below)
            artists = ", ".join(a["name"] for a in track.get("artists", []) if a.get("name"))
            album_info = track.get("album", {})
            album_name = album_info.get("name", "")

            # Get ISRC from external_ids
            external_ids = track.get("external_ids", {})
            isrc = external_ids.get("isrc")

            # Dedup: same ISRC already in DB (different Spotify ID, same recording)
            if isrc:
                with get_db(db_path) as conn:
                    isrc_dup = conn.execute(
                        "SELECT id FROM tracks WHERE isrc = ? AND isrc IS NOT NULL",
                        (isrc,),
                    ).fetchone()
                if isrc_dup:
                    log.info(
                        "Duplicate ISRC %s for %s - %s (existing track %d), skipping",
                        isrc, artists, track.get("name"), isrc_dup[0],
                    )
                    continue

            # Dedup: same artist + title already in DB
            track_name = track.get("name", "")
            if artists and track_name:
                with get_db(db_path) as conn:
                    title_dup = conn.execute(
                        "SELECT id FROM tracks WHERE lower(artist) = lower(?) AND lower(title) = lower(?)",
                        (artists, track_name),
                    ).fetchone()
                if title_dup:
                    log.info(
                        "Duplicate artist+title '%s - %s' (existing track %d), skipping",
                        artists, track_name, title_dup[0],
                    )
                    continue

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
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    set_config(db_path, "last_spotify_poll", now)

    # Clear the one-shot backfill flag once the full history has been walked, so
    # subsequent polls return to fast incremental mode.
    if backfill:
        set_config(db_path, "backfill_all_liked", "0")
        log.info(
            "Full backfill complete: scanned %d liked songs, imported %d new tracks",
            scanned, new_count,
        )
        log_activity(
            db_path, "backfill_complete", None,
            f"Spotify full backfill complete: scanned {scanned}, imported {new_count} new tracks",
        )
    elif new_count > 0:
        log.info("Imported %d new tracks from Spotify (scanned %d)", new_count, scanned)
        log_activity(db_path, "poll_complete", None, f"Spotify poll complete: {new_count} new tracks")
    else:
        log.info("Spotify poll complete: no new tracks (scanned %d)", scanned)


async def poll_spotify(db_path: str):
    """Poll Spotify liked songs (async wrapper)."""
    await asyncio.to_thread(_poll, db_path)
