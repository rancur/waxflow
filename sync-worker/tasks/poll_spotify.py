"""Poll Spotify Liked Songs and insert new tracks into the database."""

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone

from tasks.helpers import (
    get_config,
    get_db,
    get_spotify_client,
    log_activity,
    set_config,
)
from tasks.nonmusic_filter import DEFAULT_MAX_DURATION_MS, is_nonmusic

log = logging.getLogger("worker.poll_spotify")

# How many times to retry a write that hits SQLite "database is locked" before
# giving up. During a full backfill the poll issues a burst of INSERTs while the
# pipeline is concurrently writing (downloads/verifies/organizes), so the shared
# sync.db can be write-locked longer than the base busy_timeout. Without this the
# poll task would crash mid-walk on a transient lock and the whole backfill would
# abort (and, before resumability below, restart from offset 0). We retry with a
# short escalating backoff so a bulk backfill grinds through under load instead.
_LOCK_RETRY_ATTEMPTS = 12
_LOCK_RETRY_BASE_DELAY = 0.25


def _with_db_retry(db_path: str, func):
    """Run func(conn) inside a get_db transaction, retrying on 'database is locked'.

    Only OperationalError whose message mentions a lock is retried; every other
    error propagates immediately. Returns whatever func returns.
    """
    last_exc: Exception | None = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            with get_db(db_path) as conn:
                return func(conn)
        except sqlite3.OperationalError as e:
            last_exc = e
            if "locked" in str(e).lower() and attempt < _LOCK_RETRY_ATTEMPTS - 1:
                time.sleep(_LOCK_RETRY_BASE_DELAY * (attempt + 1))
                continue
            raise
    if last_exc:
        raise last_exc


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

    new_count = 0
    scanned = 0
    limit = 50
    done = False
    # True only when the walk reaches the natural end of the library. A backfill
    # that exits early on a Spotify API error must NOT clear the one-shot flag —
    # it needs to resume next cycle from the persisted offset.
    walk_complete = False

    # Resumable backfill: the worker can be restarted (redeploys, crashes, or a
    # transient lock that still slips past the retry) mid-walk. Rather than
    # restart the whole ~5.5k-track walk from offset 0 every time — which wastes
    # API calls re-scanning already-present pages and, under load, may never
    # reach the deep (older) pages before the next restart — we persist the page
    # offset in app_config and resume from it. Dedup still protects against any
    # double-processing. The cursor is cleared on clean completion. Incremental
    # polls always start at 0 (they short-circuit at the last-poll cutoff fast).
    offset = 0
    if backfill:
        try:
            offset = int(get_config(db_path, "backfill_offset") or 0)
        except (TypeError, ValueError):
            offset = 0

    log.info(
        "Polling Spotify liked songs (mode=%s, last poll: %s%s)",
        "FULL-BACKFILL" if backfill else "incremental", last_poll or "never",
        f", resuming at offset {offset}" if backfill and offset else "",
    )

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
            walk_complete = True
            break

        for item in items:
            scanned += 1
            added_at = item.get("added_at", "")

            # Incremental cutoff: stop at the first track added at/before the last
            # poll (liked songs are newest-first). Disabled during full-backfill so
            # the entire history is walked.
            if effective_cutoff and added_at and added_at <= effective_cutoff:
                done = True
                walk_complete = True
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

            # Non-music filter: keep audiobooks / podcast episodes / spoken-word
            # out of the DJ library entirely (they must never enter the pipeline,
            # and — now that Lexicon watch-folder auto-import is active — never get
            # downloaded into the watch folder). Configurable + auditable.
            if (get_config(db_path, "nonmusic_filter_enabled") or "1") != "0":
                try:
                    max_dur = int(get_config(db_path, "nonmusic_max_duration_ms") or DEFAULT_MAX_DURATION_MS)
                except (TypeError, ValueError):
                    max_dur = DEFAULT_MAX_DURATION_MS
                skip, reason = is_nonmusic(
                    {
                        "type": track.get("type"),
                        "episode": track.get("episode"),
                        "duration_ms": track.get("duration_ms"),
                        "title": track.get("name"),
                        "album": album_name,
                        "artists": artists,
                    },
                    max_duration_ms=max_dur,
                )
                if skip:
                    log.info("Skipping non-music item '%s - %s' (%s)", artists, track.get("name"), reason)
                    log_activity(
                        db_path, "nonmusic_skipped", None,
                        f"Skipped non-music item: {artists} - {track.get('name')} ({reason})",
                        {"spotify_id": spotify_id, "reason": reason,
                         "duration_ms": track.get("duration_ms"), "type": track.get("type")},
                    )
                    continue

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

            # Insert new track (lock-resilient: a bulk backfill races the pipeline
            # for the shared sync.db, so retry transient "database is locked").
            def _insert(conn, _added=added_at, _sid=spotify_id, _track=track,
                        _artists=artists, _album=album_name, _isrc=isrc):
                conn.execute(
                    """INSERT INTO tracks
                    (spotify_id, spotify_uri, spotify_added_at, title, artist, album,
                     duration_ms, isrc, spotify_popularity, pipeline_stage, match_status,
                     download_status, verify_status, lexicon_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', 'pending', 'pending', 'pending', 'pending')""",
                    (
                        _sid,
                        _track.get("uri"),
                        _added,
                        _track.get("name"),
                        _artists,
                        _album,
                        _track.get("duration_ms"),
                        _isrc,
                        _track.get("popularity"),
                    ),
                )
                return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            track_db_id = _with_db_retry(db_path, _insert)

            log_activity(
                db_path,
                "spotify_import",
                track_db_id,
                f"Imported from Spotify: {artists} - {track.get('name')}",
                {"spotify_id": spotify_id, "isrc": isrc, "added_at": added_at},
            )
            new_count += 1

        offset += limit

        # Persist the resumable cursor after each fully-processed page so a
        # restart mid-backfill continues from here instead of from 0.
        if backfill:
            try:
                set_config(db_path, "backfill_offset", str(offset))
            except sqlite3.OperationalError:
                pass  # best-effort; a missed checkpoint only costs a re-scan

        # Spotify returns total, check if we've gone past it
        total = results.get("total", 0)
        if offset >= total:
            walk_complete = True
            break

    # Update last poll timestamp
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    set_config(db_path, "last_spotify_poll", now)

    # Clear the one-shot backfill flag ONLY when the full history was actually
    # walked to the end. If the walk exited early (e.g. a Spotify API error),
    # leave the flag set and the offset persisted so the next poll resumes and
    # finishes the backfill instead of silently dropping to incremental with a
    # partial library.
    if backfill and walk_complete:
        set_config(db_path, "backfill_all_liked", "0")
        # Clear the resumable cursor so the next explicit backfill starts fresh.
        set_config(db_path, "backfill_offset", "0")
        log.info(
            "Full backfill complete: scanned %d liked songs, imported %d new tracks",
            scanned, new_count,
        )
        log_activity(
            db_path, "backfill_complete", None,
            f"Spotify full backfill complete: scanned {scanned}, imported {new_count} new tracks",
        )
    elif backfill and not walk_complete:
        log.warning(
            "Backfill did not reach end (exited at offset %d); flag left set to resume "
            "next cycle. Scanned %d, imported %d so far.",
            offset, scanned, new_count,
        )
    elif new_count > 0:
        log.info("Imported %d new tracks from Spotify (scanned %d)", new_count, scanned)
        log_activity(db_path, "poll_complete", None, f"Spotify poll complete: {new_count} new tracks")
    else:
        log.info("Spotify poll complete: no new tracks (scanned %d)", scanned)


async def poll_spotify(db_path: str):
    """Poll Spotify liked songs (async wrapper)."""
    await asyncio.to_thread(_poll, db_path)
