"""Offline import queue — Phase 3 sleep-tolerance drain/enqueue over ``import_queue``.

WHY
    The only pipeline stage that talks to Lexicon is ``organizing`` (import + playlist
    link + tag). When the Lexicon-host Mac is asleep, those HTTP calls fail and — before
    this — every organizing track was pushed to ``error``. That both loses the natural
    ordering and floods the error state on a routine, self-healing condition (the Mac
    just went to sleep).

WHAT
    When Lexicon is UNAVAILABLE we ENQUEUE each organizing track into the v3
    ``import_queue`` scaffold table (durable, in sync.db) and LEAVE the track parked in
    ``organizing`` (a valid stage — no CHECK-constraint change). Everything that does NOT
    need Lexicon (Spotify poll, match, download to the NAS) keeps running.

    When Lexicon comes BACK we DRAIN the queue in id order, applying each item through the
    NORMAL safe path (``process_pipeline._organize_track`` — which itself already routes
    link-only tracks to the Phase 2 direct-write when that flag is on). Every write on that
    path is idempotent (playlist membership check / INSERT OR IGNORE / diff-guarded comment),
    so a double-drain can never double-apply. Successful items are marked ``done``; failures
    are kept ``pending`` with exponential backoff for retry; a mid-drain loss of Lexicon
    stops the drain cleanly and leaves the rest queued.

SAFETY
    * Non-destructive: only enqueues rows and applies via the existing safe organize path.
      Never deletes a track, file, or playlist.
    * Persistent: ``import_queue`` lives in sync.db, so the queue survives worker restarts.
    * Idempotent enqueue: at most one active (``pending``) row per track.
    * Gated: the whole enqueue/hold/drain behaviour is behind the ``offline_queue_enabled``
      app_config flag (default OFF) so it is inert until the batched deploy flips it on.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from tasks.helpers import get_config, get_db, log_activity, update_track

log = logging.getLogger("worker.offline_queue")

# Retry policy for a queued item that fails to apply while Lexicon IS available
# (e.g. a genuine per-track error). Exponential backoff, capped.
_BACKOFF_BASE_SECONDS = 60
_BACKOFF_CAP_SECONDS = 3600
_MAX_ATTEMPTS = 8  # after this an item is parked in 'error' for inspection

_MONTHS = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def is_offline_queue_enabled(db_path: str) -> bool:
    """Feature flag (default OFF). Keeps the whole hold/drain behaviour inert until
    the batched deploy explicitly enables it."""
    return str(get_config(db_path, "offline_queue_enabled") or "0").lower() in (
        "1", "true", "on", "yes",
    )


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _monthly_playlist_name(added_at: str | None) -> str:
    """Mirror process_pipeline / lexicon_direct_write monthly-playlist naming, for
    context stored on the queue row (drain re-derives everything from the track)."""
    try:
        dt = datetime.fromisoformat((added_at or "").replace("Z", "+00:00"))
    except (ValueError, TypeError):
        dt = _now()
    return f"{dt.month:02d}. {_MONTHS[dt.month]} {dt.year}"


def _op_for(track: dict) -> str:
    """A link-only track (already present in Lexicon) needs only a playlist link +
    tag; anything else needs a real import. Recorded for observability/replay."""
    if track.get("match_source") == "lexicon_existing" and track.get("lexicon_track_id"):
        return "link"
    return "import"


def enqueue_organizing(db_path: str, held_reason: str, limit: int = 500) -> int:
    """Enqueue every ``organizing`` track that has no active queue row yet.

    Idempotent: a track already holding a ``pending`` row is skipped, so repeated
    calls while the Mac stays asleep never create duplicates. Returns the number of
    NEW rows enqueued.
    """
    enqueued = 0
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM tracks
               WHERE pipeline_stage = 'organizing'
               ORDER BY created_at ASC LIMIT ?""",
            (limit,),
        ).fetchall()
        for r in rows:
            track = dict(r)
            tid = track["id"]
            active = conn.execute(
                "SELECT 1 FROM import_queue WHERE track_id = ? AND state = 'pending'",
                (tid,),
            ).fetchone()
            if active:
                continue
            conn.execute(
                """INSERT INTO import_queue
                   (track_id, mac_path, playlist_target, op, state, held_reason)
                   VALUES (?, ?, ?, ?, 'pending', ?)""",
                (
                    tid,
                    track.get("file_path"),
                    _monthly_playlist_name(track.get("spotify_added_at")),
                    _op_for(track),
                    held_reason,
                ),
            )
            enqueued += 1
    if enqueued:
        log.info("offline_queue: enqueued %d organizing track(s) — held (%s)", enqueued, held_reason)
        log_activity(
            db_path, "import_queue_held", None,
            f"Lexicon unavailable ({held_reason}) — {enqueued} track(s) queued for import on wake",
            {"held_reason": held_reason, "enqueued": enqueued},
        )
    return enqueued


def _backoff_seconds(attempts: int) -> int:
    return min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2 ** max(0, attempts - 1)))


def drain(db_path: str, organize_fn, limit: int = 100) -> dict:
    """Drain pending queue items in id order through the safe organize path.

    ``organize_fn(db_path, track_dict)`` must be the normal, idempotent Lexicon
    organize call (process_pipeline._organize_track). Returns a counts dict:
    ``{drained, still_pending, errored, stopped_early}``.

    Ordering + idempotency: items are applied oldest-first; each success marks the
    row ``done``; a transient empty-import (file not yet synced to the Mac) keeps the
    row ``pending`` with backoff; a mid-drain loss of Lexicon stops the pass and
    leaves the remainder queued (``stopped_early``). Never double-applies because the
    underlying organize writes are all idempotent.
    """
    # Late imports to avoid a circular import with process_pipeline.
    from tasks.process_pipeline import LexiconImportEmpty

    now = _now()
    with get_db(db_path) as conn:
        pending = conn.execute(
            """SELECT * FROM import_queue
               WHERE state = 'pending'
                 AND (next_retry_at IS NULL OR next_retry_at <= ?)
               ORDER BY id ASC LIMIT ?""",
            (_iso(now), limit),
        ).fetchall()
        pending = [dict(r) for r in pending]

    drained = errored = still_pending = 0
    stopped_early = False

    for row in pending:
        qid = row["id"]
        tid = row["track_id"]
        with get_db(db_path) as conn:
            trow = conn.execute("SELECT * FROM tracks WHERE id = ?", (tid,)).fetchone()
        track = dict(trow) if trow else None

        # Track gone or already applied elsewhere -> the queue row is satisfied.
        if track is None or track.get("pipeline_stage") == "complete":
            _mark_done(db_path, qid)
            drained += 1
            continue

        try:
            organize_fn(db_path, track)
            _mark_done(db_path, qid)
            drained += 1
        except LexiconImportEmpty as e:
            # File not yet synced to the Mac (transient). Keep pending with backoff;
            # the organize path also keeps the track in 'organizing'. Not an error.
            still_pending += 1
            _bump_retry(db_path, qid, held_reason=f"empty_import: {e}")
        except (ConnectionError, OSError) as e:
            # Lexicon disappeared mid-drain (Mac went back to sleep). Stop cleanly;
            # the rest stay pending for the next wake.
            log.warning("offline_queue: Lexicon lost mid-drain (%s) — stopping drain", e)
            _bump_retry(db_path, qid, held_reason=f"lexicon_lost: {e}")
            stopped_early = True
            break
        except Exception as e:  # noqa: BLE001
            # Per-track error while Lexicon IS up. Retry with backoff; park after cap.
            attempts = int(row.get("attempts") or 0) + 1
            if attempts >= _MAX_ATTEMPTS:
                _mark_error(db_path, qid, str(e))
                errored += 1
                update_track(
                    db_path, tid,
                    pipeline_stage="error",
                    pipeline_error=f"import_queue drain failed after {attempts} attempts: {e}",
                )
                log.error("offline_queue: item %d (track %d) parked in error: %s", qid, tid, e)
            else:
                still_pending += 1
                _bump_retry(db_path, qid, held_reason=str(e))
                log.warning("offline_queue: item %d (track %d) retry %d: %s", qid, tid, attempts, e)

    counts = {
        "drained": drained,
        "still_pending": still_pending,
        "errored": errored,
        "stopped_early": stopped_early,
    }
    if drained or errored:
        log.info("offline_queue: drain complete %s", counts)
        log_activity(
            db_path, "import_queue_drained", None,
            f"Import queue drained: {drained} applied, {still_pending} pending, {errored} errored",
            counts,
        )
    return counts


def queue_counts(db_path: str) -> dict:
    """Heartbeat counts for the health/stats surface: pending / done / error."""
    try:
        with get_db(db_path) as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) c FROM import_queue GROUP BY state"
            ).fetchall()
        return {r["state"]: r["c"] for r in rows}
    except Exception:
        return {}


def _mark_done(db_path: str, qid: int) -> None:
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE import_queue SET state='done', held_reason=NULL WHERE id=?", (qid,)
        )


def _mark_error(db_path: str, qid: int, reason: str) -> None:
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE import_queue SET state='error', held_reason=? WHERE id=?",
            (reason[:500], qid),
        )


def _bump_retry(db_path: str, qid: int, held_reason: str) -> None:
    with get_db(db_path) as conn:
        row = conn.execute("SELECT attempts FROM import_queue WHERE id=?", (qid,)).fetchone()
        attempts = int(row["attempts"] or 0) + 1 if row else 1
        next_at = _iso(_now() + timedelta(seconds=_backoff_seconds(attempts)))
        conn.execute(
            "UPDATE import_queue SET attempts=?, next_retry_at=?, held_reason=? WHERE id=?",
            (attempts, next_at, held_reason[:500], qid),
        )
