"""Missing-track HUNTER (Phase 4, WaxFlow v3 Feature #2).

A Radarr/Sonarr-style background loop that keeps trying to source tracks WaxFlow
could not acquire losslessly on the first pass. It reuses the v3 scaffold end to
end — the ``wanted`` ledger, the ``source_attempts`` backoff log (via
``SourceBackoff``), the source-plugin ``registry``, and the ``purchase_links``
table — rather than inventing anything new.

Flow per run (all idempotent + restart-safe; state lives entirely in SQLite):

  1. reconcile  — any ``wanted`` item whose track has since reached
                  ``pipeline_stage='complete'`` is marked ``resolved``.
  2. enqueue    — tracks parked at ``pipeline_stage='error'`` (couldn't be sourced)
                  that are not already tracked get a ``wanted`` row.
  3. re-attempt — for each READY wanted item (item-level exponential backoff via
                  ``wanted.next_retry_at``), search every enabled ACQUIRE source
                  (Tidal, Soulseek, + future plugins). On a hit the track is
                  re-armed (``pipeline_stage='new'``) so the NORMAL pipeline does
                  the real download -> verify -> organize/import, and the wanted
                  item moves to ``sourcing`` (then ``resolved`` once complete). On a
                  miss the item backs off and buy-links are (re)generated so the
                  user always has a purchase path.
  4. exhaust    — after ``hunter_max_attempts`` misses the item becomes
                  ``exhausted`` (buy-links remain the answer). Never deleted.
  5. heartbeat  — fail-loud counts (wanted / resolved-this-run / still-wanting)
                  to the activity log, a heartbeat file, and app_config.

HARD CONSTRAINT: the hunter NEVER purchases, never enters payment info, never
creates accounts. Buy-link generation produces store search URLs only. Acquisition
happens solely through existing ACQUIRE sources (Tidal/Soulseek) whose behavior is
unchanged. Behind the ``hunter_enabled`` flag (default OFF); the batched deploy
flips it on.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from tasks.helpers import get_config, get_db, log_activity, set_config
from tasks.sources import registry
from tasks.sources.base import SourceBackoff, TrackQuery
from tasks.sources.linkbuild import dedup_key
from tasks.v3_schema import ensure_v3_schema

log = logging.getLogger("worker.hunter")

# Defaults (all overridable via app_config).
DEFAULT_MAX_ATTEMPTS = 8      # cap so a truly-unfindable track stops hammering
DEFAULT_BATCH_SIZE = 25       # gentle per-run cap on re-attempts
# Stages that mean "we tried and could not source this" -> enqueue as wanted.
UNSOURCED_STAGES = ("error",)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bool_config(db_path: str, key: str, default: bool) -> bool:
    val = get_config(db_path, key)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _int_config(db_path: str, key: str, default: int) -> int:
    val = get_config(db_path, key)
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _heartbeat_path(db_path: str) -> str:
    return os.path.join(os.path.dirname(db_path) or ".", ".hunter_heartbeat")


# --------------------------------------------------------------------------- #
# Step 1 — reconcile resolved.
# --------------------------------------------------------------------------- #
def reconcile_resolved(db_path: str) -> int:
    """Mark wanted items resolved once their track reaches ``complete``."""
    with get_db(db_path) as conn:
        cur = conn.execute(
            """UPDATE wanted
                  SET state = 'resolved', updated_at = datetime('now')
                WHERE state IN ('wanted', 'sourcing')
                  AND track_id IN (
                      SELECT id FROM tracks WHERE pipeline_stage = 'complete'
                  )"""
        )
        return cur.rowcount or 0


# --------------------------------------------------------------------------- #
# Step 2 — enqueue unsourced tracks.
# --------------------------------------------------------------------------- #
def enqueue_from_failures(db_path: str) -> int:
    """Create wanted rows for unsourced (error-stage) tracks not already tracked.

    Idempotent: a track that already has ANY wanted row is skipped, so re-running
    never duplicates.
    """
    placeholders = ",".join("?" for _ in UNSOURCED_STAGES)
    with get_db(db_path) as conn:
        cur = conn.execute(
            f"""INSERT INTO wanted (track_id, state, reason)
                SELECT t.id, 'wanted', 'unsourced:' || t.pipeline_stage
                  FROM tracks t
                 WHERE t.pipeline_stage IN ({placeholders})
                   AND t.id NOT IN (
                       SELECT track_id FROM wanted WHERE track_id IS NOT NULL
                   )""",
            UNSOURCED_STAGES,
        )
        return cur.rowcount or 0


# --------------------------------------------------------------------------- #
# Buy-links (Feature #1) — generated for every wanted track that misses.
# --------------------------------------------------------------------------- #
def generate_buy_links(db_path: str, track_id: int, q: TrackQuery) -> int:
    """(Re)generate buy-links for a track into ``purchase_links``, deduped.

    Iterates enabled SEARCH_LINK sources (Beatport/Qobuz/Bandcamp) and upserts one
    active row per (source, track) keyed by ``dedup_key`` — so repeat generations
    refresh rather than spam. Returns how many links were written/refreshed. NEVER
    purchases; these are search URLs only.
    """
    written = 0
    with get_db(db_path) as conn:
        for src in registry.enabled_link_sources(db_path):
            try:
                res = src.purchase_link(q)
            except Exception as e:  # noqa: BLE001 — one bad plugin must not break the rest
                log.warning("buy-link gen failed for %s: %s", src.name, e)
                continue
            if not res or not res.url:
                continue
            conn.execute(
                """INSERT INTO purchase_links
                       (track_id, source, url, format_hint, price, confidence,
                        dedup_key, status, first_generated_at, last_refreshed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now'), datetime('now'))
                   ON CONFLICT(dedup_key) DO UPDATE SET
                       url = excluded.url,
                       format_hint = excluded.format_hint,
                       price = excluded.price,
                       confidence = excluded.confidence,
                       status = 'active',
                       last_refreshed_at = datetime('now')""",
                (
                    track_id, src.name, res.url, res.format_hint, res.price,
                    res.confidence, dedup_key(src.name, q),
                ),
            )
            written += 1
    return written


# --------------------------------------------------------------------------- #
# Step 3/4 — re-attempt ready wanted items.
# --------------------------------------------------------------------------- #
def _load_track(conn, track_id: int) -> dict | None:
    row = conn.execute(
        """SELECT id, spotify_id, title, artist, album, isrc, duration_ms,
                  pipeline_stage
             FROM tracks WHERE id = ?""",
        (track_id,),
    ).fetchone()
    return dict(row) if row else None


def _rearm_track(conn, track_id: int) -> None:
    """Hand a found track back to the NORMAL pipeline for download/verify/import.

    Mirrors retry_unmatched's reset: pipeline_stage='new' + match reset. The pipeline
    (with its lossless gate + existing import/organize path) does the real work — the
    hunter never downloads or imports itself.
    """
    conn.execute(
        """UPDATE tracks
              SET pipeline_stage = 'new',
                  match_status = 'pending',
                  pipeline_error = NULL,
                  updated_at = datetime('now')
            WHERE id = ?""",
        (track_id,),
    )


def process_wanted(db_path: str, *, max_attempts: int, batch_size: int) -> dict:
    """Re-attempt READY wanted items across enabled ACQUIRE sources.

    Returns counts: {attempted, sourced, still_wanting, exhausted, links_written}.
    """
    counts = {"attempted": 0, "sourced": 0, "still_wanting": 0, "exhausted": 0, "links_written": 0}
    now_iso = _now().isoformat()

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, track_id, attempts
                 FROM wanted
                WHERE state = 'wanted'
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY (next_retry_at IS NULL) DESC, next_retry_at ASC
                LIMIT ?""",
            (now_iso, batch_size),
        ).fetchall()
        ready = [dict(r) for r in rows]

    for item in ready:
        wanted_id = item["id"]
        track_id = item["track_id"]
        prior_attempts = item["attempts"] or 0
        counts["attempted"] += 1

        with get_db(db_path) as conn:
            track = _load_track(conn, track_id)
        if not track:
            # Orphaned wanted row (track vanished) — retire it, never crash.
            with get_db(db_path) as conn:
                conn.execute(
                    "UPDATE wanted SET state='exhausted', reason='orphaned', "
                    "updated_at=datetime('now') WHERE id=?",
                    (wanted_id,),
                )
            continue

        q = TrackQuery.from_row(track)
        hit_source = None

        for src in registry.enabled_acquire_sources(db_path):
            try:
                results = src.search(db_path, q)
            except Exception as e:  # noqa: BLE001 — a flaky source must not stall the hunter
                SourceBackoff.record(db_path, track_id, src.name, "error", error=str(e)[:500])
                continue
            if results:
                SourceBackoff.record(
                    db_path, track_id, src.name, "found",
                    search_query=f"{q.artist} {q.title}".strip(),
                    result_count=len(results),
                )
                hit_source = src.name
                break
            SourceBackoff.record(
                db_path, track_id, src.name, "no_match",
                search_query=f"{q.artist} {q.title}".strip(), result_count=0,
            )

        if hit_source:
            with get_db(db_path) as conn:
                _rearm_track(conn, track_id)
                conn.execute(
                    """UPDATE wanted
                          SET state='sourcing', last_source=?, attempts=?,
                              last_attempt_at=datetime('now'),
                              best_result_json=?, next_retry_at=NULL,
                              updated_at=datetime('now')
                        WHERE id=?""",
                    (hit_source, prior_attempts + 1,
                     json.dumps({"source": hit_source}), wanted_id),
                )
            log_activity(db_path, "hunter_sourced", track_id,
                         f"Hunter re-sourced via {hit_source}: {q.artist} - {q.title}")
            counts["sourced"] += 1
            continue

        # Miss: back off, (re)generate buy-links so the user always has a path.
        new_attempts = prior_attempts + 1
        counts["links_written"] += generate_buy_links(db_path, track_id, q)
        if new_attempts >= max_attempts:
            with get_db(db_path) as conn:
                conn.execute(
                    """UPDATE wanted
                          SET state='exhausted', attempts=?, last_attempt_at=datetime('now'),
                              next_retry_at=NULL, updated_at=datetime('now')
                        WHERE id=?""",
                    (new_attempts, wanted_id),
                )
            counts["exhausted"] += 1
        else:
            backoff = SourceBackoff.delay_for(new_attempts)
            next_retry = _now().timestamp() + backoff
            next_iso = datetime.fromtimestamp(next_retry, tz=timezone.utc).isoformat()
            with get_db(db_path) as conn:
                conn.execute(
                    """UPDATE wanted
                          SET attempts=?, next_retry_at=?, last_attempt_at=datetime('now'),
                              updated_at=datetime('now')
                        WHERE id=?""",
                    (new_attempts, next_iso, wanted_id),
                )
            counts["still_wanting"] += 1

    return counts


# --------------------------------------------------------------------------- #
# Heartbeat + entrypoint.
# --------------------------------------------------------------------------- #
def _wanted_totals(db_path: str) -> dict:
    with get_db(db_path) as conn:
        rows = conn.execute(
            "SELECT state, COUNT(*) c FROM wanted GROUP BY state"
        ).fetchall()
    by_state = {r["state"]: r["c"] for r in rows}
    by_state["total"] = sum(by_state.values())
    return by_state


def _write_heartbeat(db_path: str, summary: dict) -> None:
    ts = _now().isoformat()
    try:
        with open(_heartbeat_path(db_path), "w") as f:
            f.write(str(_now().timestamp()))
    except Exception as e:  # noqa: BLE001 — heartbeat is best-effort
        log.warning("hunter heartbeat file write failed: %s", e)
    try:
        set_config(db_path, "hunter_last_run", ts)
        set_config(db_path, "hunter_last_summary", json.dumps(summary))
    except Exception as e:  # noqa: BLE001
        log.warning("hunter heartbeat config write failed: %s", e)


def run_hunter(db_path: str) -> dict:
    """One hunter pass. Sync (call via asyncio.to_thread from the worker loop).

    No-op (returns ``{"status": "disabled"}``) unless ``hunter_enabled`` is set, so
    it ships behind a flag and the batched deploy activates it. Never raises to the
    caller — fail-loud via logs + heartbeat, but the worker loop keeps running.
    """
    ensure_v3_schema(db_path)

    if not _bool_config(db_path, "hunter_enabled", default=False):
        return {"status": "disabled"}

    max_attempts = _int_config(db_path, "hunter_max_attempts", DEFAULT_MAX_ATTEMPTS)
    batch_size = _int_config(db_path, "hunter_batch_size", DEFAULT_BATCH_SIZE)

    resolved = reconcile_resolved(db_path)
    enqueued = enqueue_from_failures(db_path)
    counts = process_wanted(db_path, max_attempts=max_attempts, batch_size=batch_size)

    totals = _wanted_totals(db_path)
    summary = {
        "status": "ok",
        "resolved_reconciled": resolved,
        "enqueued": enqueued,
        **counts,
        "wanted_total": totals.get("total", 0),
        "still_wanting_total": totals.get("wanted", 0),
    }
    _write_heartbeat(db_path, summary)
    log.info(
        "hunter: enqueued=%d attempted=%d sourced=%d still_wanting=%d exhausted=%d "
        "links=%d | wanted_total=%d",
        enqueued, counts["attempted"], counts["sourced"], counts["still_wanting"],
        counts["exhausted"], counts["links_written"], totals.get("total", 0),
    )
    log_activity(
        db_path, "hunter_run", None,
        f"Hunter pass: {counts['sourced']} sourced, {counts['still_wanting']} still "
        f"wanting, {counts['exhausted']} exhausted ({totals.get('total', 0)} wanted total)",
        details=summary,
    )
    return summary


async def hunter(db_path: str) -> None:
    """Async wrapper for the worker task loop; runs the sync pass off the event loop."""
    import asyncio
    try:
        await asyncio.to_thread(run_hunter, db_path)
    except Exception as e:  # noqa: BLE001 — fail-loud, never kill the loop
        log.error("hunter pass crashed: %s", e, exc_info=True)
