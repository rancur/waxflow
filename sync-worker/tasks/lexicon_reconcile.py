"""Lexicon deletion reconciler — turn "deleted in Lexicon" into a durable tombstone.

WHY THIS EXISTS (2026-09-01)
    Deleting a track inside Lexicon is invisible to WaxFlow. The WaxFlow row stays
    ``complete``, still pointing at a ``Track.id`` Lexicon no longer has. Nothing
    re-downloads it *immediately* — but every re-arm path (Retry, Bulk Retry, the
    hunter, the catch-up pass, ``recheck-mappings --apply``, unignore) hands it back
    to the pipeline, which cheerfully re-downloads and re-imports the very match the
    user just rejected. The quality rechecker keeps "upgrading" it. And ``file_index``
    keeps offering the NAS master to the next like of that song.

    Worse, on this install Lexicon is set to delete-from-disk, so the Mac copy is
    gone — but the NAS master survives (the NAS->Mac rsync is pull-only, no
    ``--delete``), and the 6-hourly reconcile copies the deleted file straight back
    onto the Mac. Measured on the live library: 34 complete tracks pointed at missing
    Lexicon rows; 27 still had a NAS master waiting to be resurrected.

WHAT IT DOES
    Runs only when the Mac/Lexicon is AVAILABLE (an unreachable Lexicon must never
    be mistaken for an emptied library). Pages the whole Lexicon library
    (``GET /v1/tracks``, 1000/page, against the reported total) into an id set. Every
    ``complete`` track whose ``lexicon_track_id`` is missing from that set — or whose
    Lexicon row is archived — is TOMBSTONED:

      * ``tracks``: pipeline_stage='ignored', is_protected=1, lexicon_status='skipped',
        pipeline_error='deleted_in_lexicon:<iso ts>'
      * a ``tombstones`` row keyed by everything a future re-match could collide on:
        spotify_id, isrc, tidal_id, lexicon_track_id, file_path, file hash
      * the NAS master is moved into the share's Synology Recycle Bin
        (``/music/#recycle/<relative path>``) and its ``file_index`` row removed, so
        neither WaxFlow nor the Mac sync can bring it back. WaxFlow-trashed files are
        purged after ``tombstone_purge_days`` (default 30); files WaxFlow did not
        trash are never touched.

    A tombstone is terminal: the retry/unignore/reject/bulk-retry endpoints and every
    worker re-arm path refuse it. The only way back is the explicit Restore endpoint
    (``restore_tombstone`` below), which also un-trashes the file if it is still there.

SAFETY
    * Never writes to Lexicon. Never deletes a file it did not itself trash.
    * A page failure mid-listing ABORTS the pass with zero writes — a partial id set
      would tombstone half the library.
    * A ``lexicon_existing`` track (a pre-existing library track WaxFlow only LINKED,
      never downloaded) is tombstoned but its file is left alone: that file was never
      WaxFlow's to move.
    * Bounded per pass (``tombstone_batch``, default 200) so a catastrophic Lexicon
      state (empty DB restore) cannot wipe the index in one cycle; the activity log
      shows every batch.
    * Sanity fuse: if the live Lexicon library is empty or smaller than
      ``lexicon_reconcile_min_library`` (default 100) the pass refuses to run.

CONFIG (read live from app_config)
    lexicon_reconcile_enabled            default 1
    lexicon_reconcile_interval_seconds   default 900
    lexicon_reconcile_min_library        default 100
    tombstone_trash_enabled              default 1
    tombstone_purge_days                 default 30
    tombstone_batch                      default 200
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

import httpx

from tasks.helpers import (
    LEXICON_API_URL,
    MUSIC_LIBRARY_PATH,
    get_config,
    get_db,
    log_activity,
    update_track,
)

log = logging.getLogger("worker.lexicon_reconcile")

REASON_DELETED = "deleted_in_lexicon"
REASON_ARCHIVED = "archived_in_lexicon"
TOMBSTONE_ERROR_PREFIX = "deleted_in_lexicon:"

# The share's Synology Recycle Bin, as seen from the worker container. The Mac-side
# rsync already excludes '#recycle', so anything moved here can never replicate back.
RECYCLE_DIRNAME = "#recycle"

_PAGE_SIZE = 1000
_MAX_PAGES = 100  # 100k tracks — well beyond any DJ library

DEFAULT_INTERVAL = 900
DEFAULT_MIN_LIBRARY = 100
DEFAULT_PURGE_DAYS = 30
DEFAULT_BATCH = 200


# ----------------------------------------------------------------------------- utils
def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _flag(db_path: str, key: str, default: bool) -> bool:
    raw = get_config(db_path, key)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _int(db_path: str, key: str, default: int) -> int:
    raw = get_config(db_path, key)
    try:
        return int(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        return default


def is_enabled(db_path: str) -> bool:
    return _flag(db_path, "lexicon_reconcile_enabled", True)


def _music_root() -> str:
    """The bind-mounted share root the recycle bin lives in. MUSIC_LIBRARY_PATH may
    point at a SUBDIRECTORY of the share (e.g. /music/Database, see 2.11.0), and the
    recycle bin is at the share root, so walk up to the mount."""
    root = os.environ.get("MUSIC_SHARE_ROOT")
    if root:
        return root.rstrip("/") or "/"
    lib = (MUSIC_LIBRARY_PATH or "/music").rstrip("/")
    # /music/Database -> /music ; /music -> /music
    parts = lib.split("/")
    return "/".join(parts[:2]) if len(parts) > 2 else lib


# ------------------------------------------------------------------- lexicon listing
class LexiconListingIncomplete(Exception):
    """Raised when the library could not be listed in full. The caller MUST treat this
    as 'no information' — never as 'everything is deleted'."""


def fetch_lexicon_ids(api_url: str, timeout: float = 120.0) -> tuple[set[int], set[int]]:
    """Return (all_ids, archived_ids) for the whole Lexicon library.

    Pages ``GET /v1/tracks`` like ``lexicon_coverage.fetch_tracks`` does (the API caps
    at 1000 rows and rejects larger limits). Any failure raises
    LexiconListingIncomplete so a partial listing can never be acted on.
    """
    ids: set[int] = set()
    archived: set[int] = set()
    with httpx.Client(base_url=api_url, timeout=timeout) as client:
        total = None
        for page in range(_MAX_PAGES):
            try:
                resp = client.get(
                    "/v1/tracks",
                    params={"limit": _PAGE_SIZE, "offset": page * _PAGE_SIZE},
                )
            except Exception as e:  # noqa: BLE001
                raise LexiconListingIncomplete(f"page {page}: {e}") from e
            if resp.status_code != 200:
                raise LexiconListingIncomplete(f"page {page}: HTTP {resp.status_code}")
            try:
                data = resp.json().get("data") or {}
            except Exception as e:  # noqa: BLE001
                raise LexiconListingIncomplete(f"page {page}: bad JSON ({e})") from e
            batch = data.get("tracks") or []
            for t in batch:
                tid = t.get("id")
                if tid is None:
                    continue
                try:
                    tid = int(tid)
                except (TypeError, ValueError):
                    continue
                ids.add(tid)
                if t.get("archived") in (1, True, "1"):
                    archived.add(tid)
            total = data.get("total", total)
            if len(batch) < _PAGE_SIZE or (total is not None and len(ids) >= int(total)):
                break
        else:
            raise LexiconListingIncomplete(f"more than {_MAX_PAGES} pages")
        if total is not None and len(ids) < int(total):
            raise LexiconListingIncomplete(f"listed {len(ids)} of reported {total}")
    return ids, archived


# ---------------------------------------------------------------------- tombstones
def is_tombstoned(db_path: str, track_id: int) -> bool:
    """A track is tombstoned when it is parked in 'ignored' AND carries a live
    (un-restored) tombstones row. Both halves matter: a plain user 'ignore' has no
    tombstone, and a restored tombstone keeps its row for history."""
    with get_db(db_path) as conn:
        row = conn.execute(
            """SELECT 1 FROM tracks t
                JOIN tombstones tb ON tb.track_id = t.id AND tb.restored_at IS NULL
               WHERE t.id = ? AND t.pipeline_stage = 'ignored' LIMIT 1""",
            (track_id,),
        ).fetchone()
    return row is not None


def find_tombstone_collision(
    db_path: str,
    *,
    tidal_id: str | None = None,
    isrc: str | None = None,
    file_path: str | None = None,
    file_hash: str | None = None,
    exclude_track_id: int | None = None,
) -> dict | None:
    """Return the live tombstone a candidate match collides with, or None.

    Used at match-acceptance time: a candidate that names the same Tidal track, ISRC
    or file as something the user deliberately deleted from Lexicon must not be
    auto-imported again — it is routed to review instead.
    """
    clauses, params = [], []
    if tidal_id:
        clauses.append("tidal_id = ?"); params.append(str(tidal_id))
    if isrc:
        clauses.append("upper(isrc) = upper(?)"); params.append(isrc)
    if file_path:
        clauses.append("file_path = ?"); params.append(file_path)
    if file_hash:
        clauses.append("file_hash_sha256 = ?"); params.append(file_hash)
    if not clauses:
        return None
    sql = f"SELECT * FROM tombstones WHERE restored_at IS NULL AND ({' OR '.join(clauses)})"
    if exclude_track_id is not None:
        sql += " AND track_id != ?"; params.append(exclude_track_id)
    sql += " ORDER BY id DESC LIMIT 1"
    with get_db(db_path) as conn:
        row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _trash_path_for(file_path: str) -> str | None:
    """Map a library file to its slot in the share's recycle bin, preserving the
    relative path (so a restore can put it back exactly). None if the file is not
    under the share root."""
    root = _music_root()
    if not file_path or not file_path.startswith(root.rstrip("/") + "/"):
        return None
    rel = file_path[len(root.rstrip("/")) + 1:]
    if rel.startswith(RECYCLE_DIRNAME + "/"):
        return None  # already in the bin
    return os.path.join(root, RECYCLE_DIRNAME, rel)


def _trash_file(file_path: str) -> str | None:
    """Move the NAS master into the recycle bin. Returns the new path, or None if
    there was nothing to move / it could not be moved (logged, never raised)."""
    if not file_path or not os.path.isfile(file_path):
        return None
    dest = _trash_path_for(file_path)
    if not dest:
        log.warning("tombstone: %s is outside the share root — leaving in place", file_path)
        return None
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if os.path.exists(dest):
            stem, ext = os.path.splitext(dest)
            dest = f"{stem}.{int(_now().timestamp())}{ext}"
        shutil.move(file_path, dest)
        return dest
    except OSError as e:
        log.warning("tombstone: could not trash %s: %s", file_path, e)
        return None


def _drop_file_index(conn, file_path: str | None) -> None:
    if not file_path:
        return
    try:
        conn.execute("DELETE FROM file_index WHERE file_path = ?", (file_path,))
    except Exception:  # file_index may not exist yet
        pass


def tombstone_track(db_path: str, track: dict, reason: str, *, trash: bool) -> dict:
    """Tombstone one track. Idempotent: a track with a live tombstone is left alone."""
    tid = track["id"]
    if is_tombstoned(db_path, tid):
        return {"track_id": tid, "skipped": "already_tombstoned"}

    file_path = track.get("file_path")
    # A linked pre-existing library track was never WaxFlow's file to move.
    owns_file = (track.get("match_source") or "") != "lexicon_existing" and \
                (track.get("download_source") or "") != "lexicon_existing"
    trashed = _trash_file(file_path) if (trash and owns_file) else None

    now = _now()
    purge_after = _iso(now + timedelta(days=DEFAULT_PURGE_DAYS)) if trashed else None
    with get_db(db_path) as conn:
        conn.execute(
            """INSERT INTO tombstones
               (track_id, spotify_id, isrc, tidal_id, lexicon_track_id, file_path,
                file_hash_sha256, reason, trashed_path, purge_after)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tid, track.get("spotify_id"), track.get("isrc"), track.get("tidal_id"),
                str(track.get("lexicon_track_id") or "") or None, file_path,
                track.get("file_hash_sha256"), reason, trashed, purge_after,
            ),
        )
        if owns_file:
            _drop_file_index(conn, file_path)
    update_track(
        db_path, tid,
        pipeline_stage="ignored",
        is_protected=1,
        lexicon_status="skipped",
        pipeline_error=f"{TOMBSTONE_ERROR_PREFIX}{_iso(now)} ({reason})",
    )
    log_activity(
        db_path, "lexicon_deleted_tombstoned", tid,
        f"Deleted in Lexicon — tombstoned: {track.get('artist')} - {track.get('title')}"
        + (f" (NAS file moved to recycle bin)" if trashed else ""),
        {"reason": reason, "lexicon_track_id": track.get("lexicon_track_id"),
         "trashed_path": trashed, "file_path": file_path},
    )
    return {"track_id": tid, "reason": reason, "trashed": trashed}


def restore_tombstone(db_path: str, track_id: int) -> dict:
    """Explicit user reversal. Un-trashes the file if it is still in the bin, closes
    the tombstone (restored_at), and re-arms the track from 'new' so the normal
    pipeline re-resolves it. Raises ValueError when there is nothing to restore."""
    with get_db(db_path) as conn:
        tb = conn.execute(
            """SELECT * FROM tombstones WHERE track_id = ? AND restored_at IS NULL
               ORDER BY id DESC LIMIT 1""",
            (track_id,),
        ).fetchone()
        if not tb:
            raise ValueError(f"track {track_id} has no live tombstone")
        tb = dict(tb)

    restored_file = None
    if tb.get("trashed_path") and tb.get("file_path") and os.path.isfile(tb["trashed_path"]):
        try:
            os.makedirs(os.path.dirname(tb["file_path"]), exist_ok=True)
            shutil.move(tb["trashed_path"], tb["file_path"])
            restored_file = tb["file_path"]
        except OSError as e:
            log.warning("restore: could not move %s back: %s", tb["trashed_path"], e)

    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE tombstones SET restored_at = ? WHERE id = ?", (_iso(_now()), tb["id"])
        )
    # Match reset mirrors /tracks/{id}/retry; the file (if restored) is re-found by
    # the file-index / library checks, otherwise the track is re-sourced.
    update_track(
        db_path, track_id,
        pipeline_stage="new", is_protected=0, pipeline_error=None,
        match_status="pending", download_status="pending", verify_status="pending",
        lexicon_status="pending", lexicon_track_id=None,
        file_path=restored_file,
    )
    log_activity(
        db_path, "tombstone_restored", track_id,
        "Tombstone restored by user — track re-entered the pipeline"
        + (" (file recovered from recycle bin)" if restored_file else ""),
        {"tombstone_id": tb["id"], "restored_file": restored_file},
    )
    return {"track_id": track_id, "restored_file": restored_file}


def purge_expired(db_path: str) -> int:
    """Delete WaxFlow-trashed files whose purge_after has passed. Only ever touches
    paths this module wrote into tombstones.trashed_path."""
    now = _iso(_now())
    purged = 0
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, trashed_path FROM tombstones
               WHERE trashed_path IS NOT NULL AND purged_at IS NULL
                 AND restored_at IS NULL AND purge_after IS NOT NULL AND purge_after <= ?""",
            (now,),
        ).fetchall()
    for r in rows:
        path = r["trashed_path"]
        # Only ever delete inside the recycle bin. A tombstone row pointing anywhere
        # else (corrupt row, hand-edited DB) is refused, loudly.
        if not path or f"/{RECYCLE_DIRNAME}/" not in path:
            log.warning("purge: refusing to delete %s — not a recycle-bin path", path)
            continue
        try:
            if path and os.path.isfile(path):
                os.remove(path)
            with get_db(db_path) as conn:
                conn.execute("UPDATE tombstones SET purged_at = ? WHERE id = ?", (now, r["id"]))
            purged += 1
        except OSError as e:
            log.warning("purge: could not delete %s: %s", path, e)
    if purged:
        log_activity(db_path, "tombstone_purged", None,
                     f"Purged {purged} recycle-bin file(s) older than the retention window",
                     {"count": purged})
    return purged


# --------------------------------------------------------------------------- the pass
def run_reconcile(db_path: str, *, probe_fn=None, fetch_fn=None) -> dict:
    """One reconcile pass. Sync; the worker wraps it in a thread.

    ``probe_fn`` / ``fetch_fn`` exist for tests (offline). Returns a summary dict and
    never raises — a failure is logged and the pass is a no-op.
    """
    if not is_enabled(db_path):
        return {"status": "disabled"}

    if probe_fn is None:
        from tasks.mac_availability import probe as probe_fn  # noqa: N806
    try:
        avail = probe_fn(db_path)
    except Exception as e:  # noqa: BLE001
        log.warning("lexicon_reconcile: availability probe failed (%s) — skipping", e)
        return {"status": "skipped", "why": "probe_error"}
    if not avail.lexicon_available:
        return {"status": "skipped", "why": avail.state}

    api_url = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    if fetch_fn is None:
        fetch_fn = fetch_lexicon_ids
    try:
        live_ids, archived_ids = fetch_fn(api_url)
    except LexiconListingIncomplete as e:
        log.warning("lexicon_reconcile: incomplete listing (%s) — no writes", e)
        return {"status": "skipped", "why": f"listing_incomplete: {e}"}
    except Exception as e:  # noqa: BLE001
        log.warning("lexicon_reconcile: listing failed (%s) — no writes", e)
        return {"status": "skipped", "why": f"listing_failed: {e}"}

    min_lib = _int(db_path, "lexicon_reconcile_min_library", DEFAULT_MIN_LIBRARY)
    if len(live_ids) < min_lib:
        log.error(
            "lexicon_reconcile: Lexicon reports only %d tracks (< %d) — refusing to "
            "tombstone anything; is the library being restored?", len(live_ids), min_lib,
        )
        log_activity(db_path, "lexicon_reconcile_refused", None,
                     f"Lexicon listed only {len(live_ids)} tracks (< {min_lib}); reconcile refused",
                     {"live": len(live_ids), "min": min_lib})
        return {"status": "refused", "live": len(live_ids)}

    trash = _flag(db_path, "tombstone_trash_enabled", True)
    batch = _int(db_path, "tombstone_batch", DEFAULT_BATCH)

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM tracks
               WHERE pipeline_stage = 'complete'
                 AND lexicon_track_id IS NOT NULL AND lexicon_track_id != ''
               ORDER BY id ASC"""
        ).fetchall()
        candidates = [dict(r) for r in rows]

    deleted, archived, trashed = 0, 0, 0
    for t in candidates:
        try:
            lid = int(str(t["lexicon_track_id"]).strip())
        except (TypeError, ValueError):
            continue
        if lid in live_ids and lid not in archived_ids:
            continue
        if deleted + archived >= batch:
            break
        reason = REASON_ARCHIVED if lid in archived_ids else REASON_DELETED
        res = tombstone_track(db_path, t, reason, trash=trash)
        if res.get("skipped"):
            continue
        if reason == REASON_ARCHIVED:
            archived += 1
        else:
            deleted += 1
        if res.get("trashed"):
            trashed += 1

    purged = purge_expired(db_path)

    summary = {
        "status": "ok", "live": len(live_ids), "checked": len(candidates),
        "deleted": deleted, "archived": archived, "trashed": trashed, "purged": purged,
    }
    if deleted or archived or purged:
        log.info("lexicon_reconcile: %s", summary)
        log_activity(
            db_path, "lexicon_reconcile_pass", None,
            f"Lexicon reconcile: {deleted} deleted + {archived} archived tombstoned "
            f"({trashed} NAS file(s) moved to recycle bin), {purged} purged",
            summary,
        )
    else:
        log.debug("lexicon_reconcile: clean (%d live, %d checked)", len(live_ids), len(candidates))
    return summary


async def lexicon_reconcile(db_path: str) -> None:
    """Worker task entry point."""
    import asyncio
    try:
        await asyncio.to_thread(run_reconcile, db_path)
    except Exception as e:  # noqa: BLE001 — never kill the loop
        log.error("lexicon_reconcile crashed: %s", e, exc_info=True)
