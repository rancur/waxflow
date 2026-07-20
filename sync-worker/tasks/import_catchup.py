"""Sleep-tolerance catch-up pass — rescue downloaded-but-not-imported tracks.

WHY
    The always-on NAS worker downloads a track fine even while the Lexicon-host
    Mac is asleep. The import into Lexicon happens in the ``organizing`` stage,
    which talks to the Mac. The Phase-3 offline queue (``offline_queue.py``) HOLDS
    organizing work *proactively* when a pre-check says the Mac/Lexicon is
    unavailable — but there is a gap it cannot cover: a Lexicon call that PASSES the
    pre-check (SSH port answered, ``GET /v1/playlists`` returned 200) and then FAILS
    mid-import as the Mac slips into sleep — ``database is locked``, ``timed out``,
    or an empty import once the SMB mount drops. Those land the track in a TERMINAL
    ``pipeline_stage='error'`` with ``download_status='complete'`` and no
    ``lexicon_track_id`` — a real, playable file on disk that Lexicon never got, and
    nothing ever retries it. Observed live (5 tracks, all timestamped overnight /
    early-AM): NGHTMRE "Hold Me Close", Goo Goo Dolls "Iris", Jeff Buckley
    "Hallelujah", The Maine "I Wanna Love You", Ludwig Göransson "Hades".

WHAT
    A periodic pass that, ONLY WHEN Lexicon is available (so it naturally fires on
    the first cycle after the Mac wakes — the "on-wake scan"), finds those orphans
    and revives them to the appropriate earlier pipeline stage so the normal, fully
    guarded pipeline re-imports them on the next cycle. It is purely a re-arm: it
    performs NO Lexicon write itself — the real import runs through
    ``_process_organizing`` / ``_process_verifying`` with all their guards
    (lossless gate, ISRC match guard, empty-import grace, dedup existence check),
    which are idempotent, so a track already in Lexicon links instead of duplicating.

SAFETY
    * Non-destructive: only flips pipeline_stage/lexicon_status/pipeline_error on
      the track row and bumps a counter. Never touches files, Lexicon, or playlists.
    * Bounded (no hot loop): only revives an error older than ``min_age`` seconds,
      and at most ``max_attempts`` times per track (``tracks.catchup_attempts``), so
      a genuinely-broken track is retried a handful of times then left alone.
    * Targeted: only tracks whose error signature is a transient Lexicon/Mac-
      unavailability condition and whose download is complete. Import orphans
      (no lexicon_track_id) additionally require the file to still exist on disk;
      bookkeeping orphans (import succeeded, then the playlist/tag step failed as
      the Mac slept — lexicon_track_id set) are re-armed without a file check
      because organizing is idempotent for them (reuses the id, dedups the
      playlist add).
    * Gated: ``import_catchup_enabled`` (default ON) — flip to 0 to disable live.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from tasks.helpers import get_config, get_db, log_activity, update_track

log = logging.getLogger("worker.import_catchup")

# Only revive an error that has been settled for at least this long, so the pass
# never races a track another cycle is actively (re)processing.
_DEFAULT_MIN_AGE_SECONDS = 300
# Hard ceiling on revivals per track — bounds retries so a track that keeps failing
# for a non-sleep reason cannot loop forever.
_DEFAULT_MAX_ATTEMPTS = 6
_BATCH = 200

# Substrings that mark a terminal 'error' as a *transient* Lexicon/Mac-unavailability
# failure (the sleep signature) rather than a per-track data problem. Matched
# case-insensitively against pipeline_error. Deliberately specific — a "not lossless"
# or "fingerprint too low" error is NOT re-armed here.
_TRANSIENT_SIGNATURES = (
    "database is locked",
    "timed out",
    "timeout",
    "mount down",
    "mount likely down",
    "file never synced",
    "returned 0 tracks",
    "lexicon_import_empty",
    "import empty",
    "connection refused",
    "connection reset",
    "connection error",
    "network is unreachable",
    "unreachable",
    "read timed out",
    "temporarily unavailable",
)


def is_catchup_enabled(db_path: str) -> bool:
    """Feature flag, default ON. The catch-up pass is the durable sleep-tolerance
    fix, so it ships enabled; set ``import_catchup_enabled=0`` to disable live."""
    val = get_config(db_path, "import_catchup_enabled")
    if val is None:
        return True
    return str(val).lower() in ("1", "true", "on", "yes")


def _int_config(db_path: str, key: str, default: int) -> int:
    try:
        return int(get_config(db_path, key) or default)
    except (TypeError, ValueError):
        return default


def _is_transient(pipeline_error: str | None) -> bool:
    if not pipeline_error:
        return False
    low = pipeline_error.lower()
    return any(sig in low for sig in _TRANSIENT_SIGNATURES)


def _reentry_stage(pipeline_error: str | None) -> str:
    """A track that failed at the VERIFY stage (e.g. sync.db lock during ffprobe
    bookkeeping) must re-enter at 'verifying' so it is re-verified before import;
    everything else failed at import time and re-enters at 'organizing'."""
    if pipeline_error and "verification error" in pipeline_error.lower():
        return "verifying"
    return "organizing"


def run_catchup(db_path: str) -> dict:
    """Revive downloaded-but-not-imported tracks stranded in 'error' by a transient
    Lexicon/Mac-unavailability failure. Returns a counts dict.

    Only acts when Lexicon is actually available (on-wake behaviour); a pure no-op
    otherwise. Idempotent + bounded (see module docstring)."""
    if not is_catchup_enabled(db_path):
        return {"skipped": "disabled"}

    # On-wake gate: nothing can be imported while the Mac is asleep / Lexicon down.
    # Probing here means the pass does its work on the first cycle after wake.
    try:
        from tasks.mac_availability import probe
        avail = probe(db_path)
        if not avail.lexicon_available:
            return {"skipped": "lexicon_unavailable", "state": avail.state}
    except Exception as e:  # never let the probe break the pass
        log.warning("import_catchup: availability probe failed (%s) — skipping", e)
        return {"skipped": "probe_error"}

    min_age = _int_config(db_path, "import_catchup_min_age_seconds", _DEFAULT_MIN_AGE_SECONDS)
    max_attempts = _int_config(db_path, "import_catchup_max_attempts", _DEFAULT_MAX_ATTEMPTS)

    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM tracks
               WHERE pipeline_stage = 'error'
                 AND download_status = 'complete'
                 AND COALESCE(catchup_attempts, 0) < ?
                 AND (updated_at IS NULL OR updated_at <= datetime('now', ?))
               ORDER BY updated_at ASC
               LIMIT ?""",
            (max_attempts, f"-{int(min_age)} seconds", _BATCH),
        ).fetchall()
        candidates = [dict(r) for r in rows]

    revived = 0
    missing_file = 0
    skipped_nontransient = 0

    for track in candidates:
        if not _is_transient(track.get("pipeline_error")):
            skipped_nontransient += 1
            continue

        # Two classes of sleep-orphan, both revivable:
        #   (1) IMPORT orphan — no lexicon_track_id: the file was downloaded but
        #       Lexicon never got it. Requires the file to still exist on disk.
        #   (2) BOOKKEEPING orphan — lexicon_track_id IS set: the import itself
        #       succeeded, then a post-import step (monthly-playlist add, [sls:]
        #       comment tag) hit a transient failure ("database is locked" as the
        #       Mac slipped into sleep). Re-arming to 'organizing' is idempotent —
        #       the pipeline reuses the existing lexicon_track_id, skips the
        #       import, and dedups the playlist add — so no file check is needed
        #       (Lexicon may have organized/renamed the file since).
        already_imported = bool(track.get("lexicon_track_id"))
        file_path = track.get("file_path")
        # The worker container mounts /music and /downloads, and file_path is the
        # container path, so this existence check is authoritative for "the file we
        # downloaded is still on disk and re-importable".
        if not already_imported and (not file_path or not os.path.exists(file_path)):
            missing_file += 1
            log.warning(
                "import_catchup: track %d file missing on disk (%s) — not revived",
                track["id"], file_path,
            )
            continue

        stage = "organizing" if already_imported else _reentry_stage(track.get("pipeline_error"))
        attempts = int(track.get("catchup_attempts") or 0) + 1
        update_track(
            db_path, track["id"],
            pipeline_stage=stage,
            lexicon_status="pending",
            catchup_attempts=attempts,
            pipeline_error=(
                f"[catchup {attempts}/{max_attempts}] re-armed to '{stage}' after "
                f"sleep-orphaned import failure; awaiting re-import"
            ),
        )
        log_activity(
            db_path, "import_catchup_revived", track["id"],
            f"Re-armed sleep-orphaned download for import: {track.get('artist')} - {track.get('title')}",
            {"reentry_stage": stage, "attempt": attempts,
             "was_error": (track.get("pipeline_error") or "")[:160]},
        )
        log.info(
            "import_catchup: revived track %d (%s - %s) -> %s (attempt %d/%d)",
            track["id"], track.get("artist"), track.get("title"),
            stage, attempts, max_attempts,
        )
        revived += 1

    counts = {
        "revived": revived,
        "missing_file": missing_file,
        "skipped_nontransient": skipped_nontransient,
        "candidates": len(candidates),
    }
    if revived or missing_file:
        log.info("import_catchup: pass complete %s", counts)
        log_activity(
            db_path, "import_catchup_pass", None,
            f"Catch-up pass revived {revived} sleep-orphaned track(s)",
            counts,
        )
    return counts


async def import_catchup(db_path: str) -> None:
    """Async worker-task entry point (mirrors the other task wrappers)."""
    await asyncio.to_thread(run_catchup, db_path)
