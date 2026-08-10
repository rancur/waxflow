"""Quality rechecker — keep hunting until every track is at or above the cutoff.

THE MODEL (Radarr/Sonarr)
    A profile says three things: the FLOOR we will never go below, the CUTOFF at
    which we stop looking, and the TARGET we ask for first. Downloading walks the
    ladder DOWN (hi-res, 24-bit, lossless, 320k) and takes the best available.
    This module walks it UP: anything still below the cutoff stays on the hunt, and
    when something better turns up the file is replaced and Lexicon re-pointed.

WHY A SEPARATE WATCHER
    The download path only ever sees a track once. A 320k copy accepted today
    because nothing better existed should become lossless the week someone uploads
    it -- which means periodically re-asking, forever, without re-downloading things
    that are already good enough. The cutoff is what makes that terminate.

WHAT IT WILL NOT DO
    Replace anything on its own. A verified better file is staged in
    `relocation_queue` and applied by the relocator during a maintenance window,
    because swapping the file also means rewriting Lexicon's Track.location and that
    must happen with Lexicon quit. Staging and applying are deliberately separate:
    this task runs whenever, the write happens only when it is safe.

CONFIG (all read live from app_config)
    quality_upgrade_enabled            default 1
    quality_upgrade_interval_seconds   default 21600 (6h)
    quality_upgrade_batch              default 3     tracks per cycle
    quality_upgrade_max_attempts       default 6     before giving up on a track
    quality_upgrade_min_free_gb        default 20
    quality_floor_tier / _cutoff_tier / _target_tier   the profile
    relocation_enabled                 must be on, or nothing is staged
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone

from tasks.helpers import get_config, get_db, log_activity, update_track

log = logging.getLogger("worker.quality_upgrade")

DEFAULT_INTERVAL = 21600
DEFAULT_BATCH = 3
DEFAULT_MAX_ATTEMPTS = 6
DEFAULT_MIN_FREE_GB = 20


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _flag(db_path: str, key: str, default: str) -> bool:
    raw = get_config(db_path, key)
    raw = default if raw is None or raw == "" else raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _num(db_path: str, key: str, default: int) -> int:
    try:
        return int(str(get_config(db_path, key) or default).strip())
    except (TypeError, ValueError):
        return default


def is_enabled(db_path: str) -> bool:
    return _flag(db_path, "quality_upgrade_enabled", "1")


def can_stage(db_path: str) -> bool:
    """Staging is pointless unless something can later apply it."""
    return _flag(db_path, "relocation_enabled", "0")


def _free_gb(path: str) -> float:
    try:
        return shutil.disk_usage(path).free / 1e9
    except OSError:
        return float("inf")


def find_candidates(db_path: str, profile: dict, limit: int) -> list[dict]:
    """Tracks sitting below the cutoff, oldest-checked first.

    Ordering by last check makes this a fair round-robin rather than a loop that
    re-attacks the same few tracks every cycle.
    """
    from tasks import quality
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT t.id, t.artist, t.title, t.duration_ms, t.file_path,
                      t.lexicon_track_id, t.quality_tier, t.quality_score,
                      t.quality_bit_rate, t.verify_codec, t.verify_sample_rate,
                      t.verify_bit_depth, t.upgrade_attempts, t.upgrade_checked_at
               FROM tracks t
               WHERE t.pipeline_stage = 'complete'
                 AND t.file_path IS NOT NULL
                 AND COALESCE(t.upgrade_state, '') <> 'exhausted'
                 AND COALESCE(t.upgrade_attempts, 0) < ?
                 AND NOT EXISTS (
                     SELECT 1 FROM relocation_queue r
                     WHERE r.track_id = t.id AND r.state = 'pending')
               ORDER BY COALESCE(t.upgrade_checked_at, '') ASC, t.id ASC
               LIMIT ?""",
            (_num(db_path, "quality_upgrade_max_attempts", DEFAULT_MAX_ATTEMPTS),
             limit * 8),
        ).fetchall()

    out = []
    for row in rows:
        track = dict(row)
        current = quality.score_row(track)
        if quality.needs_upgrade(current, profile):
            track["_current"] = current
            out.append(track)
        if len(out) >= limit:
            break
    return out


def _stage_replacement(db_path: str, track: dict, new_path: str,
                       old_score, new_score) -> None:
    """Record a verified improvement for the relocator to apply."""
    with get_db(db_path) as conn:
        conn.execute(
            """INSERT INTO relocation_queue
                   (track_id, lexicon_track_id, old_path, new_path,
                    old_score, new_score, old_tier, new_tier, state, created_at)
               VALUES (?,?,?,?,?,?,?,?, 'pending', datetime('now'))""",
            (track["id"], track.get("lexicon_track_id"), track.get("file_path"),
             new_path, old_score.score if old_score else None, new_score.score,
             old_score.tier_name if old_score else None, new_score.tier_name),
        )
    log_activity(
        db_path, "upgrade_found", track["id"],
        f"Found {new_score.tier_name} to replace "
        f"{old_score.tier_name if old_score else 'unknown'} — staged for relocation",
        {"old": old_score.as_dict() if old_score else None,
         "new": new_score.as_dict(), "new_path": new_path},
    )
    log.info("Track %d: staged upgrade %s -> %s", track["id"],
             old_score.tier_name if old_score else "?", new_score.tier_name)


def _record_attempt(db_path: str, track_id: int, attempts: int, exhausted: bool) -> None:
    update_track(
        db_path, track_id,
        upgrade_attempts=attempts,
        upgrade_checked_at=_now_iso(),
        upgrade_state="exhausted" if exhausted else "hunting",
    )


def process_one(db_path: str, track: dict, client, profile: dict) -> bool:
    """Hunt for a better copy of one track. Returns True if an upgrade was staged."""
    from tasks import quality
    from tasks import soulseek_fallback as sf
    from tasks.lossless_verify import verify_lossless

    current = track["_current"]
    attempts = (track.get("upgrade_attempts") or 0) + 1
    max_attempts = _num(db_path, "quality_upgrade_max_attempts", DEFAULT_MAX_ATTEMPTS)

    # Only look ABOVE what we already have -- re-downloading a sideways move is pure
    # churn, and it is exactly what a naive "search again" loop does.
    tier, cands, _query = sf.search_best_available(
        client, track.get("artist") or "", track.get("title") or "",
        track.get("duration_ms") or 0, profile, min_tier=current.tier)

    if not cands:
        _record_attempt(db_path, track["id"], attempts, attempts >= max_attempts)
        log.debug("Track %d: nothing better than %s available", track["id"], current.tier_name)
        return False

    tmpdir = tempfile.mkdtemp(prefix="upgrade_")
    try:
        for cand in cands[:sf.MAX_CANDIDATES]:
            local = sf.download_candidate(client, cand, tmpdir)
            if not local:
                continue

            gate = verify_lossless(local, track.get("duration_ms") or 0)
            if not gate.get("passed") and tier > quality.TIER_LOSSY_HIGH:
                # A lossless tier that fails the lossless gate is a fake or a
                # transcode -- exactly what that gate exists to catch.
                log.info("Track %d: candidate failed the lossless gate (%s)",
                         track["id"], "; ".join(gate.get("reasons") or []))
                continue

            candidate_score = quality.score_file(local)
            if not quality.is_upgrade(current, candidate_score):
                log.debug("Track %d: candidate %s is not better than %s",
                          track["id"], candidate_score.tier_name, current.tier_name)
                continue

            dest = sf._move_into_library(db_path, local, track.get("artist") or "",
                                         track.get("title") or "")
            _stage_replacement(db_path, track, dest, current, candidate_score)
            _record_attempt(db_path, track["id"], attempts, False)
            return True

        _record_attempt(db_path, track["id"], attempts, attempts >= max_attempts)
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run(db_path: str) -> dict:
    """One cycle. Returns a small summary for logging/tests."""
    from tasks import soulseek_fallback as sf

    summary = {"checked": 0, "staged": 0, "skipped": None}

    if not is_enabled(db_path):
        summary["skipped"] = "quality_upgrade_enabled=0"
        return summary
    if not can_stage(db_path):
        # Finding an upgrade we cannot install just wastes bandwidth and leaves an
        # orphan file on disk.
        summary["skipped"] = "relocation_enabled=0 — nothing could apply an upgrade"
        return summary
    if not sf.is_enabled(db_path):
        summary["skipped"] = "soulseek_fallback_enabled=0"
        return summary

    library = get_config(db_path, "music_library_path") or os.environ.get(
        "MUSIC_LIBRARY_PATH", "/music/Database")
    min_free = _num(db_path, "quality_upgrade_min_free_gb", DEFAULT_MIN_FREE_GB)
    if _free_gb(library) < min_free:
        summary["skipped"] = f"less than {min_free} GB free"
        log.warning("quality_upgrade: %s", summary["skipped"])
        return summary

    profile = sf.active_profile(db_path)
    batch = _num(db_path, "quality_upgrade_batch", DEFAULT_BATCH)
    candidates = find_candidates(db_path, profile, batch)
    if not candidates:
        return summary

    client = sf.build_client(db_path)
    if not client.is_logged_in():
        summary["skipped"] = "slskd not logged in"
        return summary

    for track in candidates:
        summary["checked"] += 1
        try:
            if process_one(db_path, track, client, profile):
                summary["staged"] += 1
        except Exception as e:  # noqa: BLE001 — one bad track must not stop the cycle
            log.warning("quality_upgrade: track %d failed: %s", track["id"], e)
            _record_attempt(db_path, track["id"],
                            (track.get("upgrade_attempts") or 0) + 1, False)
    return summary


async def quality_upgrade(db_path: str) -> None:
    """Worker entry point."""
    try:
        result = run(db_path)
        if result.get("skipped"):
            log.debug("quality_upgrade skipped: %s", result["skipped"])
        elif result["checked"]:
            log.info("quality_upgrade: checked %d, staged %d",
                     result["checked"], result["staged"])
    except Exception as e:  # noqa: BLE001
        log.warning("quality_upgrade cycle failed: %s", e)
