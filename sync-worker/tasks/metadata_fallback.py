"""WaxFlow — metadata/ISRC re-resolution fallback (MusicBrainz).

THE PROBLEM. In Match Review Will sees tracks show "no match" because the liked
song was removed from Spotify. WaxFlow's normal matcher already searched Tidal by
the cached ISRC and by title+artist and found nothing, so the track sits at
``match_status='failed' / pipeline_stage='error'`` ("no match").

THE FALLBACK. This task recovers those tracks WITHOUT depending on the live
Spotify track, using only the data WaxFlow already cached at like-time (ISRC,
title, artist, album, duration_ms). For each unmatched track it:

  1. Re-resolves the track via MusicBrainz (free, no key): ISRC -> recording ->
     canonical title/artist + the recording's FULL ISRC set across every release.
     If the cached ISRC isn't catalogued, it falls back to a recording search by
     artist+title (duration-tie-broken).
  2. Re-attempts the Tidal match with those ALTERNATE ISRCs (a track pulled from
     Spotify under one ISRC frequently still exists on Tidal under a different
     release/ISRC of the same recording) and with MusicBrainz's canonical
     title/artist.
  3. If a Tidal candidate is found, it surfaces it in Match Review as a
     FALLBACK-sourced proposal (``match_status='mismatched'``,
     ``match_source='musicbrainz_isrc' | 'musicbrainz_search'``) for human
     approve/reject. It is NEVER auto-imported — approving runs the normal
     download->verify->organize pipeline; rejecting re-arms the track normally.

SAFETY / CORRECTNESS.
  * Non-destructive: only updates the track's sync.db row (proposes a match) and
    writes activity/attempt log rows. Never deletes/moves a file, never touches
    Lexicon.
  * Idempotent + non-looping: every attempted track gets a
    ``source_attempts(source='musicbrainz')`` row, so it is attempted at most once
    per exponential-backoff window and a later reject cannot loop it back in.
  * Hunter-safe: a recovered proposal is shielded with a ``wanted`` row
    (``state='review'``) so the missing-track hunter — which re-arms ``error``-stage
    tracks — leaves the human-review proposal untouched.

Config (app_config): ``metadata_fallback_enabled`` (default ON),
``metadata_fallback_batch`` (default 8), ``musicbrainz_user_agent``,
``metadata_fallback_interval_seconds`` (scheduler default 3600).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from tasks import musicbrainz as mb
from tasks.helpers import get_config, get_db, log_activity, update_track

log = logging.getLogger("worker.metadata_fallback")

_DEFAULT_BATCH = 8
_MB_PACING_SECONDS = 1.2  # MusicBrainz asks for <= ~1 req/sec; be courteous.


def _enabled(db_path: str) -> bool:
    # Default ON: the fallback is read-only externally and only proposes matches
    # for human review, so it is safe to run without an explicit enable step.
    val = get_config(db_path, "metadata_fallback_enabled")
    return val is None or val not in ("0", "false", "False", "")


def _batch(db_path: str) -> int:
    try:
        return int(get_config(db_path, "metadata_fallback_batch") or _DEFAULT_BATCH)
    except (TypeError, ValueError):
        return _DEFAULT_BATCH


def _user_agent(db_path: str) -> str:
    return get_config(db_path, "musicbrainz_user_agent") or mb._DEFAULT_UA


def _candidates(db_path: str, limit: int) -> list[dict]:
    """No-match tracks not yet attempted by the MusicBrainz fallback.

    A track is a candidate when it is parked as unmatched (``failed`` / ``error``)
    and has SOME cached identity (ISRC, or title+artist) to resolve from, and has
    no prior ``source_attempts`` row for source='musicbrainz' (idempotency guard).
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT t.* FROM tracks t
                WHERE t.match_status = 'failed'
                  AND t.pipeline_stage = 'error'
                  AND (t.isrc IS NOT NULL
                       OR (t.title IS NOT NULL AND t.artist IS NOT NULL))
                  AND NOT EXISTS (
                      SELECT 1 FROM source_attempts sa
                       WHERE sa.track_id = t.id AND sa.source = 'musicbrainz'
                  )
                ORDER BY t.updated_at DESC
                LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def _resolve_via_musicbrainz(track: dict, user_agent: str) -> dict | None:
    """Return a MusicBrainz recording {mbid,title,artist,isrcs[]} + how it matched."""
    isrc = (track.get("isrc") or "").strip()
    if isrc:
        rec = mb.recording_from_isrc(isrc, user_agent)
        if rec:
            rec["_via"] = "isrc"
            return rec
    rec = mb.search_recording(
        track.get("artist") or "", track.get("title") or "",
        track.get("duration_ms"), user_agent,
    )
    if rec:
        rec["_via"] = "search"
    return rec


def _recover(db_path: str, track: dict, rec: dict) -> dict | None:
    """Re-attempt a match using MusicBrainz's alternate ISRCs + canonical name.

    Priority order (highest value + safest first):
      0. LOCAL library (file_index under /music) — the track may be one Will
         ALREADY OWNS that the original match missed on a metadata quirk (feat.
         ordering, capitalization). No download, no Tidal dependency. This is the
         most valuable recovery, and the file lives under /music where the worker
         can verify it via the normal existing-file pipeline.
      1. TIDAL by ALTERNATE ISRC — a track removed from Spotify under one ISRC often
         still exists on Tidal under a different release/ISRC of the same recording.
      2. TIDAL by canonical MusicBrainz title/artist.

    (A recovery against the Mac-side Lexicon library by canonical name is a known
    follow-up — it needs the organizing-stage Lexicon-link path, whose lossless gate
    reads the file the container can't reach, so it is intentionally NOT done here.)

    Returns a dict describing the recovery, or None.
    """
    from tasks import process_pipeline as pp
    from tasks.sources import tidal as tidal_source

    original_isrc = (track.get("isrc") or "").strip().upper()
    duration_ms = track.get("duration_ms") or 0

    # Strategy 0 — already-owned LOCAL file via alternate ISRC, then canonical name.
    for alt in rec.get("isrcs") or []:
        if not alt:
            continue
        m = pp._check_existing_by_isrc(
            db_path, {"isrc": alt, "title": rec.get("title"), "artist": rec.get("artist")}
        )
        if m and pp._is_likely_lossless(m.get("file_path", "")):
            return {"kind": "local", "file_path": m["file_path"],
                    "match_source": "musicbrainz_local", "confidence": 1.0,
                    "matched_isrc": alt}
        if m:  # owned but lossy — still recover; verify stage will source lossless
            return {"kind": "local", "file_path": m["file_path"],
                    "match_source": "musicbrainz_local", "confidence": 0.9,
                    "matched_isrc": alt}
    can_local = pp._check_existing_by_isrc(
        db_path, {"isrc": None, "title": rec.get("title"), "artist": rec.get("artist")}
    ) or pp._check_existing_in_library(
        {"title": rec.get("title"), "artist": rec.get("artist")}, db_path
    )
    if can_local:
        return {"kind": "local", "file_path": can_local["file_path"],
                "match_source": "musicbrainz_local", "confidence": 0.85,
                "matched_isrc": None}

    # Strategy 1 — Tidal via alternate ISRCs from MusicBrainz (skip the one we tried).
    for alt in rec.get("isrcs") or []:
        if not alt or alt == original_isrc:
            continue
        try:
            results = tidal_source.search_raw(alt)
        except Exception as e:  # noqa: BLE001
            log.debug("Tidal ISRC search failed (%s): %s", alt, e)
            continue
        for item in results:
            if (item.get("isrc") or "").upper() == alt:
                return {
                    "kind": "tidal",
                    "tidal_id": str(item["id"]),
                    "match_source": "musicbrainz_isrc",
                    "confidence": 0.97,
                    "matched_isrc": alt,
                }

    # Strategy 2 — canonical MusicBrainz title/artist via title/artist matching.
    can_title = rec.get("title") or track.get("title") or ""
    can_artist = rec.get("artist") or track.get("artist") or ""
    if not (can_title and can_artist):
        return None
    try:
        results = tidal_source.search_raw(f"{can_artist} {can_title}".strip())
    except Exception as e:  # noqa: BLE001
        log.debug("Tidal metadata search failed: %s", e)
        return None

    spotify_norm = pp._normalize_title(can_title)
    best_id, best_conf = None, 0.0
    for item in results:
        item_title = item.get("title") or ""
        item_norm = pp._normalize_title(item_title)
        tidal_artist = ""
        if item.get("artist", {}).get("name"):
            tidal_artist = item["artist"]["name"]
        elif item.get("artists"):
            tidal_artist = " ".join(a.get("name", "") for a in item["artists"])
        if not pp._artists_match(can_artist, tidal_artist):
            continue
        item_dur = (item.get("duration") or 0) * 1000
        dur_diff = abs(item_dur - duration_ms)
        exact = spotify_norm == item_norm
        partial = spotify_norm in item_norm or item_norm in spotify_norm
        conf = 0.0
        if exact and dur_diff <= 5000:
            conf = 0.90
        elif exact and dur_diff <= 15000:
            conf = 0.85
        elif partial and dur_diff <= 5000:
            conf = 0.82
        if conf > best_conf:
            best_conf, best_id = conf, str(item["id"])
    if best_id and best_conf >= 0.82:
        return {
            "kind": "tidal",
            "tidal_id": best_id,
            "match_source": "musicbrainz_search",
            "confidence": best_conf,
            "matched_isrc": None,
        }
    return None


def _record_attempt(db_path: str, track_id: int, status: str, detail: str | None = None):
    """Idempotency + backoff breadcrumb via the shared SourceBackoff log."""
    try:
        from tasks.sources.base import SourceBackoff
        SourceBackoff.record(db_path, track_id, "musicbrainz", status, error=detail)
    except Exception as e:  # noqa: BLE001 — never let logging break the loop
        log.debug("source_attempts record failed for track %d: %s", track_id, e)


def _shield_from_hunter(conn, track_id: int, reason: str):
    """Insert/lift a wanted(state='review') row so the hunter won't re-arm this
    human-review proposal. The hunter only enqueues error-stage tracks NOT already
    in ``wanted`` and only re-attempts state='wanted', so state='review' is inert
    to it while keeping the track visible as a wanted-ledger entry."""
    existing = conn.execute(
        "SELECT id FROM wanted WHERE track_id = ? LIMIT 1", (track_id,)
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE wanted SET state='review', reason=?, updated_at=datetime('now') WHERE id=?",
            (reason, existing["id"]),
        )
    else:
        conn.execute(
            "INSERT INTO wanted (track_id, state, reason) VALUES (?, 'review', ?)",
            (track_id, reason),
        )


def _propose_match(db_path: str, track: dict, rec: dict, hit: dict):
    """Surface a recovered match in Match Review, labeled fallback-sourced.

    A 'local' recovery (Will already owns the file under /music) is proposed with
    the file already attached (download_status='complete') so approving routes it
    straight into the existing verify->organize path. A 'tidal' recovery is proposed
    with the tidal_id so approving downloads it. Either way match_status='mismatched'
    surfaces it in Match Review; pipeline_stage stays 'error' (nothing auto-advances
    it) and it is shielded from the hunter until a human decides.
    """
    track_id = track["id"]
    provenance = {
        "fallback": {
            "via": hit["match_source"],
            "kind": hit["kind"],
            "mb_recording_id": rec.get("mbid"),
            "resolved_title": rec.get("title"),
            "resolved_artist": rec.get("artist"),
            "matched_isrc": hit.get("matched_isrc"),
            "original_isrc": track.get("isrc"),
            "tidal_id": hit.get("tidal_id"),
            "file_path": hit.get("file_path"),
        }
    }
    fields = dict(
        match_status="mismatched",
        match_source=hit["match_source"],
        match_confidence=hit["confidence"],
        pipeline_error="Recovered via MusicBrainz fallback — awaiting Match Review approval",
        notes=json.dumps(provenance),
    )
    if hit["kind"] == "local":
        # Already-owned file: attach it so approve -> verifying -> organizing (the
        # proven existing-file path; a lossy owned file will source lossless via the
        # normal verify-stage Soulseek route).
        fields.update(
            file_path=hit["file_path"],
            download_status="complete",
            download_source="existing",
        )
        detail = f"local file {hit['file_path']}"
    else:
        fields.update(tidal_id=hit["tidal_id"], download_status="pending")
        detail = f"tidal_id={hit['tidal_id']}"

    update_track(db_path, track_id, **fields)
    with get_db(db_path) as conn:
        _shield_from_hunter(conn, track_id, f"mb_fallback:{hit['match_source']}")
    log_activity(
        db_path, "metadata_fallback_recovered", track_id,
        f"Recovered '{track.get('artist')} - {track.get('title')}' via {hit['match_source']} "
        f"({detail}, conf={hit['confidence']}) — surfaced in Match Review",
        provenance["fallback"],
    )
    log.info("Track %d recovered via %s (%s)", track_id, hit["match_source"], detail)


def _run_sync(db_path: str) -> dict:
    counts = {"attempted": 0, "recovered": 0, "mb_hit_no_source": 0, "no_mb": 0}
    if not _enabled(db_path):
        return counts
    user_agent = _user_agent(db_path)
    for track in _candidates(db_path, _batch(db_path)):
        track_id = track["id"]
        counts["attempted"] += 1
        try:
            rec = _resolve_via_musicbrainz(track, user_agent)
            time.sleep(_MB_PACING_SECONDS)  # courteous MusicBrainz pacing
            if not rec:
                counts["no_mb"] += 1
                _record_attempt(db_path, track_id, "no_mb_recording")
                continue
            hit = _recover(db_path, track, rec)
            if hit:
                _propose_match(db_path, track, rec, hit)
                _record_attempt(db_path, track_id, "recovered", hit["match_source"])
                counts["recovered"] += 1
            else:
                counts["mb_hit_no_source"] += 1
                _record_attempt(db_path, track_id, "mb_hit_no_source", rec.get("mbid"))
        except Exception as e:  # noqa: BLE001 — one bad track must not stall the task
            log.warning("metadata_fallback error for track %d: %s", track_id, e, exc_info=True)
            _record_attempt(db_path, track_id, "error", str(e)[:300])
    if counts["attempted"]:
        log.info(
            "metadata_fallback: attempted=%d recovered=%d mb_hit_no_source=%d no_mb=%d",
            counts["attempted"], counts["recovered"], counts["mb_hit_no_source"], counts["no_mb"],
        )
        log_activity(
            db_path, "metadata_fallback_batch", None,
            f"MusicBrainz fallback: {counts['recovered']} recovered / {counts['attempted']} attempted "
            f"({counts['mb_hit_no_source']} MB-hit-no-source, {counts['no_mb']} no-MB)",
            counts,
        )
    return counts


async def metadata_fallback(db_path: str):
    """Worker entrypoint — run one MusicBrainz re-resolution pass off the loop."""
    await asyncio.to_thread(_run_sync, db_path)
