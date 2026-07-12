"""Soulseek (slskd) fallback stage — a LOSSLESS-VERIFIED alternative to Tidal.

When Tidal (tiddl) cannot deliver a genuinely-lossless copy of a liked track —
either there is no Tidal match, or the Tidal copy is lossy AAC and fails the verify
stage — the track is queued here. This stage:

  1. searches slskd for the track (artist + title),
  2. ranks true-.flac candidates and tries them best-first (multi-peer, because the
     VPN has no forwarded port so some peers can never connect),
  3. downloads the first that transfers, fetches the bytes to the worker,
  4. runs the lossless_verify gate (codec/bits/sr + clean decode + spectral
     transcode/fake-FLAC detection + duration match),
  5. on PASS: files the file into the library exactly like a Tidal download
     (/music/<Artist>/<Artist> - <Title>.flac, chowned) and hands it to the normal
     'verifying' -> 'organizing' import path,
  6. on FAIL / no lossless candidate: leaves the track at 'error' (never imports a fake).

Queue model (why no new pipeline_stage): the tracks table's ``pipeline_stage`` has a
CHECK constraint, so we do NOT invent a new stage value. Instead a track that Tidal
couldn't provide as lossless is parked at the existing 'error' stage AND given a row
in ``fallback_attempts`` with ``source='soulseek', status='queued'``. This stage
drains queued rows; on completion the row is finalised (success / all_failed /
no_candidates) so a track is only ever attempted once.

Everything is behind the app_config flag ``soulseek_fallback_enabled`` (default on).
slskd endpoint/credentials come from app_config (env fallback); see build_client.
All slskd P2P egress is through the sabnzbd VPN on pi-dl; this module only speaks the
LAN REST API + file server.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile

from tasks.helpers import (
    MUSIC_LIBRARY_PATH,
    get_config,
    get_db,
    log_activity,
    sanitize_filename,
    update_track,
)
from tasks.lossless_verify import verify_lossless
from tasks.slskd_client import SlskdClient

log = logging.getLogger("worker.soulseek_fallback")

# Retained for backwards-compat / logging only. NOT written to tracks.pipeline_stage
# (that column is CHECK-constrained); the queue lives in fallback_attempts instead.
STAGE = "soulseek_fallback"
BATCH = 3                      # tracks per cycle (each may do several peer downloads)
MAX_CANDIDATES = 6            # peers to try before giving up on a track
PER_PEER_TIMEOUT_S = 120.0


def build_client(db_path: str) -> SlskdClient:
    """Build an slskd client from app_config (DB), falling back to env defaults.

    Config keys (app_config): slskd_url, slskd_api_key, slskd_files_url,
    slskd_files_user, slskd_files_password. Storing them in the DB (like the Spotify
    tokens) lets the running worker pick up config without a container recreate. Any
    key left unset falls back to the SLSKD_* environment defaults in SlskdClient.
    """
    def cfg(k):
        v = get_config(db_path, k)
        return v if (v is not None and v != "") else None
    return SlskdClient(
        base=cfg("slskd_url"),
        api_key=cfg("slskd_api_key"),
        files_url=cfg("slskd_files_url"),
        files_user=cfg("slskd_files_user"),
        files_password=cfg("slskd_files_password"),
    )


def is_enabled(db_path: str) -> bool:
    val = get_config(db_path, "soulseek_fallback_enabled")
    if val is None:
        return True  # default on
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def already_attempted(db_path: str, track_id: int) -> bool:
    """True if this track already has ANY Soulseek fallback_attempts row (queued or
    finalised) — prevents re-queuing the same track."""
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM fallback_attempts WHERE track_id = ? AND source = 'soulseek' LIMIT 1",
            (track_id,),
        ).fetchone()
    return row is not None


def queue_for_fallback(db_path: str, track_id: int, reason: str) -> None:
    """Park a track for the Soulseek fallback by inserting a queued attempt row.

    The track itself stays at the allowed 'error' pipeline_stage (set by the caller);
    this row is what the fallback stage scans for. Idempotent-ish: guarded by
    already_attempted() at the call site.
    """
    with get_db(db_path) as conn:
        conn.execute(
            """INSERT INTO fallback_attempts (track_id, source, status, search_query)
               VALUES (?, 'soulseek', 'queued', ?)""",
            (track_id, reason),
        )


def _finalize(db_path: str, fa_id: int, status: str, result_count: int = 0,
              error: str | None = None):
    with get_db(db_path) as conn:
        conn.execute(
            """UPDATE fallback_attempts
               SET status = ?, result_count = ?, error = ?, attempted_at = datetime('now')
               WHERE id = ?""",
            (status, result_count, error, fa_id),
        )


def _supersede_stale_queue(db_path: str) -> int:
    """Finalise queued rows whose track has since LEFT the 'error' holding stage.

    A track is parked at 'error' when queued for Soulseek. If something else (e.g. the
    self-heal re-queue, or the normal pipeline) has since moved it on (complete /
    verifying / organizing), the fallback must NOT fight that state machine — mark the
    queued row 'superseded' so it is not reprocessed. Returns the number superseded.
    """
    with get_db(db_path) as conn:
        cur = conn.execute(
            """UPDATE fallback_attempts
               SET status = 'superseded', attempted_at = datetime('now')
               WHERE source = 'soulseek' AND status = 'queued'
                 AND track_id IN (SELECT id FROM tracks WHERE pipeline_stage <> 'error')"""
        )
        return cur.rowcount


def _queued_tracks(db_path: str, limit: int) -> list[dict]:
    """Tracks genuinely parked at 'error' awaiting the Soulseek fallback, oldest first.

    Only 'error'-stage tracks are eligible: that is exactly the holding state the
    routing sets when it queues a track. A queued track that has moved to another
    stage is handled by _supersede_stale_queue (never reprocessed here). The 5s
    settle guard avoids racing a just-updated row.
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT t.*, fa.id AS _fa_id
               FROM tracks t
               JOIN fallback_attempts fa
                 ON fa.track_id = t.id AND fa.source = 'soulseek' AND fa.status = 'queued'
               WHERE t.pipeline_stage = 'error'
                 AND (t.updated_at IS NULL OR t.updated_at < datetime('now', '-5 seconds'))
               GROUP BY t.id
               ORDER BY t.created_at ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def _expected_size_range(duration_ms: int | None):
    """Plausible byte range for a lossless FLAC of this duration (~350-1500 kbps)."""
    if not duration_ms:
        return (5_000_000, 200_000_000)
    secs = duration_ms / 1000.0
    lo = int(secs * 350 * 1000 / 8 * 0.7)
    hi = int(secs * 1500 * 1000 / 8 * 1.6)
    return (max(3_000_000, lo), max(hi, 30_000_000))


def rank_candidates(responses: list[dict], duration_ms: int | None) -> list[dict]:
    """Flatten peer responses to ranked .flac candidates (best download prospect first)."""
    lo, hi = _expected_size_range(duration_ms)
    cands = []
    for r in responses:
        for f in r.get("files", []):
            fn = f.get("filename", "")
            if not fn.lower().endswith(".flac"):
                continue
            size = int(f.get("size") or 0)
            if not (lo <= size <= hi):
                continue
            cands.append({
                "username": r["username"],
                "filename": fn,
                "size": size,
                "free": bool(r.get("hasFreeUploadSlot")),
                "queue": int(r.get("queueLength") or 9999),
                "speed": int(r.get("uploadSpeed") or 0),
            })
    # free slot first, then shortest queue, then fastest
    cands.sort(key=lambda c: (not c["free"], c["queue"], -c["speed"]))
    return cands


def _build_queries(artist: str, title: str) -> list[str]:
    first_artist = artist.split(",")[0].split("&")[0].strip()
    base_title = title
    for sep in (" (", " - ", " ["):
        if sep in base_title:
            base_title = base_title.split(sep)[0].strip()
    queries = []
    for q in (f"{first_artist} {title}", f"{first_artist} {base_title}", title):
        q = " ".join(q.split())
        if q and q.lower() not in [x.lower() for x in queries]:
            queries.append(q)
    return queries


def _move_into_library(db_path: str, src_path: str, artist: str, title: str) -> str:
    """Place a verified file into the music library, mirroring the tiddl download path.

    IMPORTANT (Synology ACL, see _download_track_via_tiddl): /volume1/music carries an
    inheritable Synology ACL that lets Synology Drive deliver a file to the Lexicon
    Mac's ~/Music replica. A file created FRESH in the share inherits it, but ANY mode
    change — os.chmod, or shutil.move/copy2's copystat — strips it to plain POSIX mode
    and strands the file. So we copy DATA ONLY (shutil.copyfile) + unlink the source,
    set owner with chown (which preserves the ACL), and deliberately NEVER chmod.
    """
    safe_artist = sanitize_filename(artist.split(",")[0].strip()) or "Unknown Artist"
    safe_title = sanitize_filename(title) or "Unknown Title"
    dest_dir = os.path.join(MUSIC_LIBRARY_PATH, safe_artist)
    os.makedirs(dest_dir, exist_ok=True)
    ext = os.path.splitext(src_path)[1] or ".flac"
    dest = os.path.join(dest_dir, f"{safe_artist} - {safe_title}{ext}")
    if os.path.exists(dest):
        base, extension = os.path.splitext(dest)
        dest = f"{base}_slsk{extension}"
    shutil.copyfile(src_path, dest)  # data only — dest inherits the share ACL
    try:
        os.remove(src_path)
    except OSError:
        pass
    uid = int(get_config(db_path, "plex_uid") or "1000")
    gid = int(get_config(db_path, "plex_gid") or "1000")
    try:
        os.chown(dest_dir, uid, gid)
        os.chown(dest, uid, gid)  # chown preserves the ACL; never chmod (would strip it)
    except OSError as e:
        log.warning("could not chown %s: %s", dest, e)
    return dest


def _process_one(db_path: str, track: dict, client: SlskdClient) -> None:
    track_id = track["id"]
    fa_id = track["_fa_id"]
    artist = track.get("artist", "") or ""
    title = track.get("title", "") or ""
    duration_ms = track.get("duration_ms") or 0
    query_used = f"{artist} {title}".strip()

    if not client.is_logged_in():
        log.warning("slskd not logged in — leaving track %d queued for next cycle", track_id)
        return  # transient: try again next cycle (leave the queued row in place)

    # search across query variants until we have candidates
    responses = []
    for q in _build_queries(artist, title):
        query_used = q
        responses = client.search(q)
        if rank_candidates(responses, duration_ms):
            break
    cands = rank_candidates(responses, duration_ms)

    if not cands:
        _finalize(db_path, fa_id, "no_candidates", 0)
        update_track(db_path, track_id, pipeline_stage="error",
                     pipeline_error="Soulseek: no lossless FLAC candidates found")
        log_activity(db_path, "soulseek_no_candidates", track_id,
                     f"No FLAC candidates for {artist} - {title}")
        log.info("Track %d: no soulseek FLAC candidates", track_id)
        return

    tmpdir = tempfile.mkdtemp(prefix="slsk_")
    try:
        tried = 0
        for c in cands[:MAX_CANDIDATES]:
            tried += 1
            log.info("Track %d: trying peer %s (%s)", track_id, c["username"][:16],
                     c["filename"].replace("\\", "/").split("/")[-1][:50])
            try:
                ok = client.download_and_wait(
                    c["username"], c["filename"], c["size"], timeout_s=PER_PEER_TIMEOUT_S
                )
            except Exception as e:  # noqa: BLE001
                log.warning("Track %d: download error from %s: %s", track_id, c["username"][:16], e)
                continue
            if not ok:
                continue

            relpath = client.ondisk_relpath(c["filename"])
            local = os.path.join(tmpdir, os.path.basename(relpath))
            try:
                got = client.fetch_file(relpath, local)
            except Exception as e:  # noqa: BLE001
                log.warning("Track %d: fetch failed for %s: %s", track_id, relpath, e)
                continue
            if got == 0:
                continue

            gate = verify_lossless(local, expected_duration_ms=duration_ms)
            log.info("Track %d: verify gate for %s -> passed=%s reasons=%s",
                     track_id, c["username"][:16], gate["passed"], gate["reasons"])
            if not gate["passed"]:
                log_activity(db_path, "soulseek_verify_fail", track_id,
                             f"Rejected fake/lossy from {c['username'][:16]}: "
                             f"{'; '.join(gate['reasons'])}", gate.get("checks"))
                try:
                    os.remove(local)
                except OSError:
                    pass
                continue

            # PASS — file into library and hand to the normal verify/import path
            dest = _move_into_library(db_path, local, artist, title)
            _finalize(db_path, fa_id, "success", len(cands))
            update_track(
                db_path, track_id,
                download_status="complete",
                download_source="soulseek",
                match_source="soulseek",
                file_path=dest,
                pipeline_stage="verifying",
                verify_status="pending",
                pipeline_error=None,
            )
            log_activity(
                db_path, "soulseek_success", track_id,
                f"Verified lossless FLAC sourced via Soulseek from {c['username'][:16]}: {dest}",
                {"peer": c["username"], "dest": dest, "spectral": gate["checks"].get("spectral"),
                 "spectral_verdict": gate["checks"].get("spectral_verdict")},
            )
            log.info("Track %d: SUCCESS — verified lossless via Soulseek -> %s", track_id, dest)
            return

        # exhausted candidates without a verified-lossless pass
        _finalize(db_path, fa_id, "all_failed", len(cands))
        update_track(db_path, track_id, pipeline_stage="error",
                     pipeline_error=f"Soulseek: tried {tried} peer(s), none delivered a verified-lossless FLAC")
        log_activity(db_path, "soulseek_all_failed", track_id,
                     f"{tried} peer(s) tried, none passed the lossless gate")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def process_soulseek_fallback(db_path: str) -> None:
    """One pipeline cycle of the Soulseek fallback stage."""
    if not is_enabled(db_path):
        return
    # scan mode is read-only; never source/import in scan mode
    if (get_config(db_path, "sync_mode") or "scan") == "scan":
        return
    superseded = _supersede_stale_queue(db_path)
    if superseded:
        log.info("Soulseek: superseded %d queued row(s) whose track left 'error'", superseded)
    tracks = _queued_tracks(db_path, BATCH)
    if not tracks:
        return
    client = build_client(db_path)
    if not client.configured:
        log.warning("slskd not configured (slskd_url/slskd_api_key) — cannot run fallback")
        return
    for track in tracks:
        try:
            _process_one(db_path, track, client)
        except Exception as e:  # noqa: BLE001
            log.error("Soulseek fallback error for track %d: %s", track["id"], e, exc_info=True)
            _finalize(db_path, track["_fa_id"], "error", 0, str(e)[:300])
            update_track(db_path, track["id"], pipeline_stage="error",
                         pipeline_error=f"Soulseek fallback error: {e}")
            log_activity(db_path, "soulseek_error", track["id"], f"Fallback failed: {e}")
