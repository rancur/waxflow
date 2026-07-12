"""Soulseek (slskd) fallback stage — a LOSSLESS-VERIFIED alternative to Tidal.

When Tidal (tiddl) cannot deliver a genuinely-lossless copy of a liked track —
either there is no Tidal match, or the Tidal copy is lossy AAC and fails the verify
stage — the track is routed here (pipeline_stage='soulseek_fallback'). This stage:

  1. searches slskd for the track (artist + title),
  2. ranks true-.flac candidates and tries them best-first (multi-peer, because the
     VPN has no forwarded port so some peers can never connect),
  3. downloads the first that transfers, fetches the bytes to the worker,
  4. runs the lossless_verify gate (codec/bits/sr + clean decode + spectral
     transcode/fake-FLAC detection + duration match),
  5. on PASS: files the file into the library exactly like a Tidal download
     (/music/<Artist>/<Artist> - <Title>.flac, chowned) and hands it to the normal
     'verifying' -> 'organizing' import path,
  6. on FAIL / no lossless candidate: records the attempt and terminates the track
     at 'error' (never imports a fake).

Everything is behind the app_config flag ``soulseek_fallback_enabled`` (default on).
slskd endpoint/credentials come from env (see slskd_client). All slskd P2P egress is
through the sabnzbd VPN on pi-dl; this module only speaks the LAN REST API + file
server.
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
    get_tracks_by_stage,
    log_activity,
    sanitize_filename,
    update_track,
)
from tasks.lossless_verify import verify_lossless
from tasks.slskd_client import SlskdClient

log = logging.getLogger("worker.soulseek_fallback")

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
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM fallback_attempts WHERE track_id = ? AND source = 'soulseek' LIMIT 1",
            (track_id,),
        ).fetchone()
    return row is not None


def _record_attempt(db_path: str, track_id: int, status: str, query: str,
                    result_count: int = 0, error: str | None = None):
    with get_db(db_path) as conn:
        conn.execute(
            """INSERT INTO fallback_attempts
               (track_id, source, status, error, search_query, result_count)
               VALUES (?, 'soulseek', ?, ?, ?, ?)""",
            (track_id, status, error, query, result_count),
        )


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
    """Place a verified file into the library, mirroring the tiddl download path."""
    safe_artist = sanitize_filename(artist.split(",")[0].strip()) or "Unknown Artist"
    safe_title = sanitize_filename(title) or "Unknown Title"
    dest_dir = os.path.join(MUSIC_LIBRARY_PATH, safe_artist)
    os.makedirs(dest_dir, exist_ok=True)
    ext = os.path.splitext(src_path)[1] or ".flac"
    dest = os.path.join(dest_dir, f"{safe_artist} - {safe_title}{ext}")
    if os.path.exists(dest):
        base, extension = os.path.splitext(dest)
        dest = f"{base}_slsk{extension}"
    shutil.move(src_path, dest)
    uid = int(get_config(db_path, "plex_uid") or "1000")
    gid = int(get_config(db_path, "plex_gid") or "1000")
    try:
        os.chown(dest_dir, uid, gid)
        os.chown(dest, uid, gid)
        os.chmod(dest, 0o664)
        os.chmod(dest_dir, 0o775)
    except OSError as e:
        log.warning("could not chown %s: %s", dest, e)
    return dest


def _process_one(db_path: str, track: dict, client: SlskdClient) -> None:
    track_id = track["id"]
    artist = track.get("artist", "") or ""
    title = track.get("title", "") or ""
    duration_ms = track.get("duration_ms") or 0
    query_used = f"{artist} {title}".strip()

    if not client.is_logged_in():
        log.warning("slskd not logged in — leaving track %d at %s for next cycle", track_id, STAGE)
        return  # transient: try again next cycle (do NOT burn the attempt)

    # search across query variants until we have candidates
    responses = []
    for q in _build_queries(artist, title):
        query_used = q
        responses = client.search(q)
        cands = rank_candidates(responses, duration_ms)
        if cands:
            break
    cands = rank_candidates(responses, duration_ms)

    if not cands:
        _record_attempt(db_path, track_id, "no_candidates", query_used, 0)
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
            _record_attempt(db_path, track_id, "success", query_used, len(cands))
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
        _record_attempt(db_path, track_id, "all_failed", query_used, len(cands))
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
    tracks = get_tracks_by_stage(db_path, STAGE, limit=BATCH)
    if not tracks:
        return
    client = build_client(db_path)
    if not client.configured:
        log.warning("slskd not configured (SLSKD_URL/SLSKD_API_KEY) — cannot run fallback")
        return
    for track in tracks:
        try:
            _process_one(db_path, track, client)
        except Exception as e:  # noqa: BLE001
            log.error("Soulseek fallback error for track %d: %s", track["id"], e, exc_info=True)
            update_track(db_path, track["id"], pipeline_stage="error",
                         pipeline_error=f"Soulseek fallback error: {e}")
            log_activity(db_path, "soulseek_error", track["id"], f"Fallback failed: {e}")
