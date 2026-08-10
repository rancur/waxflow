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
import re
import shutil
import tempfile

from tasks.helpers import (
    MUSIC_LIBRARY_PATH,
    get_config,
    get_db,
    log_activity,
    sanitize_filename,
    set_config,
    update_track,
)
from tasks.lossless_verify import verify_lossless
from tasks.slskd_client import SlskdClient

log = logging.getLogger("worker.soulseek_fallback")

# Retained for backwards-compat / logging only. NOT written to tracks.pipeline_stage
# (that column is CHECK-constrained); the queue lives in fallback_attempts instead.
STAGE = "soulseek_fallback"
BATCH = 3                      # tracks per cycle (each may do several peer downloads)
MAX_CANDIDATES = 10          # peers/candidates to try before giving up on a track
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


def reject_nonlossless_for_import(db_path: str, track: dict) -> bool:
    """Import-gate guard: REFUSE to import any file that is not genuinely lossless.

    This is a hard, path-independent guarantee protecting Will's lossless standard.
    The organizing stage is the single chokepoint that writes files into Lexicon, but
    tracks can reach it WITHOUT a passing lossless verify (observed live: verify-failed
    lossy AAC tracks were re-imported to 'complete' during concurrent recovery, and the
    self-heal 're-queue complete-but-unlinked' path resets tracks straight to
    'organizing'). This guard re-checks the actual file at the import gate and, if it
    is not a genuinely-lossless container, refuses the import and routes the track to
    the Soulseek fallback instead (queued for a verified-lossless FLAC).

    Returns True if the track was REJECTED (caller must skip importing it), else False.
    """
    file_path = track.get("file_path")
    # Nothing on disk to mis-import (pure Lexicon link, or mount momentarily down):
    # let the normal organizing path handle it — do not block.
    if not file_path or not os.path.exists(file_path):
        return False
    # Trust a prior genuine-lossless verdict (fast path for the common case).
    if track.get("verify_is_genuine_lossless") == 1:
        return False
    # Definitive check: probe the actual file and score it.
    #
    # BEHAVIOUR CHANGE (2.16.0). This used to be lossless-or-nothing, so a 320 kbps
    # file of a track available nowhere else was refused and the track simply stayed
    # missing. For a DJ library that is the wrong trade: a 320k file you can play
    # beats a perfect file you do not have.
    #
    # Now three outcomes rather than two:
    #   at/above target (lossless)   -> import, as before
    #   floor..target (>=320k lossy) -> IMPORT, flagged below_target, and queued for
    #                                   an upgrade so the hunt continues
    #   below the floor              -> refused, exactly as before
    #
    # Set quality_floor_tier to "lossless" to restore the old behaviour exactly.
    track_id = track["id"]
    try:
        from tasks import quality
        score = quality.score_file(file_path)
    except Exception as e:  # noqa: BLE001
        # A probe failure used to mean "allow". With a lower floor that is the hole
        # garbage would come through, so hold the track and let the next cycle retry
        # rather than importing something we could not read.
        log.warning("import-guard probe failed for %s: %s — holding", file_path, e)
        return _hold_unprobeable(db_path, track_id, file_path, e)

    floor_tier = _configured_floor_tier(db_path)

    if score.tier >= quality.TARGET_TIER:
        return False                                    # genuinely lossless — allow

    if score.tier >= floor_tier:
        # Good enough to use now, not good enough to stop looking.
        update_track(
            db_path, track_id,
            quality_tier=score.tier_name, quality_score=score.score,
            quality_bit_rate=score.bit_rate, below_target=1,
            quality_checked_at=_now_iso(),
        )
        if is_enabled(db_path) and not already_attempted(db_path, track_id):
            queue_for_fallback(
                db_path, track_id,
                f"below-target import ({score.tier_name}) — looking for lossless")
        log_activity(
            db_path, "import_below_target", track_id,
            f"Imported {score.tier_name} ({score.codec} {score.bit_rate // 1000}k) — "
            f"still hunting for lossless",
            {"file_path": file_path, **score.as_dict()})
        log.info("Track %d: importing below-target %s (%s) — upgrade queued",
                 track_id, score.tier_name, os.path.basename(file_path))
        return False

    # Below the floor — refuse, exactly as before.
    reason = (f"refused import below the quality floor "
              f"(codec={score.codec}, {score.bit_rate // 1000}k) of "
              f"{os.path.basename(file_path)}")
    if is_enabled(db_path) and not already_attempted(db_path, track_id):
        queue_for_fallback(db_path, track_id, reason)
    update_track(
        db_path, track_id,
        pipeline_stage="error", verify_status="fail", verify_is_genuine_lossless=0,
        quality_tier=score.tier_name, quality_score=score.score,
        quality_bit_rate=score.bit_rate, quality_checked_at=_now_iso(),
        pipeline_error=f"{reason} [queued for Soulseek lossless fallback]",
    )
    log_activity(db_path, "import_rejected_nonlossless", track_id, reason,
                 {"file_path": file_path, **score.as_dict()})
    log.warning("Track %d: REFUSED below-floor import (%s) -> routed to Soulseek",
                track_id, file_path)
    return True


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _configured_floor_tier(db_path: str) -> int:
    """The lowest tier we will import, from config. Defaults to 320k."""
    from tasks import quality
    name = (get_config(db_path, "quality_floor_tier") or "").strip().lower()
    by_name = {v: k for k, v in quality.TIER_NAMES.items()}
    return by_name.get(name, quality.FLOOR_TIER)


_UNPROBEABLE_ATTEMPT_CAP = 5


def _hold_unprobeable(db_path: str, track_id: int, file_path: str, err) -> bool:
    """Leave an unreadable file for the next cycle; error out after a few tries.

    Returning True here means "do not import", NOT "this is garbage" -- the track
    stays in organizing and is retried, so a transient read (mount blip, file still
    being written) resolves itself.
    """
    key = f"_unprobeable_attempts_{track_id}"
    try:
        attempts = int(get_config(db_path, key) or 0) + 1
    except (TypeError, ValueError):
        attempts = 1
    set_config(db_path, key, str(attempts))
    if attempts >= _UNPROBEABLE_ATTEMPT_CAP:
        update_track(
            db_path, track_id, pipeline_stage="error",
            pipeline_error=f"could not probe {os.path.basename(file_path)} after "
                           f"{attempts} attempts: {err}")
        log.warning("Track %d: unprobeable after %d attempts — erroring", track_id, attempts)
    return True


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


def _expected_size_range(duration_ms: int | None, min_kbps: int = 350,
                         max_kbps: int = 1500):
    """Plausible byte range for a file of this duration at the given bitrate band.

    Defaults describe a lossless FLAC (~350-1500 kbps). A 320k MP3 is roughly a
    quarter the size, so searching for one with the lossless band would reject every
    real result -- the band has to move with the tier being searched for.
    """
    if not duration_ms:
        return (1_000_000, 200_000_000)
    secs = duration_ms / 1000.0
    lo = int(secs * min_kbps * 1000 / 8 * 0.7)
    hi = int(secs * max_kbps * 1000 / 8 * 1.6)
    return (max(500_000, lo), max(hi, 30_000_000))


# File extensions we will consider, and the best tier each can represent.
_LOSSLESS_EXTS = (".flac", ".aiff", ".aif", ".wav", ".alac", ".ape", ".wv")
_LOSSY_EXTS = (".mp3", ".m4a", ".aac", ".ogg", ".opus")

# Bitrate band per tier, used both to size-filter candidates and to sanity-check
# what a peer claims.
_TIER_KBPS = {
    "hi-res": (1500, 9216),
    "24-bit": (700, 4608),
    "lossless": (350, 1500),
    "320k": (256, 400),
}


def _ext_of(filename: str) -> str:
    name = (filename or "").replace("\\", "/").split("/")[-1].lower()
    dot = name.rfind(".")
    return name[dot:] if dot >= 0 else ""


def estimate_tier(filename: str, size: int, duration_ms: int | None,
                  attrs: dict | None = None) -> int:
    """Best guess at a candidate's tier BEFORE downloading it.

    slskd sometimes reports bitRate/bitDepth/sampleRate per file; when it does that
    is far better than guessing. Otherwise fall back to the extension plus the
    bitrate implied by size and duration. This is only a pre-filter -- everything is
    still probed and scored for real after download.
    """
    from tasks import quality
    attrs = attrs or {}
    ext = _ext_of(filename)
    lossless = ext in _LOSSLESS_EXTS

    sample_rate = int(attrs.get("sampleRate") or 0)
    bit_depth = int(attrs.get("bitDepth") or 0)
    bit_rate = int(attrs.get("bitRate") or 0) * 1000

    if not bit_rate and size and duration_ms:
        bit_rate = int(size * 8 / (duration_ms / 1000.0))

    if lossless:
        if sample_rate > 48_000:
            return quality.TIER_HIRES
        if bit_depth >= 24:
            return quality.TIER_24BIT
        # No attributes: infer from the implied bitrate. A 24-bit/96k file is far
        # denser than CD-quality, so size is a usable proxy when nothing else is.
        if not sample_rate and not bit_depth and bit_rate:
            if bit_rate >= 1_500_000:
                return quality.TIER_HIRES
            if bit_rate >= 700_000:
                return quality.TIER_24BIT
        return quality.TIER_LOSSLESS

    if ext in _LOSSY_EXTS:
        floor = quality.LOSSY_FLOOR_BPS * (1 - quality.LOSSY_FLOOR_TOLERANCE)
        return quality.TIER_LOSSY_HIGH if bit_rate >= floor else quality.TIER_BELOW

    return quality.TIER_BELOW


_STOPWORDS = {"the", "a", "an", "of", "and", "&", "feat", "ft", "vs", "remix", "mix",
              "edit", "vip", "original", "extended", "radio"}


def _tokens(*parts: str) -> set[str]:
    out = set()
    for p in parts:
        for w in re.split(r"[^0-9a-z]+", (p or "").lower()):
            if len(w) >= 3 and w not in _STOPWORDS:
                out.add(w)
    return out


def rank_candidates(responses: list[dict], duration_ms: int | None,
                    artist: str = "", title: str = "",
                    tier: int | None = None) -> list[dict]:
    """Flatten peer responses to ranked candidates (best prospect first).

    RELEVANCE FIRST: on generic titles (e.g. "The Wave") a search returns thousands
    of unrelated files; ranking purely by peer speed surfaces the wrong track. So we
    score each candidate by how many artist/title tokens appear in its filename and
    rank by relevance BEFORE availability (free slot / queue / speed). The duration
    gate in verify_lossless then rejects any wrong-length version that slips through.

    `tier` selects WHICH quality we are hunting for on this pass. The caller walks
    the ladder downwards -- hi-res, then 24-bit, then lossless, then 320k -- so the
    best available copy wins rather than whatever turns up first. Passing None keeps
    the original behaviour exactly: .flac only, lossless size band.
    """
    from tasks import quality
    want_artist = _tokens(artist)
    want_title = _tokens(title)
    want = want_artist | want_title

    if tier is None:
        allowed_exts, lo, hi = (".flac",), *_expected_size_range(duration_ms)
    else:
        name = quality.TIER_NAMES.get(tier, "lossless")
        min_kbps, max_kbps = _TIER_KBPS.get(name, (350, 1500))
        lo, hi = _expected_size_range(duration_ms, min_kbps, max_kbps)
        allowed_exts = _LOSSY_EXTS if tier == quality.TIER_LOSSY_HIGH else _LOSSLESS_EXTS

    cands = []
    for r in responses:
        for f in r.get("files", []):
            fn = f.get("filename", "")
            if _ext_of(fn) not in allowed_exts:
                continue
            size = int(f.get("size") or 0)
            if not (lo <= size <= hi):
                continue
            if tier is not None:
                # Only consider files that plausibly ARE the tier being hunted.
                # Everything is still probed for real after download; this just
                # stops us spending a transfer on something obviously wrong.
                attrs = f.get("attributes") or {}
                if estimate_tier(fn, size, duration_ms, attrs) < tier:
                    continue
            fn_tokens = _tokens(fn.replace("\\", "/").split("/")[-1])
            a = len(want_artist & fn_tokens)
            t = len(want_title & fn_tokens)
            cands.append({
                "username": r["username"],
                "filename": fn,
                "size": size,
                "free": bool(r.get("hasFreeUploadSlot")),
                "queue": int(r.get("queueLength") or 9999),
                "speed": int(r.get("uploadSpeed") or 0),
                "relevance": a * 2 + t,   # weight artist match higher than title
                "artist_match": a > 0,
            })
    # Correctness first: if the ARTIST appears in any candidate filename, keep only
    # those (drops wrong-artist tracks that merely share a common title word — e.g.
    # "Paper Labyrinth" vs "Mob Tactics - Labyrinth"). Otherwise fall back to any
    # title-token match, else keep all. Then rank by relevance / availability.
    if want_artist and any(c["artist_match"] for c in cands):
        cands = [c for c in cands if c["artist_match"]]
    elif want and any(c["relevance"] > 0 for c in cands):
        cands = [c for c in cands if c["relevance"] > 0]
    cands.sort(key=lambda c: (-c["relevance"], not c["free"], c["queue"], -c["speed"]))
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


def download_candidate(client: SlskdClient, cand: dict, tmpdir: str,
                       timeout_s: float = PER_PEER_TIMEOUT_S) -> str | None:
    """Transfer one candidate from a peer and fetch the bytes locally.

    Returns the local path, or None if the peer never delivered. Peers frequently
    fail to connect at all (the VPN has no forwarded port), so a None here is
    routine and the caller should simply try the next candidate.
    """
    try:
        if not client.download_and_wait(cand["username"], cand["filename"],
                                        cand["size"], timeout_s=timeout_s):
            return None
    except Exception as e:  # noqa: BLE001
        log.warning("download error from %s: %s", cand["username"][:16], e)
        return None

    relpath = client.ondisk_relpath(cand["filename"])
    local = os.path.join(tmpdir, os.path.basename(relpath))
    try:
        if client.fetch_file(relpath, local) == 0:
            return None
    except Exception as e:  # noqa: BLE001
        log.warning("fetch failed for %s: %s", relpath, e)
        return None
    return local


def active_profile(db_path: str) -> dict:
    """The configured quality profile (floor / cutoff / target)."""
    from tasks import quality
    return quality.resolve_profile(lambda k: get_config(db_path, k))


def search_best_available(client: SlskdClient, artist: str, title: str,
                          duration_ms: int, profile: dict,
                          min_tier: int | None = None) -> tuple:
    """Walk the quality ladder downwards and return the best tier with candidates.

    This is what "ask for the absolute best, then work down" actually means: try
    hi-res first, then 24-bit, then lossless, then 320k, and stop at the first tier
    that yields anything plausible. Without it the search is all-or-nothing and a
    24-bit copy is indistinguishable from a CD rip.

    `min_tier` stops the walk early -- an upgrade hunt has no reason to look at
    tiers at or below what the track already has.

    Returns (tier, candidates, query_used); tier is None when nothing was found.
    """
    from tasks import quality
    queries = _build_queries(artist, title)
    ladder = [t for t in quality.search_ladder(profile)
              if min_tier is None or t > min_tier]

    # Search once per query and re-filter per tier: the searches are the slow part,
    # and re-ranking a cached response set is free.
    seen: list[tuple] = []
    for q in queries:
        responses = client.search(q)
        if not responses:
            continue
        seen.append((q, responses))
        for tier in ladder:
            cands = rank_candidates(responses, duration_ms, artist, title, tier=tier)
            if cands:
                log.info("soulseek: %s - %s -> %d candidate(s) at %s",
                         artist, title, len(cands), quality.TIER_NAMES[tier])
                return tier, cands, q

    # Nothing matched any tier in the profile.
    return None, [], (seen[-1][0] if seen else f"{artist} {title}".strip())


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

    # Walk the quality ladder: best tier first, working down to the floor.
    profile = active_profile(db_path)
    found_tier, cands, query_used = search_best_available(
        client, artist, title, duration_ms, profile)

    if not cands:
        _finalize(db_path, fa_id, "no_candidates", 0)
        update_track(db_path, track_id, pipeline_stage="error",
                     pipeline_error="Soulseek: no candidates found at any accepted quality")
        log_activity(db_path, "soulseek_no_candidates", track_id,
                     f"No candidates for {artist} - {title}")
        log.info("Track %d: no soulseek candidates at any tier", track_id)
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
