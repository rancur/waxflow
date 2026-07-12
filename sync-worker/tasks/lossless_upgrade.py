"""Lossy-only auto-upgrade re-check.

Some liked tracks are kept as a LOSSY copy because, at import time, no genuinely
lossless copy existed anywhere — Tidal only offered lossy AAC and Soulseek had no
FLAC candidate (known examples: "Mob Tactics - Labyrinth", "Annix x Mefjus - Shai
Hulud VIP"). Will's standard is lossless everywhere it is obtainable, so rather than
leave those tracks lossy forever we KEEP the lossy as a placeholder and periodically
RE-CHECK whether a genuinely-lossless copy has since appeared. If one has, we source
+ verify it (through the EXISTING Tidal-lossless + Soulseek-fallback + lossless_verify
gate) and swap it in. If nothing lossless is found we leave the lossy exactly where it
is and try again next interval.

HARD GUARANTEE — never leave Will with neither: a lossy track is NEVER removed or
demoted unless a genuinely-lossless replacement has been sourced, verified by the
lossless gate, AND confirmably installed in Lexicon. On any failure we keep the lossy.

Design (kept deliberately small + NAS-friendly):

  * marker      — two lightweight columns on ``tracks``:
                  ``lossless_upgrade_pending`` (0/1) and ``last_upgrade_check`` (ISO ts).
                  Added idempotently by ensure_schema(); mirrored in init_db.py.
  * detection   — mark_pending() flags ``complete`` tracks that are NOT genuinely
                  lossless (verified lossy, or a plainly lossy file extension) and were
                  therefore kept as a placeholder. Conservative: a track whose file
                  looks lossless is never marked, and is_protected tracks are skipped.
  * throttle    — a track is re-checked at most once every N days (default 7) via
                  ``last_upgrade_check``; each cycle processes a small bounded batch
                  (default 2). Off in scan mode and behind ``lossless_upgrade_enabled``.
  * swap        — on a verified-lossless source, relocate the EXISTING Lexicon track to
                  the new file in place (self-verified) and clear the marker. If the
                  relocate cannot be confirmed, discard the freshly-sourced copy, keep
                  the lossy, and retry next interval (never a false "upgraded" state).

This never weakens the lossless gate or the dedup guards: it reuses lossless_verify for
every candidate and performs an in-place relocate of the SAME Lexicon track id (no new
track is ever created here, so duplicates are impossible).
"""

from __future__ import annotations

import logging
import os

from tasks.helpers import (
    MUSIC_LIBRARY_PATH,
    get_config,
    get_db,
    log_activity,
    set_config,
    update_track,
)
from tasks.lossless_verify import verify_lossless

log = logging.getLogger("worker.lossless_upgrade")

# Config keys (app_config) + defaults.
CFG_ENABLED = "lossless_upgrade_enabled"        # default on
CFG_INTERVAL_DAYS = "lossless_upgrade_interval_days"  # per-track re-check throttle
CFG_BATCH = "lossless_upgrade_batch"            # tracks re-checked per cycle
DEFAULT_INTERVAL_DAYS = 7
DEFAULT_BATCH = 2

# File extensions that are unambiguously LOSSY. A track kept with one of these is a
# placeholder eligible for upgrade. (Anything not in the lossless set that also is not
# clearly lossy is left alone — we only ever act on a clearly-lossy file or an explicit
# verified-lossy verdict, never on an unknown.)
LOSSY_EXTENSIONS = {".m4a", ".aac", ".mp3", ".ogg", ".opus", ".wma", ".m4b", ".wav.mp3"}


# --------------------------------------------------------------------------- config
def is_enabled(db_path: str) -> bool:
    val = get_config(db_path, CFG_ENABLED)
    if val is None:
        return True  # default on
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _interval_days(db_path: str) -> int:
    try:
        return max(1, int(get_config(db_path, CFG_INTERVAL_DAYS) or DEFAULT_INTERVAL_DAYS))
    except (TypeError, ValueError):
        return DEFAULT_INTERVAL_DAYS


def _batch(db_path: str) -> int:
    try:
        return max(1, int(get_config(db_path, CFG_BATCH) or DEFAULT_BATCH))
    except (TypeError, ValueError):
        return DEFAULT_BATCH


# --------------------------------------------------------------------------- schema
def ensure_schema(db_path: str) -> None:
    """Idempotently add the two marker columns to ``tracks``.

    SQLite ``ADD COLUMN`` is cheap and non-locking for a nullable/defaulted column, so
    this is safe to call on every worker cycle. Guarded by a read of table_info so it
    only fires once. No CHECK-constraint change is needed (no table rebuild).
    """
    with get_db(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(tracks)").fetchall()}
        if "lossless_upgrade_pending" not in cols:
            conn.execute(
                "ALTER TABLE tracks ADD COLUMN lossless_upgrade_pending INTEGER NOT NULL DEFAULT 0"
            )
            log.info("schema: added tracks.lossless_upgrade_pending")
        if "last_upgrade_check" not in cols:
            conn.execute("ALTER TABLE tracks ADD COLUMN last_upgrade_check TEXT")
            log.info("schema: added tracks.last_upgrade_check")


# ------------------------------------------------------------------------- detection
def _is_lossy_path(path: str | None) -> bool:
    if not path:
        return False
    return os.path.splitext(path)[1].lower() in LOSSY_EXTENSIONS


def mark_pending(db_path: str) -> int:
    """Flag complete-but-lossy tracks for periodic upgrade re-check.

    Eligible = pipeline_stage 'complete', not is_protected, not already pending, and
    NOT genuinely lossless — where "not lossless" is a *conservative* signal:
      * verify_is_genuine_lossless = 0 (we verified it and it is lossy), OR
      * the kept file has a plainly-lossy extension.
    A track whose file looks lossless is never marked. Returns the number newly marked.
    """
    marked = 0
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT id, file_path, verify_is_genuine_lossless
                 FROM tracks
                WHERE pipeline_stage = 'complete'
                  AND COALESCE(is_protected, 0) = 0
                  AND COALESCE(lossless_upgrade_pending, 0) = 0"""
        ).fetchall()
        for r in rows:
            verified_lossy = r["verify_is_genuine_lossless"] == 0
            if verified_lossy or _is_lossy_path(r["file_path"]):
                conn.execute(
                    "UPDATE tracks SET lossless_upgrade_pending = 1 WHERE id = ?",
                    (r["id"],),
                )
                marked += 1
    if marked:
        log.info("lossless_upgrade: marked %d lossy-kept track(s) for re-check", marked)
    return marked


def due_tracks(db_path: str, limit: int) -> list[dict]:
    """Pending tracks whose per-track throttle has elapsed, least-recently-checked first."""
    interval = f"-{_interval_days(db_path)} days"
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM tracks
                WHERE COALESCE(lossless_upgrade_pending, 0) = 1
                  AND pipeline_stage = 'complete'
                  AND (last_upgrade_check IS NULL
                       OR last_upgrade_check < datetime('now', ?))
                ORDER BY (last_upgrade_check IS NOT NULL), last_upgrade_check ASC,
                         created_at ASC
                LIMIT ?""",
            (interval, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def _touch_check(db_path: str, track_id: int) -> None:
    """Record that a re-check ran now (advances the throttle) without changing state."""
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE tracks SET last_upgrade_check = datetime('now') WHERE id = ?",
            (track_id,),
        )


# -------------------------------------------------------------------------- sourcing
def _source_via_tidal(db_path: str, track: dict) -> str | None:
    """Fresh Tidal-lossless attempt: re-search Tidal, download the best match, and keep
    it ONLY if it passes the lossless gate. Returns the library path or None.

    Reuses the pipeline's own Tidal search + tiddl download so there is a single
    download/placement path. Imported lazily so this module stays importable (and
    testable) without the pipeline's heavy deps.
    """
    try:
        from tasks.process_pipeline import (
            _tidal_search,
            _download_track_via_tiddl,
            _titles_match,
            _artists_match,
        )
    except Exception as e:  # noqa: BLE001
        log.debug("tidal source unavailable (import): %s", e)
        return None

    artist = (track.get("artist") or "").strip()
    title = (track.get("title") or "").strip()
    if not artist or not title:
        return None
    dur_ms = track.get("duration_ms") or 0

    try:
        items = _tidal_search(f"{artist} {title}")
    except Exception as e:  # noqa: BLE001
        log.debug("tidal search failed for %s - %s: %s", artist, title, e)
        return None

    best = None
    for it in items or []:
        it_title = it.get("title") or ""
        it_artist = (it.get("artist") or {}).get("name") or ""
        if not it_artist and it.get("artists"):
            it_artist = it["artists"][0].get("name", "")
        if not (_titles_match(title, it_title) and _artists_match(artist, it_artist)):
            continue
        # duration sanity (Tidal 'duration' is seconds)
        if dur_ms and it.get("duration"):
            if abs(float(it["duration"]) - dur_ms / 1000.0) > 5:
                continue
        # prefer a genuinely-lossless quality tier
        quality = (it.get("audioQuality") or "").upper()
        score = 2 if quality in ("LOSSLESS", "HI_RES", "HI_RES_LOSSLESS") else 0
        if best is None or score > best[0]:
            best = (score, it)
    if not best:
        return None

    shadow = dict(track)
    shadow["tidal_id"] = best[1].get("id")
    if not shadow["tidal_id"]:
        return None
    try:
        dest = _download_track_via_tiddl(db_path, shadow)
    except Exception as e:  # noqa: BLE001
        log.info("tidal upgrade download failed for %s - %s: %s", artist, title, e)
        return None

    gate = verify_lossless(dest, expected_duration_ms=dur_ms)
    if gate["passed"]:
        log.info("lossless_upgrade: Tidal delivered verified-lossless for %s - %s", artist, title)
        return dest
    # Tidal still only had a lossy/mismatched copy — discard it (keep the placeholder lossy).
    _safe_remove(dest)
    return None


def _source_via_soulseek(db_path: str, track: dict) -> str | None:
    """Fresh Soulseek attempt, gated by lossless_verify. Returns the library path or None.

    Reuses the fallback module's ranking, query building, verify gate and ACL-safe
    placement — the same verified-sourcing path as the live fallback — without touching
    the fallback_attempts queue or the track's state (this module owns the swap).
    """
    try:
        from tasks import soulseek_fallback as sf
    except Exception as e:  # noqa: BLE001
        log.debug("soulseek source unavailable (import): %s", e)
        return None

    if not sf.is_enabled(db_path):
        return None
    client = sf.build_client(db_path)
    if not client.configured or not client.is_logged_in():
        log.debug("slskd not available for upgrade re-check")
        return None

    artist = (track.get("artist") or "").strip()
    title = (track.get("title") or "").strip()
    dur_ms = track.get("duration_ms") or 0

    responses: list[dict] = []
    for q in sf._build_queries(artist, title):
        responses = client.search(q)
        if sf.rank_candidates(responses, dur_ms, artist, title):
            break
    cands = sf.rank_candidates(responses, dur_ms, artist, title)
    if not cands:
        return None

    import tempfile

    tmpdir = tempfile.mkdtemp(prefix="lslup_")
    try:
        for c in cands[: sf.MAX_CANDIDATES]:
            try:
                ok = client.download_and_wait(
                    c["username"], c["filename"], c["size"], timeout_s=sf.PER_PEER_TIMEOUT_S
                )
            except Exception:  # noqa: BLE001
                continue
            if not ok:
                continue
            relpath = client.ondisk_relpath(c["filename"])
            local = os.path.join(tmpdir, os.path.basename(relpath))
            try:
                if client.fetch_file(relpath, local) == 0:
                    continue
            except Exception:  # noqa: BLE001
                continue
            gate = verify_lossless(local, expected_duration_ms=dur_ms)
            if not gate["passed"]:
                _safe_remove(local)
                continue
            dest = sf._move_into_library(db_path, local, artist, title)
            log.info("lossless_upgrade: Soulseek delivered verified-lossless for %s - %s", artist, title)
            return dest
        return None
    finally:
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)


def _source_verified_lossless(db_path: str, track: dict) -> tuple[str, str] | None:
    """Try Tidal first, then Soulseek. Returns (library_path, source) or None."""
    dest = _source_via_tidal(db_path, track)
    if dest:
        return dest, "tidal"
    dest = _source_via_soulseek(db_path, track)
    if dest:
        return dest, "soulseek"
    return None


# ------------------------------------------------------------------------- Lexicon swap
def _relocate_in_lexicon(db_path: str, track: dict, dest: str) -> bool:
    """Point the EXISTING Lexicon track at the new lossless file, IN PLACE, and confirm.

    The pipeline has no relocate path (import find-or-import short-circuits on a known
    Lexicon id), so a genuine swap must move the existing track's ``location`` to the new
    file. We PATCH the same track id (never create a new track -> no duplicate) and then
    RE-READ it to confirm the location actually changed. Returns True only on a confirmed
    relocate; on anything uncertain we return False so the caller keeps the lossy and
    retries later (never a false "upgraded" state).
    """
    lexicon_track_id = str(track.get("lexicon_track_id") or "").strip()
    if not lexicon_track_id or lexicon_track_id.lower() in ("none", "null"):
        return False

    try:
        import httpx

        from tasks.helpers import LEXICON_API_URL
        from tasks.process_pipeline import _container_to_mac_path
    except Exception as e:  # noqa: BLE001
        log.debug("lexicon relocate unavailable (import): %s", e)
        return False

    lexicon_library_path = get_config(db_path, "lexicon_library_path") or "/music/library"
    lexicon_input_path = get_config(db_path, "lexicon_input_path") or "/music/downloads"
    lexicon_api = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    downloads_dir = get_config(db_path, "downloads_path") or os.environ.get("DOWNLOADS_PATH", "/downloads")
    mac_path = _container_to_mac_path(dest, lexicon_library_path, lexicon_input_path, downloads_dir)

    try:
        with httpx.Client(base_url=lexicon_api, timeout=60) as client:
            resp = client.patch(
                "/v1/track",
                json={"id": int(lexicon_track_id), "edits": {"location": mac_path}},
            )
            # Lexicon's v1 API rejects a ``location`` edit outright (HTTP 400
            # "'location' is not editable") and silently ignores unknown edit keys —
            # verified live against the running library. There is no /relocate or
            # /relink endpoint either. So an in-place file swap is not currently
            # expressible through the API. Detect that explicitly, persist a capability
            # flag so the re-check loop stops wastefully re-sourcing lossless copies it
            # cannot install, and fail closed (keep the lossy — never a false upgrade).
            body = (resp.text or "").lower()
            if resp.status_code >= 400 or "not editable" in body:
                set_config(db_path, "lexicon_relocate_capable", "0")
                log.warning(
                    "lossless_upgrade: Lexicon API cannot relocate track %s in place "
                    "(HTTP %s: %s) — no editable location / relocate endpoint. Keeping "
                    "lossy; will not re-source until Lexicon exposes a relocate path.",
                    lexicon_track_id, resp.status_code, (resp.text or "")[:120],
                )
                return False
            # Confirm the relocate actually took effect before trusting it.
            if _lexicon_location_is(client, lexicon_track_id, mac_path):
                set_config(db_path, "lexicon_relocate_capable", "1")
                return True
            return False
    except Exception as e:  # noqa: BLE001
        log.info("lexicon relocate failed for track id %s: %s", lexicon_track_id, e)
        return False


def _lexicon_location_is(client, lexicon_track_id: str, mac_path: str) -> bool:
    """Best-effort read-back: True iff Lexicon reports the track's location == mac_path.

    Lexicon's single-track read shape is not fully pinned here, so try a couple of
    endpoints and treat only an exact location match as confirmation.
    """
    for getter in (
        lambda: client.get(f"/v1/tracks/{lexicon_track_id}"),
        lambda: client.get("/v1/track", params={"id": int(lexicon_track_id)}),
        lambda: client.get("/v1/tracks", params={"id": int(lexicon_track_id)}),
    ):
        try:
            resp = getter()
            if resp.status_code != 200:
                continue
            data = resp.json()
            for t in _iter_tracks(data):
                if str(t.get("id")) == lexicon_track_id and t.get("location") == mac_path:
                    return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _iter_tracks(data):
    """Yield track dicts from the various Lexicon response envelope shapes.

    Live Lexicon (v1) single-track read ``GET /v1/track?id=<n>`` returns the SINGULAR
    envelope ``{"data": {"track": {...}}}`` — verified against the running library. The
    search endpoint returns the PLURAL ``{"data": {"tracks": [...]}}``. Both are handled
    here so the relocate read-back can confirm a track id/location either way.
    """
    if isinstance(data, dict):
        d = data.get("data", data)
        if isinstance(d, dict):
            tracks = d.get("tracks")
            if isinstance(tracks, list):
                yield from tracks
                return
            single = d.get("track")
            if isinstance(single, dict):
                yield single
                return
            if d.get("id") is not None:
                yield d
                return
        if isinstance(d, list):
            yield from d


def _safe_remove(path: str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        pass


# ----------------------------------------------------------------------- capability
def _lexicon_can_relocate(db_path: str, track: dict) -> bool:
    """Cheap probe: does this Lexicon accept a track ``location`` edit at all?

    Lexicon v1 currently answers ``PATCH /v1/track {edits:{location}}`` with HTTP 400
    "'location' is not editable" (verified live), and exposes no /relocate endpoint — so
    an in-place lossless swap is impossible and there is no point downloading a
    replacement we cannot install. This probe does a NO-OP location edit (the track's own
    current location) so it costs one GET + one PATCH and mutates nothing: HTTP 200 means
    the capability exists, HTTP 400 / "not editable" means it does not. The result is
    cached in ``lexicon_relocate_capable`` and the probe naturally re-enables the feature
    the day Lexicon starts accepting location edits.
    """
    lexicon_track_id = str(track.get("lexicon_track_id") or "").strip()
    if not lexicon_track_id or lexicon_track_id.lower() in ("none", "null"):
        return False
    try:
        import httpx

        from tasks.helpers import LEXICON_API_URL
    except Exception:  # noqa: BLE001
        return False
    lexicon_api = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    try:
        with httpx.Client(base_url=lexicon_api, timeout=30) as client:
            r = client.get("/v1/track", params={"id": int(lexicon_track_id)})
            if r.status_code != 200:
                return False
            current = None
            for t in _iter_tracks(r.json()):
                if str(t.get("id")) == lexicon_track_id:
                    current = t.get("location")
                    break
            if not current:
                return False
            pr = client.patch(
                "/v1/track",
                json={"id": int(lexicon_track_id), "edits": {"location": current}},
            )
            capable = pr.status_code < 400 and "not editable" not in (pr.text or "").lower()
            set_config(db_path, "lexicon_relocate_capable", "1" if capable else "0")
            return capable
    except Exception:  # noqa: BLE001
        return False


# ----------------------------------------------------------------------- orchestration
def _attempt_upgrade(db_path: str, track: dict) -> str:
    """Re-check one lossy-kept track. Returns 'upgraded' | 'staged' | 'none' | 'blocked'."""
    track_id = track["id"]
    artist = track.get("artist") or ""
    title = track.get("title") or ""

    # Do not burn Tidal/Soulseek bandwidth sourcing a lossless copy we cannot install:
    # if Lexicon cannot relocate a track's file in place, keep the lossy and move on. The
    # probe re-enables automatically if Lexicon ever gains an editable location.
    if not _lexicon_can_relocate(db_path, track):
        _touch_check(db_path, track_id)
        log.info(
            "lossless_upgrade: Lexicon relocate unavailable; kept lossy for %s - %s "
            "(deferred, not re-sourced)",
            artist, title,
        )
        return "blocked"

    sourced = _source_verified_lossless(db_path, track)
    if not sourced:
        _touch_check(db_path, track_id)
        log.info("lossless_upgrade: no lossless found for %s - %s; kept lossy", artist, title)
        return "none"

    dest, source = sourced
    # We now HAVE a verified-lossless file. Only commit the swap if Lexicon confirms the
    # in-place relocate; otherwise keep the lossy untouched and discard the new copy.
    if _relocate_in_lexicon(db_path, track, dest):
        update_track(
            db_path, track_id,
            file_path=dest,
            download_source=source,
            match_source="lossless_upgrade",
            verify_status="pass",
            verify_is_genuine_lossless=1,
            lossless_upgrade_pending=0,
            pipeline_error=None,
        )
        # update_track can't call datetime('now'); stamp the throttle timestamp here.
        _touch_check(db_path, track_id)
        log_activity(
            db_path, "lossless_upgraded", track_id,
            f"Upgraded to verified-lossless via {source}: {artist} - {title} -> {dest}",
            {"source": source, "dest": dest},
        )
        log.info("lossless_upgrade: UPGRADED %s - %s via %s -> %s", artist, title, source, dest)
        return "upgraded"

    # Verified-lossless obtained but the Lexicon relocate could not be confirmed. Never
    # leave a false-upgraded state or an orphaned copy: discard the new file, keep the
    # lossy + the pending marker, and retry next interval (e.g. after the coordinated
    # rebuild once Lexicon relocation is validated).
    _safe_remove(dest)
    _touch_check(db_path, track_id)
    log_activity(
        db_path, "lossless_upgrade_deferred", track_id,
        f"Verified-lossless found via {source} for {artist} - {title} but Lexicon relocate "
        f"was not confirmable — kept lossy, will retry",
        {"source": source},
    )
    log.warning(
        "lossless_upgrade: relocate NOT confirmed for %s - %s; kept lossy, retry later",
        artist, title,
    )
    return "staged"


def run_lossless_upgrade(db_path: str) -> None:
    """One cycle: ensure schema, mark newly-lossy-kept tracks, and re-check a small
    throttled batch. NAS-friendly (bounded batch, weekly per-track throttle). No-op in
    scan mode or when disabled."""
    ensure_schema(db_path)
    if not is_enabled(db_path):
        return
    # scan mode is strictly read-only: never source/download/import.
    if (get_config(db_path, "sync_mode") or "scan") == "scan":
        return

    mark_pending(db_path)
    tracks = due_tracks(db_path, _batch(db_path))
    if not tracks:
        return
    log.info("lossless_upgrade: re-checking %d lossy-kept track(s)", len(tracks))
    for track in tracks:
        try:
            _attempt_upgrade(db_path, track)
        except Exception as e:  # noqa: BLE001 — one bad track must not stall the loop
            log.error("lossless_upgrade error for track %d: %s", track.get("id"), e, exc_info=True)
            try:
                _touch_check(db_path, track["id"])
            except Exception:  # noqa: BLE001
                pass
