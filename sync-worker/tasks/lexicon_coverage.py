"""Post-processing coverage: how much of the library actually got cues, tags, keys.

WHY THIS EXISTS
    The suspicion was that Lexicon's post-processing actions ("Find tags & album
    art", "Generate cue points", "Upload to Cloud") had stopped running. Measuring
    it showed the opposite -- of 400 tracks imported since July, 400 had BPM, 399
    had cue points, and only 4 tracks in the entire 5,600-track library lacked cues.
    Nothing was broken; there was simply no way to SEE it, so the absence of
    evidence read as evidence of absence.

    That is the gap this closes. It answers "is post-processing keeping up?" with a
    number instead of a hunch.

WHAT CAN AND CANNOT BE MEASURED
    Lexicon's /v1/tracks exposes cuepoints, tags, tempomarkers, bpm, key and genre,
    so those are measured directly. It exposes NO artwork or cloud-upload field --
    that state lives in Lexicon's own SQLite (the CloudFile table), which the worker
    runs on the NAS and cannot reach. Those two are deliberately reported as
    unavailable rather than guessed at; see docs for the ops-side check.

COST
    /v1/tracks returns the entire library in one response, so this is a genuinely
    expensive call -- it is emphatically not something a request handler should do.
    It runs hourly on the worker, gated on the Mac being awake, and writes a small
    JSON rollup into app_config for sync-api to serve instantly.

Config (read live from app_config):
    lexicon_coverage_enabled            default 1
    lexicon_coverage_interval_seconds   default 3600
    lexicon_api_url                     falls back to LEXICON_API_URL
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from tasks.helpers import LEXICON_API_URL, get_config, set_config

log = logging.getLogger("worker.lexicon_coverage")

COVERAGE_KEY = "lexicon_coverage"
CHECKED_AT_KEY = "lexicon_coverage_checked_at"

# Fields Lexicon reports and we can therefore count honestly.
_MEASURABLE = ("cuepoints", "tempomarkers", "tags", "bpm", "key", "genre")

# Post-processing outputs that /v1/tracks does not expose at all.
_UNMEASURABLE = ("artwork", "cloud")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def is_enabled(db_path: str) -> bool:
    val = get_config(db_path, "lexicon_coverage_enabled")
    if val is None:
        return True
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _has(track: dict, field: str) -> bool:
    """Did this field actually get populated?

    Lists (cuepoints, tempomarkers, tags) count as present only when non-empty;
    scalars (bpm, key, genre) when truthy. A bpm of 0 means unanalyzed, so treating
    0 as absent is correct here rather than a bug.
    """
    value = track.get(field)
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, str):
        return value.strip() != ""
    return bool(value)


def summarize(tracks: list[dict]) -> dict:
    """Turn the raw track list into a rollup. Pure -- no I/O, so it is testable."""
    total = len(tracks)
    counts = {field: 0 for field in _MEASURABLE}
    for track in tracks:
        if track.get("archived"):
            continue  # archived tracks are out of the library, not "missing cues"
        for field in _MEASURABLE:
            if _has(track, field):
                counts[field] += 1

    active = sum(1 for t in tracks if not t.get("archived"))
    coverage = {
        field: {
            "count": counts[field],
            "pct": round(counts[field] / active * 100, 1) if active else 0.0,
        }
        for field in _MEASURABLE
    }
    return {
        "total_tracks": total,
        "active_tracks": active,
        "coverage": coverage,
        "unavailable": list(_UNMEASURABLE),
    }


# Lexicon caps /v1/tracks at 1000 rows per request and rejects anything larger with
# an error, so the whole library has to be walked in pages. Reading the unpaginated
# response looked like it worked -- it returned a full 1000 tracks and a plausible
# percentage -- while actually measuring only the oldest fifth of the library.
_PAGE_SIZE = 1000
_MAX_PAGES = 100  # 100k tracks; a stop so a bad `total` cannot loop forever


def fetch_tracks(api_url: str, timeout: float = 120.0) -> list[dict]:
    """Fetch the whole Lexicon library, one page at a time. See COST above."""
    tracks: list[dict] = []
    with httpx.Client(base_url=api_url, timeout=timeout) as client:
        for page in range(_MAX_PAGES):
            resp = client.get(
                "/v1/tracks",
                params={"limit": _PAGE_SIZE, "offset": page * _PAGE_SIZE},
            )
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            batch = data.get("tracks") or []
            tracks.extend(batch)

            total = data.get("total")
            if len(batch) < _PAGE_SIZE or (total is not None and len(tracks) >= total):
                break
        else:
            log.warning(
                "lexicon_coverage: stopped at %d pages — coverage covers %d tracks, "
                "which may not be the whole library",
                _MAX_PAGES, len(tracks),
            )
    return tracks


def collect(db_path: str) -> dict | None:
    """Probe once and persist the rollup. Returns it, or None if skipped."""
    if not is_enabled(db_path):
        return None

    # Don't wake or hammer a sleeping Mac for a statistic.
    from tasks import mac_availability
    availability = mac_availability.probe(db_path, record=False)
    if not availability.lexicon_available:
        log.debug("lexicon_coverage: skipping, Mac is %s", availability.state)
        return None

    api_url = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    tracks = fetch_tracks(api_url)
    summary = summarize(tracks)

    set_config(db_path, COVERAGE_KEY, json.dumps(summary))
    set_config(db_path, CHECKED_AT_KEY, _now_iso())
    log.info(
        "lexicon_coverage: %d active tracks — cues %.1f%%, tags %.1f%%, key %.1f%%",
        summary["active_tracks"],
        summary["coverage"]["cuepoints"]["pct"],
        summary["coverage"]["tags"]["pct"],
        summary["coverage"]["key"]["pct"],
    )
    return summary


async def lexicon_coverage(db_path: str) -> None:
    """Worker entry point."""
    try:
        collect(db_path)
    except Exception as e:  # noqa: BLE001
        # Never let a statistics task take down the worker loop.
        log.warning("lexicon_coverage failed: %s", e)
