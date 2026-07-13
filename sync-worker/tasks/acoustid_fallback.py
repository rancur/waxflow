"""WaxFlow — acoustic-fingerprint fallback (Chromaprint / AcoustID).  [SCAFFOLD]

The deeper "no match" recovery: when a candidate audio FILE exists locally (a
lossy copy in the library, or a download that failed the lossless gate) but we
cannot confirm it IS the liked track from metadata alone, fingerprint the file
with Chromaprint (``fpcalc``) and identify it via AcoustID — confirming the file
is the recording even with Spotify gone.

STATUS — scaffolded + CONFIG-GATED, off by default.
  * ``fpcalc`` IS present in the worker image (chromaprint 1.5.1), so the binary
    dependency is satisfied.
  * An AcoustID API key is NOT yet provisioned (none in 1Password at build time).
    Keys are free from https://acoustid.org/new-application. Seed it into
    app_config as ``acoustid_api_key`` and flip ``acoustid_fallback_enabled=1`` to
    activate — no redeploy needed (config is read live).
  * Until both are set, this task is an explicit, logged no-op. It never fabricates
    a key or a result. The recovery logic below is complete and ready; only the
    key gates it.

Like the metadata fallback, a confirmed identification is surfaced in Match Review
as a fallback-sourced proposal for human approval — never auto-imported — and is
non-destructive to files and Lexicon.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess

from tasks.helpers import get_config, get_db, log_activity, update_track

log = logging.getLogger("worker.acoustid_fallback")

_ACOUSTID_URL = "https://api.acoustid.org/v2/lookup"
_MIN_SCORE = 0.85  # AcoustID result score threshold to accept an identification.


def _enabled(db_path: str) -> bool:
    return (get_config(db_path, "acoustid_fallback_enabled") or "0") in ("1", "true", "True")


def _fpcalc_path() -> str | None:
    return shutil.which("fpcalc") or (os.path.exists("/usr/bin/fpcalc") and "/usr/bin/fpcalc") or None


def _api_key(db_path: str) -> str | None:
    key = (get_config(db_path, "acoustid_api_key") or "").strip()
    return key or None


def readiness(db_path: str) -> dict:
    """Report why the fingerprint path is or isn't active (for diagnostics)."""
    return {
        "enabled": _enabled(db_path),
        "fpcalc_present": bool(_fpcalc_path()),
        "api_key_present": bool(_api_key(db_path)),
    }


def _fingerprint(path: str) -> tuple[int, str] | None:
    """Return (duration_seconds, chromaprint_fingerprint) for a local file, or None."""
    fp = _fpcalc_path()
    if not fp or not os.path.isfile(path):
        return None
    try:
        out = subprocess.run(
            [fp, "-json", path], capture_output=True, text=True, timeout=120, check=True,
        )
        data = json.loads(out.stdout)
        return int(data["duration"]), data["fingerprint"]
    except Exception as e:  # noqa: BLE001
        log.debug("fpcalc failed for %s: %s", path, e)
        return None


def _acoustid_lookup(api_key: str, duration: int, fingerprint: str) -> dict | None:
    """Look up a fingerprint against AcoustID. Returns the best {recording} result."""
    import httpx

    params = {
        "client": api_key,
        "duration": str(duration),
        "fingerprint": fingerprint,
        "meta": "recordings",
        "format": "json",
    }
    try:
        with httpx.Client(timeout=20) as client:
            r = client.post(_ACOUSTID_URL, data=params)
        if r.status_code != 200:
            return None
        results = (r.json() or {}).get("results") or []
        for res in results:
            if (res.get("score") or 0) >= _MIN_SCORE and res.get("recordings"):
                return res
    except Exception as e:  # noqa: BLE001
        log.debug("AcoustID lookup failed: %s", e)
    return None


def _candidates_with_local_file(db_path: str, limit: int) -> list[dict]:
    """No-match tracks that have a local candidate file to fingerprint."""
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT t.* FROM tracks t
                WHERE t.match_status = 'failed'
                  AND t.pipeline_stage = 'error'
                  AND t.file_path IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM source_attempts sa
                       WHERE sa.track_id = t.id AND sa.source = 'acoustid'
                  )
                ORDER BY t.updated_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def _run_sync(db_path: str) -> dict:
    counts = {"attempted": 0, "identified": 0, "skipped_no_key": 0}
    if not _enabled(db_path):
        return counts
    api_key = _api_key(db_path)
    if not (api_key and _fpcalc_path()):
        # Enabled but not provisioned — log once-ish and no-op. Never fabricate.
        counts["skipped_no_key"] = 1
        log.info("acoustid_fallback enabled but not provisioned: %s", readiness(db_path))
        return counts

    for track in _candidates_with_local_file(db_path, 5):
        counts["attempted"] += 1
        track_id = track["id"]
        try:
            fp = _fingerprint(track["file_path"])
            if not fp:
                continue
            duration, fingerprint = fp
            res = _acoustid_lookup(api_key, duration, fingerprint)
            if not res:
                continue
            rec = (res.get("recordings") or [{}])[0]
            provenance = {
                "fallback": {
                    "via": "acoustid",
                    "acoustid_score": res.get("score"),
                    "mb_recording_id": rec.get("id"),
                    "resolved_title": rec.get("title"),
                    "file_path": track["file_path"],
                }
            }
            update_track(
                db_path, track_id,
                match_status="mismatched",
                match_source="acoustid",
                match_confidence=float(res.get("score") or 0.0),
                pipeline_error="Identified via AcoustID fingerprint — awaiting Match Review approval",
                notes=json.dumps(provenance),
            )
            log_activity(
                db_path, "acoustid_fallback_identified", track_id,
                f"Identified track {track_id} via AcoustID (score={res.get('score')})",
                provenance["fallback"],
            )
            counts["identified"] += 1
        except Exception as e:  # noqa: BLE001
            log.warning("acoustid_fallback error for track %d: %s", track_id, e)
    return counts


async def acoustid_fallback(db_path: str):
    """Worker entrypoint — run one AcoustID fingerprint pass (no-op unless provisioned)."""
    await asyncio.to_thread(_run_sync, db_path)
