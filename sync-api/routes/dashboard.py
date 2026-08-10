import os
import time
import json
from fastapi import APIRouter, HTTPException
import httpx

from db import get_db
from models import DashboardResponse, ServiceHealth

router = APIRouter(prefix="/api", tags=["dashboard"])

LEXICON_API = os.environ.get("LEXICON_API_URL", "http://localhost:48624")
TIDARR_API = os.environ.get("TIDARR_URL", "http://localhost:8484")  # optional legacy fallback

# The worker re-probes Soulseek every 120s. If the last verdict is older than this,
# the worker itself is the thing that is unwell, and reporting its last known "ok"
# would be reporting a lie.
_SOULSEEK_STALE_AFTER_SECONDS = 900

# Statuses that mean "deliberately off", not "broken".
_SOULSEEK_INACTIVE = {"disabled", "not_configured"}


def _soulseek_service(conn) -> ServiceHealth:
    """Report the verdict last persisted by the worker's soulseek_health task."""
    keys = ("soulseek_health", "soulseek_detail", "soulseek_checked_at", "soulseek_latency_ms")
    rows = conn.execute(
        f"SELECT key, value FROM app_config WHERE key IN ({','.join('?' * len(keys))})",
        keys,
    ).fetchall()
    cfg = {r["key"]: r["value"] for r in rows}

    status = (cfg.get("soulseek_health") or "").strip()
    detail = cfg.get("soulseek_detail") or None
    checked_at = cfg.get("soulseek_checked_at")

    if not status:
        return ServiceHealth(
            name="soulseek", status="unknown",
            error="no health check recorded yet — is the worker running?",
        )

    try:
        latency = float(cfg["soulseek_latency_ms"]) if cfg.get("soulseek_latency_ms") else None
    except (TypeError, ValueError):
        latency = None

    if checked_at and status not in _SOULSEEK_INACTIVE:
        try:
            from datetime import datetime, timezone
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(checked_at)).total_seconds()
            if age > _SOULSEEK_STALE_AFTER_SECONDS:
                return ServiceHealth(
                    name="soulseek", status="unknown", latency_ms=latency,
                    error=f"last checked {int(age // 60)} min ago — worker may be stalled",
                )
        except (TypeError, ValueError):
            pass

    if status == "ok":
        return ServiceHealth(name="soulseek", status="ok", latency_ms=latency)
    if status in _SOULSEEK_INACTIVE:
        return ServiceHealth(name="soulseek", status="disabled", error=detail)
    return ServiceHealth(name="soulseek", status="error", latency_ms=latency, error=detail)


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard():
    try:
        with get_db() as conn:
            # Total tracks
            spotify_total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

            # Synced to Lexicon
            lexicon_synced = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE lexicon_status = 'synced'"
            ).fetchone()[0]

            parity_pct = round((lexicon_synced / spotify_total * 100), 2) if spotify_total > 0 else 0.0

            # Counts by pipeline_stage
            by_pipeline_stage = {}
            for row in conn.execute(
                "SELECT pipeline_stage, COUNT(*) as cnt FROM tracks GROUP BY pipeline_stage"
            ).fetchall():
                by_pipeline_stage[row["pipeline_stage"]] = row["cnt"]

            # Counts by match_status
            by_match_status = {}
            for row in conn.execute(
                "SELECT match_status, COUNT(*) as cnt FROM tracks GROUP BY match_status"
            ).fetchall():
                by_match_status[row["match_status"]] = row["cnt"]

            # Counts by download_status
            by_download_status = {}
            for row in conn.execute(
                "SELECT download_status, COUNT(*) as cnt FROM tracks GROUP BY download_status"
            ).fetchall():
                by_download_status[row["download_status"]] = row["cnt"]

            # Counts by verify_status
            by_verify_status = {}
            for row in conn.execute(
                "SELECT verify_status, COUNT(*) as cnt FROM tracks GROUP BY verify_status"
            ).fetchall():
                by_verify_status[row["verify_status"]] = row["cnt"]

            # Counts by lexicon_status
            by_lexicon_status = {}
            for row in conn.execute(
                "SELECT lexicon_status, COUNT(*) as cnt FROM tracks GROUP BY lexicon_status"
            ).fetchall():
                by_lexicon_status[row["lexicon_status"]] = row["cnt"]

            # Recent activity
            rows = conn.execute(
                "SELECT * FROM activity_log ORDER BY created_at DESC LIMIT 20"
            ).fetchall()
            recent_activity = []
            for row in rows:
                entry = dict(row)
                if entry.get("details"):
                    try:
                        entry["details"] = json.loads(entry["details"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                recent_activity.append(entry)

        # Service health checks
        services = []

        # Lexicon. Probe /v1/playlists, NOT /v1/tracks: this endpoint is polled every
        # 10s by the dashboard, and /v1/tracks returns up to 1000 full track records
        # per call just to prove the service answers.
        # lexicon_health._check_lexicon_reachable already uses /v1/playlists.
        try:
            t0 = time.monotonic()
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{LEXICON_API}/v1/playlists")
            latency = round((time.monotonic() - t0) * 1000, 1)
            services.append(ServiceHealth(
                name="lexicon",
                status="ok" if resp.status_code == 200 else "error",
                latency_ms=latency,
                error=None if resp.status_code == 200 else f"HTTP {resp.status_code}",
            ))
        except Exception as e:
            services.append(ServiceHealth(name="lexicon", status="error", error=str(e)))

        # Tidal Downloader (optional legacy Tidarr check)
        try:
            t0 = time.monotonic()
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{TIDARR_API}")
            latency = round((time.monotonic() - t0) * 1000, 1)
            services.append(ServiceHealth(
                name="tidal",
                status="ok" if resp.status_code < 500 else "error",
                latency_ms=latency,
                error=None if resp.status_code < 500 else f"HTTP {resp.status_code}",
            ))
        except Exception as e:
            services.append(ServiceHealth(name="tidal", status="error", error=str(e)))

        # Soulseek. The API has neither the slskd credentials nor the worker's client,
        # so it reports what the worker's soulseek_health probe last persisted.
        services.append(_soulseek_service(conn))

        return DashboardResponse(
            spotify_total=spotify_total,
            lexicon_synced=lexicon_synced,
            parity_pct=parity_pct,
            by_pipeline_stage=by_pipeline_stage,
            by_match_status=by_match_status,
            by_download_status=by_download_status,
            by_verify_status=by_verify_status,
            by_lexicon_status=by_lexicon_status,
            recent_activity=recent_activity,
            services=services,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/monthly")
async def monthly_progress():
    """Return sync progress broken down by month."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT
                    substr(spotify_added_at, 1, 7) as month,
                    COUNT(*) as total,
                    SUM(CASE WHEN pipeline_stage = 'complete' THEN 1 ELSE 0 END) as complete,
                    SUM(CASE WHEN pipeline_stage = 'error' THEN 1 ELSE 0 END) as errors
                FROM tracks
                WHERE spotify_added_at IS NOT NULL
                GROUP BY month
                ORDER BY month DESC
            """).fetchall()
            months = [{"month": r[0], "total": r[1], "complete": r[2], "errors": r[3]} for r in rows]
            return {"months": months}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
