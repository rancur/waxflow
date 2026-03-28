import time
import json
from fastapi import APIRouter, HTTPException
import httpx

from db import get_db
from models import DashboardResponse, ServiceHealth

router = APIRouter(prefix="/api", tags=["dashboard"])

LEXICON_API = "http://192.168.1.116:48624"
TIDARR_API = "http://192.168.1.221:8484"


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

        # Lexicon
        try:
            t0 = time.monotonic()
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{LEXICON_API}/v1/tracks")
            latency = round((time.monotonic() - t0) * 1000, 1)
            services.append(ServiceHealth(
                name="lexicon",
                status="ok" if resp.status_code == 200 else "error",
                latency_ms=latency,
                error=None if resp.status_code == 200 else f"HTTP {resp.status_code}",
            ))
        except Exception as e:
            services.append(ServiceHealth(name="lexicon", status="error", error=str(e)))

        # Tidarr
        try:
            t0 = time.monotonic()
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{TIDARR_API}")
            latency = round((time.monotonic() - t0) * 1000, 1)
            services.append(ServiceHealth(
                name="tidarr",
                status="ok" if resp.status_code < 500 else "error",
                latency_ms=latency,
                error=None if resp.status_code < 500 else f"HTTP {resp.status_code}",
            ))
        except Exception as e:
            services.append(ServiceHealth(name="tidarr", status="error", error=str(e)))

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
