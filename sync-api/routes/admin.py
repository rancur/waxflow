import os
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException

from db import get_db
from models import ConfigUpdate, HealthResponse, VersionResponse

router = APIRouter(prefix="/api", tags=["admin"])

_start_time = time.time()


SENSITIVE_KEYS = {"spotify_access_token", "spotify_refresh_token", "spotify_token_expiry"}


@router.get("/settings")
async def get_settings():
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT key, value FROM app_config").fetchall()
            settings = {r["key"]: r["value"] for r in rows if r["key"] not in SENSITIVE_KEYS}
            return {"settings": settings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/settings")
async def update_settings(body: ConfigUpdate):
    try:
        with get_db() as conn:
            for key, value in {k: v for k, v in body.settings.items() if k not in SENSITIVE_KEYS}.items():
                conn.execute(
                    "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                    (key, value),
                )
            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                ("settings_updated", "Settings updated",
                 str(body.settings)),
            )
            return {"status": "ok", "updated": list(body.settings.keys())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/health", response_model=HealthResponse)
async def health_check():
    db_status = "ok"
    try:
        with get_db() as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception:
        db_status = "error"

    status = "ok" if db_status == "ok" else "degraded"
    return HealthResponse(
        status=status,
        database=db_status,
        uptime_seconds=round(time.time() - _start_time, 1),
    )


@router.post("/admin/update")
async def trigger_update():
    """Create a signal file that the auto-update cron script watches for.
    Writes to the Docker volume at /app/data/ so it persists and is visible to host scripts.
    """
    signal_path = Path("/app/data/.update-requested")
    try:
        signal_path.write_text(f"requested at {time.time()}")
        return {"status": "ok", "message": "Update requested. The auto-update cron will pick this up."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/version", response_model=VersionResponse)
async def get_version():
    # Read version from VERSION file (baked into image at build time)
    version = None
    version_path = Path("/app/VERSION")
    try:
        if version_path.exists():
            version = version_path.read_text().strip()
    except Exception:
        pass

    # Read git SHA from env var (set as build arg)
    git_sha = os.environ.get("GIT_SHA") or None
    if git_sha == "unknown":
        git_sha = None

    return VersionResponse(version=version, git_sha=git_sha)
