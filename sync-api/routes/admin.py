import subprocess
import time
from fastapi import APIRouter, HTTPException

from db import get_db
from models import ConfigUpdate, HealthResponse, VersionResponse

router = APIRouter(prefix="/api", tags=["admin"])

_start_time = time.time()


@router.get("/settings")
async def get_settings():
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT key, value FROM app_config").fetchall()
            settings = {r["key"]: r["value"] for r in rows}
            return {"settings": settings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/settings")
async def update_settings(body: ConfigUpdate):
    try:
        with get_db() as conn:
            for key, value in body.settings.items():
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
    try:
        result = subprocess.run(
            ["git", "pull"], capture_output=True, text=True, timeout=30, cwd="/app"
        )
        return {
            "status": "ok",
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Git pull timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/version", response_model=VersionResponse)
async def get_version():
    git_sha = None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, cwd="/app",
        )
        if result.returncode == 0:
            git_sha = result.stdout.strip()
    except Exception:
        pass

    return VersionResponse(git_sha=git_sha)
