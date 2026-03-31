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


@router.get("/admin/sync-mode")
async def get_sync_mode():
    with get_db() as conn:
        mode = conn.execute("SELECT value FROM app_config WHERE key = 'sync_mode'").fetchone()
        return {"sync_mode": mode[0] if mode else "scan"}


@router.post("/admin/sync-mode")
async def set_sync_mode(body: dict):
    mode = body.get("mode", "scan")
    if mode not in ("scan", "full"):
        raise HTTPException(status_code=400, detail="Invalid mode. Must be 'scan' or 'full'.")
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES ('sync_mode', ?)", (mode,))
        conn.execute(
            "INSERT INTO activity_log (event_type, message) VALUES (?, ?)",
            ("sync_mode_changed", f"Sync mode changed to: {mode}"),
        )
        tracks_queued = 0
        if mode == "full":
            r = conn.execute(
                "UPDATE tracks SET pipeline_stage = 'matching', updated_at = datetime('now') WHERE pipeline_stage = 'waiting'"
            )
            tracks_queued = r.rowcount
        return {"sync_mode": mode, "tracks_queued": tracks_queued}


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


@router.get("/admin/export")
async def export_sync_report(format: str = "json"):
    """Export full sync report as CSV or JSON."""
    with get_db() as conn:
        tracks = conn.execute("""
            SELECT spotify_id, title, artist, album, spotify_added_at,
                   pipeline_stage, match_status, download_status, verify_status,
                   lexicon_status, verify_codec, verify_sample_rate, verify_bit_depth,
                   file_path, pipeline_error
            FROM tracks ORDER BY spotify_added_at DESC
        """).fetchall()

        if format == "csv":
            import csv
            import io

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([
                "spotify_id", "title", "artist", "album", "added",
                "pipeline", "match", "download", "verify", "lexicon",
                "codec", "sample_rate", "bit_depth", "file_path", "error",
            ])
            for t in tracks:
                writer.writerow(list(t))
            from fastapi.responses import Response
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=sync-report.csv"},
            )
        else:
            return {"tracks": [dict(t) for t in tracks], "total": len(tracks)}


@router.get("/admin/analyze-stats")
async def get_analyze_stats():
    """Return stats about the auto-analysis system."""
    try:
        with get_db() as conn:
            total_processed = conn.execute(
                "SELECT value FROM app_config WHERE key = 'analyze_total_processed'"
            ).fetchone()
            analyze_interval = conn.execute(
                "SELECT value FROM app_config WHERE key = 'analyze_interval_seconds'"
            ).fetchone()
            batch_size = conn.execute(
                "SELECT value FROM app_config WHERE key = 'analyze_batch_size'"
            ).fetchone()
            enabled = conn.execute(
                "SELECT value FROM app_config WHERE key = 'auto_analyze_enabled'"
            ).fetchone()

            # Count recent analysis events
            recent_events = conn.execute(
                "SELECT COUNT(*) FROM activity_log WHERE event_type IN ('track_analyzed', 'analyze_batch') AND created_at > datetime('now', '-24 hours')"
            ).fetchone()[0]

            return {
                "enabled": (enabled[0] if enabled else "1") != "0",
                "total_processed": int(total_processed[0]) if total_processed else 0,
                "interval_seconds": int(analyze_interval[0]) if analyze_interval else 3600,
                "batch_size": int(batch_size[0]) if batch_size else 20,
                "events_last_24h": recent_events,
            }
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
