import glob
import json
import os
import shutil
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException

from db import get_db
from models import ConfigUpdate, HealthResponse, SubsystemHealth, VersionResponse

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


_TIDAL_AUTH_PATHS = [
    "/tiddl-auth/auth.json",
    "/app/data/tiddl-auth.json",
]


@router.get("/admin/health", response_model=HealthResponse)
async def health_check():
    # --- Database ---
    t0 = time.monotonic()
    db_status, db_detail = "ok", None
    try:
        with get_db() as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as e:
        db_status, db_detail = "error", str(e)
    db_latency = round((time.monotonic() - t0) * 1000, 1)

    # --- Spotify token ---
    spotify_status, spotify_detail = "unknown", None
    try:
        with get_db() as conn:
            access = conn.execute("SELECT value FROM app_config WHERE key='spotify_access_token'").fetchone()
            expiry = conn.execute("SELECT value FROM app_config WHERE key='spotify_token_expiry'").fetchone()
        if not access or not access[0]:
            spotify_status, spotify_detail = "unauthenticated", "No access token stored"
        elif expiry and expiry[0]:
            remaining = int(expiry[0]) - int(time.time())
            if remaining < 0:
                spotify_status, spotify_detail = "expired", f"Token expired {-remaining}s ago"
            elif remaining < 300:
                spotify_status, spotify_detail = "expiring_soon", f"Expires in {remaining}s"
            else:
                spotify_status, spotify_detail = "ok", f"Valid for {remaining}s"
        else:
            spotify_status = "ok"
    except Exception as e:
        spotify_status, spotify_detail = "error", str(e)

    # --- Tidal auth ---
    tidal_status, tidal_detail = "unauthenticated", "No auth file found"
    for auth_path in _TIDAL_AUTH_PATHS:
        if os.path.exists(auth_path):
            try:
                with open(auth_path) as f:
                    auth = json.load(f)
                remaining = int(auth.get("expires_at", 0)) - int(time.time())
                if remaining < 0:
                    tidal_status, tidal_detail = "expired", f"Token expired {-remaining}s ago"
                elif remaining < 3600:
                    tidal_status, tidal_detail = "expiring_soon", f"Expires in {remaining}s"
                else:
                    tidal_status, tidal_detail = "ok", f"Valid for {remaining}s"
            except Exception as e:
                tidal_status, tidal_detail = "error", str(e)
            break

    # --- Disk space ---
    disk_status, disk_detail = "ok", None
    music_path = os.environ.get("MUSIC_LIBRARY_PATH", "/music")
    db_dir = os.path.dirname(os.environ.get("SLS_DB_PATH", "/app/data/sync.db"))
    for check_path in [music_path, db_dir]:
        if os.path.exists(check_path):
            try:
                usage = shutil.disk_usage(check_path)
                free_pct = usage.free / usage.total * 100
                free_gb = usage.free / (1024 ** 3)
                if free_pct < 5:
                    disk_status, disk_detail = "error", f"{check_path}: {free_pct:.1f}% free ({free_gb:.1f} GB)"
                    break
                elif free_pct < 15 and disk_status == "ok":
                    disk_status, disk_detail = "degraded", f"{check_path}: {free_pct:.1f}% free ({free_gb:.1f} GB)"
            except Exception as e:
                disk_status, disk_detail = "error", str(e)
                break

    # --- Overall status ---
    subsystem_statuses = {db_status, spotify_status, tidal_status, disk_status}
    if "error" in subsystem_statuses:
        overall = "error"
    elif subsystem_statuses & {"expired", "expiring_soon", "degraded", "unauthenticated"}:
        overall = "degraded"
    else:
        overall = "ok"

    return HealthResponse(
        status=overall,
        uptime_seconds=round(time.time() - _start_time, 1),
        database=SubsystemHealth(status=db_status, latency_ms=db_latency, detail=db_detail),
        spotify=SubsystemHealth(status=spotify_status, detail=spotify_detail),
        tidal=SubsystemHealth(status=tidal_status, detail=tidal_detail),
        disk=SubsystemHealth(status=disk_status, detail=disk_detail),
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


@router.post("/admin/rebuild-playlists")
async def rebuild_playlists():
    """Trigger immediate playlist rebuild."""
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES ('auto_playlists_rebuild', '1')"
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, message) VALUES (?, ?)",
                ("playlist_rebuild", "Manual playlist rebuild requested"),
            )
        return {"status": "ok", "message": "Playlist rebuild queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


@router.get("/admin/check-update")
async def check_update():
    """Check GitHub for newer releases."""
    import httpx

    current = "unknown"
    version_path = Path("/app/VERSION")
    try:
        if version_path.exists():
            current = version_path.read_text().strip()
    except Exception:
        pass

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.github.com/repos/rancur/waxflow/releases/latest"
            )
            if resp.status_code == 200:
                data = resp.json()
                latest = data.get("tag_name", "").lstrip("v")
                return {
                    "current_version": current,
                    "latest_version": latest,
                    "update_available": latest != current and latest > current,
                    "release_url": data.get("html_url"),
                    "release_notes": data.get("body", "")[:500],
                    "published_at": data.get("published_at"),
                }
    except Exception:
        pass

    return {"current_version": current, "update_available": False}


# ============================================================
# Config Backup System
# ============================================================

BACKUP_DIR = "/app/data/backups"


@router.post("/admin/backup")
async def create_backup():
    """Create a full backup of the sync database and config."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    # Backup database
    db_src = os.environ.get("SLS_DB_PATH", "/app/data/sync.db")
    db_dst = f"{BACKUP_DIR}/sync_{timestamp}.db"
    shutil.copy2(db_src, db_dst)

    # Backup config (all app_config values)
    with get_db() as conn:
        rows = conn.execute("SELECT key, value FROM app_config").fetchall()
        config = {r["key"]: r["value"] for r in rows}

    config_dst = f"{BACKUP_DIR}/config_{timestamp}.json"
    with open(config_dst, "w") as f:
        json.dump(config, f, indent=2)

    # Prune old backups (keep last 10)
    db_backups = sorted(glob.glob(f"{BACKUP_DIR}/sync_*.db"))
    for old in db_backups[:-10]:
        os.remove(old)
        config_pair = old.replace("sync_", "config_").replace(".db", ".json")
        if os.path.exists(config_pair):
            os.remove(config_pair)

    with get_db() as conn:
        conn.execute(
            "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
            ("backup_created", f"Backup created: {timestamp}",
             json.dumps({"database": db_dst, "config": config_dst})),
        )

    return {
        "status": "ok",
        "database": db_dst,
        "config": config_dst,
        "size_bytes": os.path.getsize(db_dst),
        "timestamp": timestamp,
    }


@router.get("/admin/backups")
async def list_backups():
    """List available config/db backups."""
    backups = []
    for f in sorted(glob.glob(f"{BACKUP_DIR}/sync_*.db"), reverse=True):
        ts = os.path.basename(f).replace("sync_", "").replace(".db", "")
        config_path = f.replace("sync_", "config_").replace(".db", ".json")
        backups.append({
            "timestamp": ts,
            "database": f,
            "config": config_path if os.path.exists(config_path) else None,
            "size_bytes": os.path.getsize(f),
        })
    return {"backups": backups}


@router.post("/admin/restore/{timestamp}")
async def restore_backup(timestamp: str):
    """Restore from a backup by timestamp."""
    db_backup = f"{BACKUP_DIR}/sync_{timestamp}.db"
    config_backup = f"{BACKUP_DIR}/config_{timestamp}.json"

    if not os.path.exists(db_backup):
        raise HTTPException(status_code=404, detail=f"Backup not found: {timestamp}")

    db_dst = os.environ.get("SLS_DB_PATH", "/app/data/sync.db")

    # Create a pre-restore backup first
    pre_ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    shutil.copy2(db_dst, f"{BACKUP_DIR}/sync_prerestore_{pre_ts}.db")

    # Restore database
    shutil.copy2(db_backup, db_dst)

    # Restore config values if config backup exists
    restored_keys = []
    if os.path.exists(config_backup):
        with open(config_backup) as f:
            config = json.load(f)
        with get_db() as conn:
            for key, value in config.items():
                conn.execute(
                    "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                    (key, value),
                )
                restored_keys.append(key)
            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                ("backup_restored", f"Restored from backup: {timestamp}",
                 json.dumps({"config_keys": restored_keys})),
            )

    return {
        "status": "ok",
        "restored_from": timestamp,
        "database_restored": True,
        "config_keys_restored": len(restored_keys),
        "pre_restore_backup": f"sync_prerestore_{pre_ts}.db",
    }
