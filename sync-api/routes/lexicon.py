import json
import logging
import os
from fastapi import APIRouter, HTTPException

from db import get_db
from models import LexiconStatusResponse, LexiconBackupOut
from services.lexicon_sync import LexiconSyncService

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lexicon", tags=["lexicon"])


@router.get("/status", response_model=LexiconStatusResponse)
async def lexicon_status():
    lexicon = LexiconSyncService()
    connected = False
    track_count = None

    try:
        tracks = await lexicon.get_tracks()
        connected = True
        track_count = len(tracks) if isinstance(tracks, list) else None
    except Exception as e:
        log.debug("Lexicon connection check failed: %s", e)

    with get_db() as conn:
        last_sync_row = conn.execute(
            "SELECT value FROM app_config WHERE key = 'last_lexicon_sync'"
        ).fetchone()
        last_sync = last_sync_row["value"] if last_sync_row else None

    return LexiconStatusResponse(
        connected=connected,
        base_url=lexicon.base_url,
        last_sync=last_sync,
        track_count=track_count,
    )


@router.post("/backup")
async def create_backup():
    try:
        lexicon = LexiconSyncService()
        result = await lexicon.backup()

        with get_db() as conn:
            backup_path = result.get("path", "unknown")
            backup_size = result.get("size", 0)
            conn.execute(
                "INSERT INTO lexicon_backups (backup_path, backup_size_bytes, trigger) VALUES (?, ?, ?)",
                (backup_path, backup_size, "manual"),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                ("lexicon_backup", "Lexicon backup created", json.dumps(result)),
            )

        return {"status": "ok", "backup": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backups")
async def list_backups():
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM lexicon_backups ORDER BY created_at DESC"
            ).fetchall()
            backups = [LexiconBackupOut(**dict(r)) for r in rows]
            return {"backups": backups}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/protected")
async def protected_tracks():
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM tracks WHERE is_protected = 1 ORDER BY artist, title"
            ).fetchall()
            from routes.tracks import row_to_track
            from models import TrackOut
            tracks = [TrackOut(**row_to_track(r)) for r in rows]
            return {"tracks": tracks, "total": len(tracks)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
