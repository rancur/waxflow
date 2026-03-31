import json
import os
import shutil
import time

import httpx
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from db import get_db
from models import DownloadQueueItem, DownloadStatsResponse, TrackOut
from routes.tracks import row_to_track

router = APIRouter(prefix="/api/downloads", tags=["downloads"])


@router.get("/active")
async def active_downloads():
    """Get currently active downloads from the worker."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT dq.*, t.title, t.artist, t.album, t.tidal_id
                FROM download_queue dq
                JOIN tracks t ON dq.track_id = t.id
                WHERE dq.status = 'downloading'
                ORDER BY dq.started_at DESC
                LIMIT 5
            """).fetchall()
            items = []
            for r in rows:
                d = dict(r)
                items.append({
                    "id": d["id"],
                    "track_id": d["track_id"],
                    "source": d["source"],
                    "status": d["status"],
                    "title": d.get("title"),
                    "artist": d.get("artist"),
                    "album": d.get("album"),
                    "tidal_id": d.get("tidal_id"),
                    "started_at": d.get("started_at"),
                    "attempts": d.get("attempts", 0),
                })
            return {"active": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def list_downloads(
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("created_at"),
    sort_dir: Optional[str] = Query("desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    try:
        with get_db() as conn:
            conditions = []
            params = []

            if status:
                conditions.append("dq.status = ?")
                params.append(status)

            if search:
                conditions.append("(t.title LIKE ? OR t.artist LIKE ? OR t.album LIKE ?)")
                like = f"%{search}%"
                params.extend([like, like, like])

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            total = conn.execute(
                f"SELECT COUNT(*) FROM download_queue dq LEFT JOIN tracks t ON t.id = dq.track_id {where}", params
            ).fetchone()[0]

            # Validate sort column
            allowed_sorts = {
                "created_at": "dq.created_at",
                "started_at": "dq.started_at",
                "completed_at": "dq.completed_at",
                "status": "dq.status",
                "title": "t.title",
                "artist": "t.artist",
                "attempts": "dq.attempts",
                "priority": "dq.priority",
            }
            order_col = allowed_sorts.get(sort_by, "dq.created_at")
            order_dir = "ASC" if sort_dir and sort_dir.lower() == "asc" else "DESC"

            offset = (page - 1) * per_page
            rows = conn.execute(
                f"""SELECT dq.*, t.title, t.artist, t.album, t.spotify_id,
                        t.file_path, t.verify_codec, t.verify_sample_rate, t.verify_bit_depth
                    FROM download_queue dq
                    LEFT JOIN tracks t ON t.id = dq.track_id
                    {where}
                    ORDER BY {order_col} {order_dir}
                    LIMIT ? OFFSET ?""",
                params + [per_page, offset],
            ).fetchall()

            items = []
            for r in rows:
                d = dict(r)
                item = {
                    "id": d["id"],
                    "track_id": d["track_id"],
                    "priority": d["priority"],
                    "source": d["source"],
                    "status": d["status"],
                    "attempts": d["attempts"],
                    "max_attempts": d["max_attempts"],
                    "error": d["error"],
                    "created_at": d["created_at"],
                    "started_at": d["started_at"],
                    "completed_at": d["completed_at"],
                    "track_title": d.get("title"),
                    "track_artist": d.get("artist"),
                    "track_album": d.get("album"),
                    "file_path": d.get("file_path"),
                    "codec": d.get("verify_codec"),
                    "sample_rate": d.get("verify_sample_rate"),
                    "bit_depth": d.get("verify_bit_depth"),
                }
                items.append(item)

            return {
                "items": items,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": max(1, (total + per_page - 1) // per_page),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent")
async def recent_downloads(limit: int = Query(10, ge=1, le=50)):
    """Last N completed downloads with file details."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT dq.*, t.title, t.artist, t.album, t.file_path,
                        t.verify_codec, t.verify_sample_rate, t.verify_bit_depth
                    FROM download_queue dq
                    LEFT JOIN tracks t ON t.id = dq.track_id
                    WHERE dq.status = 'complete' AND dq.completed_at IS NOT NULL
                    ORDER BY dq.completed_at DESC
                    LIMIT ?""",
                (limit,),
            ).fetchall()

            items = []
            for r in rows:
                d = dict(r)
                items.append({
                    "id": d["id"],
                    "track_id": d["track_id"],
                    "track_title": d.get("title"),
                    "track_artist": d.get("artist"),
                    "track_album": d.get("album"),
                    "file_path": d.get("file_path"),
                    "codec": d.get("verify_codec"),
                    "sample_rate": d.get("verify_sample_rate"),
                    "bit_depth": d.get("verify_bit_depth"),
                    "completed_at": d["completed_at"],
                })
            return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{track_id}/retry")
async def retry_download(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            # Reset download status on track
            conn.execute(
                """UPDATE tracks SET
                    download_status = 'pending',
                    download_error = NULL,
                    download_attempts = 0,
                    pipeline_stage = 'downloading',
                    updated_at = datetime('now')
                WHERE id = ?""",
                (track_id,),
            )

            # Reset or create queue entry
            existing = conn.execute(
                "SELECT id FROM download_queue WHERE track_id = ?", (track_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE download_queue SET
                        status = 'pending', attempts = 0, error = NULL,
                        started_at = NULL, completed_at = NULL
                    WHERE track_id = ?""",
                    (track_id,),
                )
            else:
                conn.execute(
                    "INSERT INTO download_queue (track_id, priority, source, status) VALUES (?, 0, 'tiddl', 'pending')",
                    (track_id,),
                )

            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("download_retry", track_id, f"Download retry queued for track {track_id}"),
            )

            return {"status": "queued", "track_id": track_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=DownloadStatsResponse)
async def download_stats():
    try:
        with get_db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM download_queue").fetchone()[0]
            pending = conn.execute("SELECT COUNT(*) FROM download_queue WHERE status = 'pending'").fetchone()[0]
            queued = conn.execute("SELECT COUNT(*) FROM download_queue WHERE status = 'queued'").fetchone()[0]
            downloading = conn.execute("SELECT COUNT(*) FROM download_queue WHERE status = 'downloading'").fetchone()[0]
            complete = conn.execute("SELECT COUNT(*) FROM download_queue WHERE status = 'complete'").fetchone()[0]
            failed = conn.execute("SELECT COUNT(*) FROM download_queue WHERE status = 'failed'").fetchone()[0]

            avg_row = conn.execute(
                """SELECT AVG(
                    CAST((julianday(completed_at) - julianday(started_at)) * 86400 AS REAL)
                ) as avg_time
                FROM download_queue
                WHERE status = 'complete' AND started_at IS NOT NULL AND completed_at IS NOT NULL"""
            ).fetchone()
            avg_time = round(avg_row["avg_time"], 1) if avg_row and avg_row["avg_time"] else None

            remaining_count = pending + queued + downloading
            estimated_remaining = round(avg_time * remaining_count, 1) if avg_time and remaining_count else None

        # Check tiddl availability
        tiddl_available = shutil.which("tiddl") is not None

        # Check Tidal auth file exists (same paths as tidal.py)
        tidal_auth_paths = ["/tiddl-auth/auth.json", "/app/data/tiddl-auth.json"]
        tidal_authed = False
        for path in tidal_auth_paths:
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        auth = json.load(f)
                    if auth.get("expires_at", 0) > time.time():
                        tidal_authed = True
                except Exception:
                    pass
                break

        # Check Tidarr reachability (quick timeout)
        tidarr_url = os.environ.get("TIDARR_URL", "http://tidarr:8484")
        tidarr_reachable = False
        try:
            resp = httpx.get(f"{tidarr_url}/api/health", timeout=2)
            tidarr_reachable = resp.status_code == 200
        except Exception:
            pass

        # Determine active method
        method = "tiddl" if (tiddl_available and tidal_authed) else ("tidarr" if tidarr_reachable else "none")

        return DownloadStatsResponse(
            total=total,
            pending=pending,
            queued=queued,
            downloading=downloading,
            complete=complete,
            failed=failed,
            avg_download_time_seconds=avg_time,
            estimated_remaining_seconds=estimated_remaining,
            method=method,
            tiddl_available=tiddl_available and tidal_authed,
            tidarr_reachable=tidarr_reachable,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
