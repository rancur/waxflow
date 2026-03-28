from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from db import get_db
from models import DownloadQueueItem, DownloadStatsResponse, TrackOut
from routes.tracks import row_to_track

router = APIRouter(prefix="/api/downloads", tags=["downloads"])


@router.get("")
async def list_downloads(
    status: Optional[str] = Query(None),
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

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            total = conn.execute(
                f"SELECT COUNT(*) FROM download_queue dq {where}", params
            ).fetchone()[0]

            offset = (page - 1) * per_page
            rows = conn.execute(
                f"""SELECT dq.*, t.title, t.artist, t.album, t.spotify_id
                    FROM download_queue dq
                    LEFT JOIN tracks t ON t.id = dq.track_id
                    {where}
                    ORDER BY dq.priority DESC, dq.created_at ASC
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
                }
                items.append(item)

            return {"items": items, "total": total, "page": page, "per_page": per_page}
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
                    "INSERT INTO download_queue (track_id, priority, source, status) VALUES (?, 0, 'tidarr', 'pending')",
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

            return DownloadStatsResponse(
                total=total,
                pending=pending,
                queued=queued,
                downloading=downloading,
                complete=complete,
                failed=failed,
                avg_download_time_seconds=avg_time,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
