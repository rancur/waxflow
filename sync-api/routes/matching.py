import json
from fastapi import APIRouter, HTTPException

from db import get_db
from models import TrackOut, ManualMatchRequest
from routes.tracks import row_to_track

router = APIRouter(prefix="/api/matching", tags=["matching"])


@router.get("/review")
async def review_matches():
    """Return mismatched tracks with details useful for side-by-side comparison."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM tracks WHERE match_status = 'mismatched' ORDER BY updated_at DESC"
            ).fetchall()
            tracks = []
            for r in rows:
                t = row_to_track(r)
                # Add comparison-friendly fields
                track_out = TrackOut(**t)
                comparison = {
                    "track": track_out,
                    "spotify_duration_s": round((t.get("duration_ms") or 0) / 1000.0, 1),
                    "file_path": t.get("file_path"),
                    "fingerprint_match_score": t.get("fingerprint_match_score"),
                    "verify_codec": t.get("verify_codec"),
                    "verify_sample_rate": t.get("verify_sample_rate"),
                    "pipeline_error": t.get("pipeline_error"),
                    "tidal_id": t.get("tidal_id"),
                    "match_source": t.get("match_source"),
                    "match_confidence": t.get("match_confidence"),
                }
                tracks.append(comparison)
            return {"tracks": tracks, "total": len(tracks)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{track_id}/approve", response_model=TrackOut)
async def approve_match(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")
            if row["match_status"] not in ("mismatched", "pending"):
                raise HTTPException(status_code=400, detail=f"Cannot approve track with status '{row['match_status']}'")

            conn.execute(
                """UPDATE tracks SET
                    match_status = 'matched',
                    pipeline_stage = CASE
                        WHEN download_status = 'complete' THEN 'verifying'
                        ELSE 'downloading'
                    END,
                    updated_at = datetime('now')
                WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("match_approved", track_id, f"Match approved for track {track_id}"),
            )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{track_id}/reject", response_model=TrackOut)
async def reject_match(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            conn.execute(
                """UPDATE tracks SET
                    match_status = 'failed',
                    tidal_id = NULL,
                    match_confidence = NULL,
                    match_source = NULL,
                    pipeline_stage = 'matching',
                    updated_at = datetime('now')
                WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("match_rejected", track_id, f"Match rejected for track {track_id}, re-entering fallback"),
            )
            # Record fallback attempt
            conn.execute(
                "INSERT INTO fallback_attempts (track_id, source, status, error) VALUES (?, ?, ?, ?)",
                (track_id, "manual_reject", "rejected", "User rejected match"),
            )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{track_id}/manual", response_model=TrackOut)
async def manual_match(track_id: int, body: ManualMatchRequest):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            if not body.tidal_id and not body.file_path:
                raise HTTPException(status_code=400, detail="Provide either tidal_id or file_path")

            updates = {
                "match_status": "manual",
                "match_source": "manual",
                "match_confidence": 1.0,
            }

            if body.tidal_id:
                updates["tidal_id"] = body.tidal_id
                updates["pipeline_stage"] = "downloading"
            if body.file_path:
                updates["file_path"] = body.file_path
                updates["download_status"] = "complete"
                updates["pipeline_stage"] = "verifying"

            set_parts = []
            params = []
            for k, v in updates.items():
                set_parts.append(f"{k} = ?")
                params.append(v)
            set_parts.append("updated_at = datetime('now')")
            params.append(track_id)

            conn.execute(
                f"UPDATE tracks SET {', '.join(set_parts)} WHERE id = ?",
                params,
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                ("manual_match", track_id, f"Manual match set for track {track_id}",
                 json.dumps(body.model_dump(exclude_unset=True))),
            )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
