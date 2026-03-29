import json
import math
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from db import get_db
from models import TrackOut, TrackUpdate, TrackListResponse, ParityResponse

router = APIRouter(prefix="/api", tags=["tracks"])


def row_to_track(row) -> dict:
    d = dict(row)
    if "is_protected" in d:
        d["is_protected"] = bool(d["is_protected"])
    if "verify_is_genuine_lossless" in d and d["verify_is_genuine_lossless"] is not None:
        d["verify_is_genuine_lossless"] = bool(d["verify_is_genuine_lossless"])
    return d


@router.get("/tracks", response_model=TrackListResponse)
async def list_tracks(
    status: Optional[str] = Query(None, description="Filter by match_status"),
    pipeline_stage: Optional[str] = Query(None, description="Filter by pipeline_stage"),
    search: Optional[str] = Query(None, description="Search title/artist/album"),
    playlist_id: Optional[int] = Query(None, description="Filter by playlist"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    try:
        with get_db() as conn:
            conditions = []
            params = []

            if status:
                conditions.append("t.match_status = ?")
                params.append(status)
            if pipeline_stage:
                conditions.append("t.pipeline_stage = ?")
                params.append(pipeline_stage)
            if search:
                conditions.append("(t.title LIKE ? OR t.artist LIKE ? OR t.album LIKE ?)")
                like = f"%{search}%"
                params.extend([like, like, like])

            join_clause = ""
            if playlist_id is not None:
                join_clause = "JOIN playlist_tracks pt ON pt.track_id = t.id"
                conditions.append("pt.playlist_id = ?")
                params.append(playlist_id)

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            count_sql = f"SELECT COUNT(*) FROM tracks t {join_clause} {where}"
            total = conn.execute(count_sql, params).fetchone()[0]

            pages = max(1, math.ceil(total / per_page))
            offset = (page - 1) * per_page

            query = f"""
                SELECT t.* FROM tracks t {join_clause} {where}
                ORDER BY t.spotify_added_at DESC
                LIMIT ? OFFSET ?
            """
            rows = conn.execute(query, params + [per_page, offset]).fetchall()
            tracks = [TrackOut(**row_to_track(r)) for r in rows]

            return TrackListResponse(
                tracks=tracks,
                total=total,
                page=page,
                per_page=per_page,
                pages=pages,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/parity", response_model=ParityResponse)
async def parity_check():
    try:
        with get_db() as conn:
            spotify_total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
            lexicon_synced = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE lexicon_status = 'synced'"
            ).fetchone()[0]
            missing = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE lexicon_status IN ('pending', 'error')"
            ).fetchone()[0]
            mismatched = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE match_status = 'mismatched'"
            ).fetchone()[0]
            lexicon_only = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE lexicon_status = 'skipped'"
            ).fetchone()[0]

            parity_pct = round((lexicon_synced / spotify_total * 100), 2) if spotify_total > 0 else 0.0

            return ParityResponse(
                spotify_total=spotify_total,
                lexicon_synced=lexicon_synced,
                missing=missing,
                mismatched=mismatched,
                lexicon_only=lexicon_only,
                parity_pct=parity_pct,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/errors")
async def get_error_tracks():
    """Get all error tracks grouped by error category."""
    try:
        with get_db() as conn:
            errors = conn.execute(
                """SELECT * FROM tracks WHERE pipeline_stage = 'error'
                   ORDER BY pipeline_error, title"""
            ).fetchall()
            ignored = conn.execute(
                """SELECT * FROM tracks WHERE pipeline_stage = 'ignored'
                   ORDER BY title"""
            ).fetchall()

            categories = {
                "not_lossless": [],
                "no_tidal_match": [],
                "download_failed": [],
                "lexicon_sync_failed": [],
                "fingerprint_mismatch": [],
                "other": [],
            }
            for r in errors:
                t = row_to_track(r)
                err = (t.get("pipeline_error") or "").lower()
                if "not lossless" in err or "aac" in err or "mp3" in err:
                    categories["not_lossless"].append(t)
                elif "no tidal match" in err or "no match" in err or "not found on tidal" in err:
                    categories["no_tidal_match"].append(t)
                elif "download failed" in err or "download error" in err:
                    categories["download_failed"].append(t)
                elif "lexicon" in err:
                    categories["lexicon_sync_failed"].append(t)
                elif "fingerprint" in err:
                    categories["fingerprint_mismatch"].append(t)
                else:
                    categories["other"].append(t)

            return {
                "categories": categories,
                "ignored": [row_to_track(r) for r in ignored],
                "total_errors": len(errors),
                "total_ignored": len(ignored),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/bulk-ignore")
async def bulk_ignore_tracks(track_ids: list[int]):
    """Ignore multiple tracks at once."""
    try:
        with get_db() as conn:
            for track_id in track_ids:
                conn.execute(
                    """UPDATE tracks SET pipeline_stage = 'ignored', is_protected = 1,
                       updated_at = datetime('now') WHERE id = ?""",
                    (track_id,),
                )
                conn.execute(
                    "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                    ("track_ignored", track_id, f"Track {track_id} bulk-ignored by user"),
                )
        return {"status": "ok", "count": len(track_ids)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/{track_id}", response_model=TrackOut)
async def get_track(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/{track_id}/activity")
async def get_track_activity(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            rows = conn.execute(
                "SELECT * FROM activity_log WHERE track_id = ? ORDER BY created_at DESC",
                (track_id,),
            ).fetchall()
            entries = []
            for r in rows:
                entry = dict(r)
                if entry.get("details"):
                    try:
                        entry["details"] = json.loads(entry["details"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                entries.append(entry)
            return {"track_id": track_id, "activity": entries}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/tracks/{track_id}", response_model=TrackOut)
async def update_track(track_id: int, update: TrackUpdate):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            updates = {}
            for field, value in update.model_dump(exclude_unset=True).items():
                if field == "is_protected" and value is not None:
                    updates[field] = int(value)
                else:
                    updates[field] = value

            if updates:
                updates["updated_at"] = "datetime('now')"
                set_parts = []
                params = []
                for k, v in updates.items():
                    if k == "updated_at":
                        set_parts.append(f"{k} = datetime('now')")
                    else:
                        set_parts.append(f"{k} = ?")
                        params.append(v)
                params.append(track_id)
                conn.execute(
                    f"UPDATE tracks SET {', '.join(set_parts)} WHERE id = ?",
                    params,
                )
                conn.execute(
                    "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                    ("track_updated", track_id, f"Track {track_id} updated",
                     json.dumps(update.model_dump(exclude_unset=True))),
                )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/{track_id}/ignore")
async def ignore_track(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")
            conn.execute(
                """UPDATE tracks SET pipeline_stage = 'ignored', is_protected = 1,
                   updated_at = datetime('now') WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("track_ignored", track_id, f"Track {track_id} ignored by user"),
            )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/{track_id}/unignore")
async def unignore_track(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT id FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")
            conn.execute(
                """UPDATE tracks SET pipeline_stage = 'new', is_protected = 0,
                   pipeline_error = NULL, updated_at = datetime('now') WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("track_unignored", track_id, f"Track {track_id} un-ignored by user"),
            )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/{track_id}/retry", response_model=TrackOut)
async def retry_track(track_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            conn.execute(
                """UPDATE tracks SET
                    pipeline_stage = 'new',
                    pipeline_error = NULL,
                    match_status = 'pending',
                    download_status = 'pending',
                    download_error = NULL,
                    download_attempts = 0,
                    verify_status = 'pending',
                    lexicon_status = 'pending',
                    updated_at = datetime('now')
                WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("pipeline_retry", track_id, f"Track {track_id} re-entered pipeline"),
            )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
