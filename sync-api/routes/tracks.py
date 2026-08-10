import json
import math
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from db import get_db
from error_categories import ERROR_CATEGORIES, canonical_category, categorize_error
from models import (
    BulkRetryRequest,
    TrackOut,
    TrackUpdate,
    TrackListResponse,
    ParityResponse,
)

router = APIRouter(prefix="/api", tags=["tracks"])

# Putting a track back at the head of the pipeline. Shared by the single-track and
# bulk retry paths so they can never reset different sets of columns.
_RETRY_RESET_SQL = """
    UPDATE tracks SET
        pipeline_stage = 'new',
        pipeline_error = NULL,
        match_status = 'pending',
        download_status = 'pending',
        download_error = NULL,
        download_attempts = 0,
        verify_status = 'pending',
        lexicon_status = 'pending',
        updated_at = datetime('now')
    WHERE id = ?
"""


def _clear_retry_blockers(conn, track_ids: list[int]) -> None:
    """Remove the rows that would otherwise make a retry a no-op.

    soulseek_fallback.already_attempted() treats ANY fallback_attempts row --
    including a finalised, failed one -- as "we tried this already" and refuses to
    re-queue the track. So resetting the pipeline columns alone produces a track
    that marches straight back to the same error without ever re-contacting
    Soulseek. Clearing the attempt history is what makes a retry mean anything.
    """
    if not track_ids:
        return
    conn.executemany(
        "DELETE FROM fallback_attempts WHERE track_id = ?",
        [(tid,) for tid in track_ids],
    )
    conn.executemany(
        "DELETE FROM source_attempts WHERE track_id = ?",
        [(tid,) for tid in track_ids],
    )


def row_to_track(row) -> dict:
    d = dict(row)
    if "is_protected" in d:
        d["is_protected"] = bool(d["is_protected"])
    if "verify_is_genuine_lossless" in d and d["verify_is_genuine_lossless"] is not None:
        d["verify_is_genuine_lossless"] = bool(d["verify_is_genuine_lossless"])
    return d


ALLOWED_SORT_COLUMNS = {
    "title", "artist", "album", "duration_ms", "spotify_added_at",
    "pipeline_stage", "match_status", "download_status", "verify_status",
    "verify_codec", "lexicon_status", "match_confidence",
    "verify_sample_rate", "verify_bit_depth",
}


@router.get("/tracks", response_model=TrackListResponse)
async def list_tracks(
    status: Optional[str] = Query(None, description="Filter by match_status"),
    pipeline_stage: Optional[str] = Query(None, description="Filter by pipeline_stage"),
    download_status: Optional[str] = Query(None, description="Filter by download_status"),
    verify_status: Optional[str] = Query(None, description="Filter by verify_status"),
    lexicon_status: Optional[str] = Query(None, description="Filter by lexicon_status"),
    search: Optional[str] = Query(None, description="Search title/artist/album"),
    month: Optional[str] = Query(
        None, pattern=r"^\d{4}-\d{2}$",
        description="Filter by Spotify added month, YYYY-MM (dashboard drill-down)",
    ),
    playlist_id: Optional[int] = Query(None, description="Filter by playlist"),
    sort_by: Optional[str] = Query(None, description="Column to sort by"),
    sort_dir: Optional[str] = Query("desc", description="Sort direction: asc or desc"),
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
            if download_status:
                conditions.append("t.download_status = ?")
                params.append(download_status)
            if verify_status:
                conditions.append("t.verify_status = ?")
                params.append(verify_status)
            if lexicon_status:
                conditions.append("t.lexicon_status = ?")
                params.append(lexicon_status)
            if search:
                conditions.append("(t.title LIKE ? OR t.artist LIKE ? OR t.album LIKE ?)")
                like = f"%{search}%"
                params.extend([like, like, like])
            if month:
                # Half-open range rather than substr(spotify_added_at, 1, 7) = ?, so
                # the index on spotify_added_at can actually be used. The regex on the
                # query param is what makes this string arithmetic safe.
                year, mon = int(month[:4]), int(month[5:7])
                next_month = f"{year + 1:04d}-01" if mon == 12 else f"{year:04d}-{mon + 1:02d}"
                conditions.append("t.spotify_added_at >= ? AND t.spotify_added_at < ?")
                params.extend([f"{month}-01", f"{next_month}-01"])

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

            # Determine sort column (whitelist to prevent SQL injection)
            order_col = "t.spotify_added_at"
            if sort_by and sort_by in ALLOWED_SORT_COLUMNS:
                order_col = f"t.{sort_by}"
            order_dir = "ASC" if sort_dir and sort_dir.lower() == "asc" else "DESC"

            query = f"""
                SELECT t.* FROM tracks t {join_clause} {where}
                ORDER BY {order_col} {order_dir}
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

            categories: dict[str, list] = {key: [] for key in ERROR_CATEGORIES}
            for r in errors:
                t = row_to_track(r)
                categories[categorize_error(t)].append(t)

            return {
                "categories": categories,
                "ignored": [row_to_track(r) for r in ignored],
                "total_errors": len(errors),
                "total_ignored": len(ignored),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks/errors/summary")
async def get_error_summary():
    """Counts only, for the nav badge.

    The layout polls for a badge number every 30s; it used to call
    /tracks/errors, which returns every errored track in full.
    """
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT pipeline_error, verify_status, verify_codec
                   FROM tracks WHERE pipeline_stage = 'error'"""
            ).fetchall()
            ignored = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE pipeline_stage = 'ignored'"
            ).fetchone()[0]

            by_category = {key: 0 for key in ERROR_CATEGORIES}
            for r in rows:
                by_category[categorize_error(dict(r))] += 1

            return {
                "total_errors": len(rows),
                "total_ignored": ignored,
                "by_category": by_category,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/bulk-ignore")
async def bulk_ignore_tracks(track_ids: list[int]):
    """Ignore multiple tracks at once."""
    try:
        with get_db() as conn:
            conn.executemany(
                """UPDATE tracks SET pipeline_stage = 'ignored', is_protected = 1,
                   updated_at = datetime('now') WHERE id = ?""",
                [(tid,) for tid in track_ids],
            )
            # One summary row, not one per track: bulk-ignoring a category used to
            # insert thousands of activity rows and bury the dashboard feed.
            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                (
                    "pipeline_bulk_ignore",
                    f"{len(track_ids)} track(s) bulk-ignored by user",
                    json.dumps({"track_ids": track_ids[:500], "count": len(track_ids)}),
                ),
            )
        return {"status": "ok", "count": len(track_ids)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tracks/bulk-retry")
async def bulk_retry_tracks(payload: BulkRetryRequest):
    """Re-enter the pipeline for many tracks at once.

    Accepts either explicit ids or a category name. Category mode resolves the
    membership SERVER-SIDE using the same classifier the Errors page renders with,
    so "Retry All 47" retries exactly those 47 -- and the client never has to POST
    thousands of ids.

    Protected/ignored tracks are skipped: ignoring something is a deliberate user
    decision and a bulk retry should not silently undo it.
    """
    try:
        with get_db() as conn:
            if payload.track_ids:
                rows = conn.execute(
                    f"""SELECT * FROM tracks
                        WHERE id IN ({','.join('?' * len(payload.track_ids))})""",
                    payload.track_ids,
                ).fetchall()
            elif payload.category:
                rows = conn.execute(
                    "SELECT * FROM tracks WHERE pipeline_stage = 'error'"
                ).fetchall()
            else:
                raise HTTPException(
                    status_code=400, detail="Provide either track_ids or category"
                )

            wanted = canonical_category(payload.category) if payload.category else None
            if wanted and wanted not in ERROR_CATEGORIES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown category '{payload.category}'. "
                           f"Expected one of: {', '.join(ERROR_CATEGORIES)}",
                )

            eligible, skipped = [], 0
            for r in rows:
                t = row_to_track(r)
                if t.get("is_protected") or t.get("pipeline_stage") == "ignored":
                    skipped += 1
                    continue
                if wanted and categorize_error(t) != wanted:
                    continue
                eligible.append(t["id"])

            eligible = eligible[: payload.limit]
            if eligible:
                conn.executemany(_RETRY_RESET_SQL, [(tid,) for tid in eligible])
                _clear_retry_blockers(conn, eligible)
                conn.execute(
                    "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                    (
                        "pipeline_bulk_retry",
                        f"{len(eligible)} track(s) re-entered the pipeline"
                        + (f" (category: {wanted})" if wanted else ""),
                        json.dumps(
                            {
                                "track_ids": eligible[:500],
                                "count": len(eligible),
                                "category": wanted,
                            }
                        ),
                    ),
                )

        return {"status": "ok", "count": len(eligible), "skipped": skipped}
    except HTTPException:
        raise
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

            conn.execute(_RETRY_RESET_SQL, (track_id,))
            _clear_retry_blockers(conn, [track_id])
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
