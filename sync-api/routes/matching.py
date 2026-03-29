import json
import re
import unicodedata
from fastapi import APIRouter, HTTPException

from db import get_db
from models import TrackOut, ManualMatchRequest
from routes.tracks import row_to_track

router = APIRouter(prefix="/api/matching", tags=["matching"])


def _normalize(s: str | None) -> str:
    """Normalize a string for comparison: lowercase, strip accents, remove punctuation."""
    if not s:
        return ""
    s = s.lower().strip()
    # Remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Remove common suffixes/parentheticals for better matching
    s = re.sub(r"\s*\(.*?\)\s*", " ", s)
    s = re.sub(r"\s*\[.*?\]\s*", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _similarity(a: str | None, b: str | None) -> str:
    """Compare two strings and return 'exact', 'partial', or 'different'."""
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return "unknown"
    if na == nb:
        return "exact"
    # Check if one contains the other
    if na in nb or nb in na:
        return "partial"
    # Check word overlap
    words_a = set(na.split())
    words_b = set(nb.split())
    if words_a and words_b:
        overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
        if overlap >= 0.5:
            return "partial"
    return "different"


@router.get("/review")
async def review_matches():
    """Return mismatched tracks with rich comparison data for side-by-side review."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM tracks WHERE match_status = 'mismatched' ORDER BY match_confidence ASC, updated_at DESC"
            ).fetchall()

            # Fetch stats for summary cards
            stats_row = conn.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE match_status = 'mismatched') AS total_mismatched,
                    COUNT(*) FILTER (WHERE match_status = 'mismatched' AND pipeline_stage NOT IN ('complete')) AS pending_review,
                    0 AS approved,
                    0 AS rejected
                FROM tracks
            """).fetchone()

            # SQLite doesn't support FILTER — use CASE instead
            stats_row = conn.execute("""
                SELECT
                    SUM(CASE WHEN match_status = 'mismatched' THEN 1 ELSE 0 END) AS total_mismatched,
                    SUM(CASE WHEN match_status = 'mismatched' THEN 1 ELSE 0 END) AS pending_review
                FROM tracks
            """).fetchone()

            # Get approved/rejected counts from activity log
            action_counts = conn.execute("""
                SELECT
                    SUM(CASE WHEN event_type = 'match_approved' THEN 1 ELSE 0 END) AS approved,
                    SUM(CASE WHEN event_type = 'match_rejected' THEN 1 ELSE 0 END) AS rejected
                FROM activity_log
            """).fetchone()

            stats = {
                "total_mismatched": stats_row["total_mismatched"] or 0 if stats_row else 0,
                "pending_review": stats_row["pending_review"] or 0 if stats_row else 0,
                "approved": action_counts["approved"] or 0 if action_counts else 0,
                "rejected": action_counts["rejected"] or 0 if action_counts else 0,
            }

            tracks = []
            for r in rows:
                t = row_to_track(r)
                track_out = TrackOut(**t)

                # Compute duration difference
                spotify_duration_s = round((t.get("duration_ms") or 0) / 1000.0, 1)
                # Try to extract file duration from verify data or fingerprint
                # For now, we don't have a separate file_duration column, so this may be None
                file_duration_s = None
                duration_diff_seconds = None
                if spotify_duration_s and file_duration_s:
                    duration_diff_seconds = round(abs(spotify_duration_s - file_duration_s), 1)

                # Compute similarities
                # For matched files, we compare spotify metadata against what was matched
                # The matched file's title/artist come from the file tags or the match source
                matched_title = t.get("title")  # Currently same as spotify title in DB
                matched_artist = t.get("artist")

                title_similarity = _similarity(t.get("title"), matched_title)
                artist_similarity = _similarity(t.get("artist"), matched_artist)

                comparison = {
                    "track": track_out,
                    # Spotify metadata
                    "spotify_title": t.get("title"),
                    "spotify_artist": t.get("artist"),
                    "spotify_album": t.get("album"),
                    "spotify_duration_s": spotify_duration_s,
                    "spotify_duration_ms": t.get("duration_ms"),
                    "spotify_isrc": t.get("isrc"),
                    "spotify_added_at": t.get("spotify_added_at"),
                    "spotify_popularity": t.get("spotify_popularity"),
                    # File metadata
                    "file_path": t.get("file_path"),
                    "verify_codec": t.get("verify_codec"),
                    "verify_sample_rate": t.get("verify_sample_rate"),
                    "verify_bit_depth": t.get("verify_bit_depth"),
                    "verify_is_genuine_lossless": t.get("verify_is_genuine_lossless"),
                    # Match metadata
                    "match_source": t.get("match_source"),
                    "match_confidence": t.get("match_confidence"),
                    "tidal_id": t.get("tidal_id"),
                    "fingerprint_match_score": t.get("fingerprint_match_score"),
                    # Pipeline info
                    "pipeline_stage": t.get("pipeline_stage"),
                    "pipeline_error": t.get("pipeline_error"),
                    "download_status": t.get("download_status"),
                    # Computed comparison fields
                    "duration_diff_seconds": duration_diff_seconds,
                    "title_similarity": title_similarity,
                    "artist_similarity": artist_similarity,
                }
                tracks.append(comparison)

            return {"tracks": tracks, "total": len(tracks), "stats": stats}
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

            # Determine next pipeline stage based on current state
            if row["download_status"] == "complete" and row["verify_status"] in ("pass", "complete"):
                next_stage = "organizing"
            elif row["download_status"] == "complete":
                next_stage = "verifying"
            else:
                next_stage = "downloading"

            conn.execute(
                """UPDATE tracks SET
                    match_status = 'matched',
                    pipeline_stage = ?,
                    updated_at = datetime('now')
                WHERE id = ?""",
                (next_stage, track_id),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("match_approved", track_id, f"Match approved for track {track_id} — advancing to {next_stage}"),
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
                    fingerprint_match_score = NULL,
                    file_path = NULL,
                    pipeline_stage = 'new',
                    pipeline_error = NULL,
                    updated_at = datetime('now')
                WHERE id = ?""",
                (track_id,),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("match_rejected", track_id, f"Match rejected for track {track_id} — re-entering pipeline from new"),
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


@router.post("/{track_id}/skip")
async def skip_match(track_id: int):
    """Skip a track for now — no state change, just acknowledge."""
    return {"status": "skipped", "track_id": track_id}


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
