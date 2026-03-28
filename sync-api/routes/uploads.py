import os
import json
import hashlib
from fastapi import APIRouter, HTTPException, UploadFile, File

from db import get_db
from models import TrackOut
from routes.tracks import row_to_track

router = APIRouter(prefix="/api/uploads", tags=["uploads"])

UPLOAD_DIR = "/app/data/uploads"


@router.post("/{track_id}", response_model=TrackOut)
async def upload_file(track_id: int, file: UploadFile = File(...)):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Track not found")

            # Validate file type
            if not file.filename or not file.filename.lower().endswith((".flac", ".wav", ".aiff")):
                raise HTTPException(status_code=400, detail="Only FLAC, WAV, and AIFF files are accepted")

            os.makedirs(UPLOAD_DIR, exist_ok=True)

            # Read and hash
            content = await file.read()
            sha256 = hashlib.sha256(content).hexdigest()

            # Save file
            ext = os.path.splitext(file.filename)[1]
            safe_name = f"{track_id}_{sha256[:12]}{ext}"
            file_path = os.path.join(UPLOAD_DIR, safe_name)

            with open(file_path, "wb") as f:
                f.write(content)

            # Update track
            conn.execute(
                """UPDATE tracks SET
                    file_path = ?,
                    file_hash_sha256 = ?,
                    download_status = 'complete',
                    download_source = 'manual_upload',
                    match_status = 'manual',
                    match_source = 'manual_upload',
                    match_confidence = 1.0,
                    pipeline_stage = 'verifying',
                    updated_at = datetime('now')
                WHERE id = ?""",
                (file_path, sha256, track_id),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                ("file_uploaded", track_id, f"File uploaded for track {track_id}",
                 json.dumps({"filename": file.filename, "size": len(content), "sha256": sha256})),
            )

            row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
            return TrackOut(**row_to_track(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
