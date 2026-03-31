import os
import json
import httpx
from datetime import datetime, timezone

from db import get_db

TIDARR_API = os.environ.get("TIDARR_URL", "http://localhost:8484")  # optional legacy fallback
MUSIC_PATH = os.environ.get("MUSIC_LIBRARY_PATH", "/music")


class DownloaderService:
    """Manages track downloads via tiddl CLI (primary) with optional Tidarr fallback."""

    def __init__(self):
        self.tidarr_url = TIDARR_API
        self.music_path = MUSIC_PATH

    async def download_track(self, track: dict, tidal_id: str) -> dict:
        """
        Queue a download via Tidal. Returns status dict.
        NOTE: This legacy code path uses the Tidarr API as fallback.
        Primary downloads now go through tiddl CLI in the sync-worker.
        """
        track_id = track["id"]
        now = datetime.now(timezone.utc).isoformat()

        with get_db() as conn:
            # Create or update queue entry
            existing = conn.execute(
                "SELECT id, attempts FROM download_queue WHERE track_id = ?", (track_id,)
            ).fetchone()

            if existing:
                if existing["attempts"] >= 3:
                    return {"status": "max_attempts", "track_id": track_id}
                conn.execute(
                    """UPDATE download_queue SET
                        status = 'downloading', started_at = ?, attempts = attempts + 1
                    WHERE track_id = ?""",
                    (now, track_id),
                )
            else:
                conn.execute(
                    """INSERT INTO download_queue
                       (track_id, priority, source, status, attempts, started_at)
                       VALUES (?, 0, 'tidarr', 'downloading', 1, ?)""",
                    (track_id, now),
                )

            conn.execute(
                "UPDATE tracks SET download_status = 'downloading', download_attempts = download_attempts + 1, updated_at = datetime('now') WHERE id = ?",
                (track_id,),
            )

        # Submit to Tidarr (legacy fallback — primary path uses tiddl CLI in worker)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{self.tidarr_url}/api/download/track",
                    json={"id": tidal_id},
                )

                if resp.status_code in (200, 201, 202):
                    data = resp.json()

                    with get_db() as conn:
                        conn.execute(
                            "UPDATE download_queue SET status = 'queued' WHERE track_id = ?",
                            (track_id,),
                        )
                        conn.execute(
                            "UPDATE tracks SET download_status = 'queued', updated_at = datetime('now') WHERE id = ?",
                            (track_id,),
                        )
                        conn.execute(
                            "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                            ("download_queued", track_id,
                             f"Download queued via Tidal for tidal_id {tidal_id}",
                             json.dumps(data)),
                        )

                    return {"status": "queued", "track_id": track_id, "tidal_response": data}
                else:
                    error = f"Tidal download returned HTTP {resp.status_code}: {resp.text}"
                    await self._mark_failed(track_id, error)
                    return {"status": "failed", "track_id": track_id, "error": error}

        except Exception as e:
            error = str(e)
            await self._mark_failed(track_id, error)
            return {"status": "failed", "track_id": track_id, "error": error}

    async def check_download_status(self, track: dict) -> dict:
        """Check if a download has completed by looking for the file."""
        track_id = track["id"]
        artist = track.get("artist", "Unknown")
        title = track.get("title", "Unknown")
        album = track.get("album", "Unknown")

        # Check common download output paths
        possible_paths = [
            os.path.join(self.music_path, artist, album, f"{title}.flac"),
            os.path.join(self.music_path, artist, f"{title}.flac"),
            os.path.join(self.music_path, artist, album),
        ]

        for path in possible_paths:
            if os.path.isfile(path):
                return await self._mark_complete(track_id, path)
            if os.path.isdir(path):
                # Look for FLAC files in directory
                for fname in os.listdir(path):
                    if fname.lower().endswith(".flac"):
                        full = os.path.join(path, fname)
                        if title.lower() in fname.lower():
                            return await self._mark_complete(track_id, full)

        return {"status": "pending", "track_id": track_id}

    async def _mark_complete(self, track_id: int, file_path: str) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            conn.execute(
                """UPDATE download_queue SET status = 'complete', completed_at = ?
                   WHERE track_id = ?""",
                (now, track_id),
            )
            conn.execute(
                """UPDATE tracks SET
                    download_status = 'complete', file_path = ?,
                    pipeline_stage = 'verifying', updated_at = datetime('now')
                WHERE id = ?""",
                (file_path, track_id),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message) VALUES (?, ?, ?)",
                ("download_complete", track_id, f"Download complete: {file_path}"),
            )
        return {"status": "complete", "track_id": track_id, "file_path": file_path}

    async def _mark_failed(self, track_id: int, error: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE download_queue SET status = 'failed', error = ? WHERE track_id = ?",
                (error, track_id),
            )
            conn.execute(
                """UPDATE tracks SET
                    download_status = 'failed', download_error = ?,
                    pipeline_stage = 'error', pipeline_error = ?,
                    updated_at = datetime('now')
                WHERE id = ?""",
                (error, error, track_id),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
                ("download_failed", track_id, f"Download failed for track {track_id}",
                 json.dumps({"error": error})),
            )
