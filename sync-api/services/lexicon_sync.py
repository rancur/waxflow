import os
import json
import calendar
from datetime import datetime, timezone
import httpx

from db import get_db

LEXICON_API = os.environ.get("LEXICON_API_URL", "http://192.168.1.116:48624")


class LexiconSyncService:
    """Manages all interactions with the Lexicon DJ API."""

    def __init__(self):
        self.base_url = LEXICON_API
        self.timeout = 30.0

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.request(method, f"{self.base_url}{path}", **kwargs)
            resp.raise_for_status()
            return resp

    async def get_playlists(self) -> list[dict]:
        """Fetch the full playlist tree from Lexicon."""
        resp = await self._request("GET", "/v1/playlists")
        data = resp.json()
        # Lexicon returns {data: {playlists: [{id:1, name:"ROOT", playlists: [...]}]}}
        root = data.get("data", {}).get("playlists", [])
        if root and root[0].get("name") == "ROOT":
            return root[0].get("playlists", [])
        return root

    async def get_tracks(self, playlist_id: str | None = None) -> list[dict]:
        """Fetch tracks, optionally filtered by playlist."""
        params = {}
        if playlist_id:
            params["playlistId"] = playlist_id
        resp = await self._request("GET", "/v1/tracks", params=params)
        return resp.json()

    async def search_tracks(self, artist: str, title: str) -> list[dict]:
        """Search Lexicon for tracks by artist and title."""
        # Lexicon API uses filter[field] format
        params = {"filter[artist]": artist, "filter[title]": title}
        resp = await self._request("GET", "/v1/search/tracks", params=params)
        data = resp.json()
        return data.get("data", {}).get("tracks", [])

    async def create_playlist(self, parent_id: str | int | None, name: str) -> str:
        """Create a new playlist in Lexicon. Returns playlist ID."""
        body = {"name": name, "type": "2"}  # type 2 = playlist
        if parent_id:
            body["parentId"] = int(parent_id)
        resp = await self._request("POST", "/v1/playlist", json=body)
        data = resp.json()
        return str(data.get("data", {}).get("id", data.get("id", "")))

    async def create_folder(self, parent_id: int | None, name: str) -> str:
        """Create a folder in Lexicon. Returns folder ID."""
        body = {"name": name, "type": "1", "folderType": "1"}  # type 1 = folder
        if parent_id:
            body["parentId"] = int(parent_id)
        resp = await self._request("POST", "/v1/playlist", json=body)
        data = resp.json()
        return str(data.get("data", {}).get("id", data.get("id", "")))

    async def add_track_to_playlist(self, playlist_id: str, track_id: str):
        """Add a track to a playlist in Lexicon."""
        await self._request(
            "POST", f"/v1/playlist/{playlist_id}/tracks",
            json={"trackIds": [int(track_id)]},
        )

    async def import_track(self, file_path: str, metadata: dict) -> dict:
        """Import a new track file into Lexicon by creating/updating track entry."""
        # Lexicon PATCH /v1/track expects {id, edits: {...}} for updates
        # For new tracks, we use POST or the import mechanism
        body = {
            "location": file_path,
            "title": metadata.get("title", ""),
            "artist": metadata.get("artist", ""),
            "albumTitle": metadata.get("album", ""),
            "genre": metadata.get("genre", ""),
        }
        try:
            resp = await self._request("POST", "/v1/track", json=body)
            return resp.json()
        except Exception:
            # Fallback: try PATCH if POST not supported
            resp = await self._request("PATCH", "/v1/track", json=body)
            return resp.json()

    async def update_track(self, track_id: str, edits: dict):
        """Update a track's fields using Lexicon's PATCH format."""
        # Lexicon uses: PATCH /v1/track with {id: trackId, edits: {field: value}}
        await self._request(
            "PATCH", "/v1/track",
            json={"id": int(track_id), "edits": edits},
        )

    async def update_track_comment(self, track_id: str, comment: str):
        """Update a track's comment field (used for [sls:spotify_id] tagging)."""
        await self.update_track(track_id, {"comment": comment})

    async def backup(self) -> dict:
        """Trigger a Lexicon backup. Returns backup info."""
        # Try Lexicon's backup endpoint if available
        try:
            resp = await self._request("POST", "/v1/backup")
            return resp.json()
        except Exception:
            # Fallback: record that we attempted
            return {
                "path": "manual_backup_required",
                "size": 0,
                "note": "Lexicon backup API not available, manual backup recommended",
            }

    async def get_or_create_month_playlist(self, year: int, month: int) -> dict:
        """
        Ensure a year folder and month playlist exist.
        Playlist name format: "03. March 2026"
        Returns dict with folder_id and playlist_id.
        """
        month_name = calendar.month_name[month]
        folder_name = str(year)
        playlist_name = f"{month:02d}. {month_name} {year}"

        # Search existing playlists
        playlists = await self.get_playlists()

        folder_id = None
        playlist_id = None

        # Find or create year folder
        # Lexicon tree: folders have type="1", playlists have type="2"
        if isinstance(playlists, list):
            for pl in playlists:
                if pl.get("name") == folder_name and pl.get("type") == "1":
                    folder_id = pl.get("id")
                    # Look for month playlist inside folder's children
                    children = pl.get("playlists", [])
                    for child in children:
                        if child.get("name") == playlist_name:
                            playlist_id = child.get("id")
                            break
                    break

        if not folder_id:
            folder_id = await self.create_folder(1, folder_name)  # parentId=1 is ROOT

        if not playlist_id:
            playlist_id = await self.create_playlist(str(folder_id), playlist_name)

        # Persist to local DB
        with get_db() as conn:
            existing = conn.execute(
                "SELECT id FROM playlists WHERE year = ? AND month = ?",
                (year, month),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE playlists SET
                        lexicon_folder_id = ?, lexicon_playlist_id = ?
                    WHERE year = ? AND month = ?""",
                    (folder_id, playlist_id, year, month),
                )
            else:
                conn.execute(
                    """INSERT INTO playlists
                       (folder_name, playlist_name, year, month, lexicon_folder_id, lexicon_playlist_id)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (folder_name, playlist_name, year, month, folder_id, playlist_id),
                )

        return {
            "folder_id": folder_id,
            "playlist_id": playlist_id,
            "folder_name": folder_name,
            "playlist_name": playlist_name,
        }
