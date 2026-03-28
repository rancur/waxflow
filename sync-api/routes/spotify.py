import os
import json
import time
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
import spotipy
from spotipy.oauth2 import SpotifyOAuth

from db import get_db
from models import SpotifyStatusResponse
from services.spotify import SpotifyService

router = APIRouter(prefix="/api/spotify", tags=["spotify"])


def _get_oauth() -> SpotifyOAuth:
    return SpotifyOAuth(
        client_id=os.environ.get("SPOTIFY_CLIENT_ID", ""),
        client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
        redirect_uri=os.environ.get("SPOTIFY_REDIRECT_URI", "http://localhost:8402/api/spotify/callback"),
        scope="user-library-read",
        cache_handler=None,  # We manage tokens in DB
    )


@router.get("/auth")
async def spotify_auth():
    """Start the Spotify OAuth flow."""
    try:
        oauth = _get_oauth()
        auth_url = oauth.get_authorize_url()
        return RedirectResponse(url=auth_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/callback")
async def spotify_callback(code: str = None, error: str = None):
    """Handle the Spotify OAuth callback."""
    if error:
        raise HTTPException(status_code=400, detail=f"Spotify auth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")

    try:
        oauth = _get_oauth()
        token_info = oauth.get_access_token(code, as_dict=True)

        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("spotify_access_token", token_info["access_token"]),
            )
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("spotify_refresh_token", token_info["refresh_token"]),
            )
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("spotify_token_expiry", str(token_info["expires_at"])),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, message) VALUES (?, ?)",
                ("spotify_auth", "Spotify OAuth completed successfully"),
            )

        return {"status": "ok", "message": "Spotify authentication successful. You can close this window."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=SpotifyStatusResponse)
async def spotify_status():
    try:
        with get_db() as conn:
            access_token = conn.execute(
                "SELECT value FROM app_config WHERE key = 'spotify_access_token'"
            ).fetchone()
            token_expiry = conn.execute(
                "SELECT value FROM app_config WHERE key = 'spotify_token_expiry'"
            ).fetchone()
            last_poll = conn.execute(
                "SELECT value FROM app_config WHERE key = 'last_spotify_poll'"
            ).fetchone()

            authenticated = access_token is not None and access_token["value"] != ""
            token_valid = False
            expiry_str = None

            if token_expiry and token_expiry["value"]:
                expiry_str = token_expiry["value"]
                try:
                    token_valid = float(expiry_str) > time.time()
                except (ValueError, TypeError):
                    pass

            return SpotifyStatusResponse(
                authenticated=authenticated,
                token_valid=token_valid,
                token_expiry=expiry_str,
                last_poll=last_poll["value"] if last_poll else None,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/poll")
async def poll_spotify():
    """Manually trigger a Spotify liked songs poll."""
    try:
        svc = SpotifyService()

        # Get last poll time
        with get_db() as conn:
            last_poll_row = conn.execute(
                "SELECT value FROM app_config WHERE key = 'last_spotify_poll'"
            ).fetchone()
            since = last_poll_row["value"] if last_poll_row else None

        liked_songs = svc.get_liked_songs(since=since)

        new_count = 0
        updated_count = 0

        with get_db() as conn:
            for item in liked_songs:
                track_data = item["track"]
                added_at = item["added_at"]
                spotify_id = track_data["id"]

                # Extract ISRC
                isrc = None
                ext_ids = track_data.get("external_ids", {})
                if ext_ids:
                    isrc = ext_ids.get("isrc")

                existing = conn.execute(
                    "SELECT id FROM tracks WHERE spotify_id = ?", (spotify_id,)
                ).fetchone()

                if existing:
                    updated_count += 1
                else:
                    artists = ", ".join(a["name"] for a in track_data.get("artists", []))
                    album_name = track_data.get("album", {}).get("name", "")

                    conn.execute(
                        """INSERT INTO tracks
                           (spotify_id, spotify_uri, spotify_added_at, title, artist, album,
                            duration_ms, isrc, spotify_popularity)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            spotify_id,
                            track_data.get("uri"),
                            added_at,
                            track_data.get("name"),
                            artists,
                            album_name,
                            track_data.get("duration_ms"),
                            isrc,
                            track_data.get("popularity"),
                        ),
                    )
                    new_count += 1

            # Update last poll timestamp
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("last_spotify_poll", now),
            )
            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                ("spotify_poll", f"Polled Spotify: {new_count} new, {updated_count} existing",
                 json.dumps({"new": new_count, "existing": updated_count, "total_fetched": len(liked_songs)})),
            )

        return {
            "status": "ok",
            "new_tracks": new_count,
            "existing_tracks": updated_count,
            "total_fetched": len(liked_songs),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
