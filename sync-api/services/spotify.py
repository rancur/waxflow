import os
import time
import spotipy
from spotipy.oauth2 import SpotifyOAuth

from db import get_db


class SpotifyService:
    """Manages Spotify API interactions with token persistence in DB."""

    def __init__(self):
        self.client_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
        self.client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
        self.redirect_uri = os.environ.get(
            "SPOTIFY_REDIRECT_URI", "http://localhost:8402/api/spotify/callback"
        )
        self.scope = "user-library-read"
        self._sp = None

    def _load_token_info(self) -> dict | None:
        with get_db() as conn:
            access = conn.execute(
                "SELECT value FROM app_config WHERE key = 'spotify_access_token'"
            ).fetchone()
            refresh = conn.execute(
                "SELECT value FROM app_config WHERE key = 'spotify_refresh_token'"
            ).fetchone()
            expiry = conn.execute(
                "SELECT value FROM app_config WHERE key = 'spotify_token_expiry'"
            ).fetchone()

        if not access or not access["value"]:
            return None

        return {
            "access_token": access["value"],
            "refresh_token": refresh["value"] if refresh else None,
            "expires_at": int(float(expiry["value"])) if expiry and expiry["value"] else 0,
            "token_type": "Bearer",
            "scope": self.scope,
        }

    def _save_token_info(self, token_info: dict):
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("spotify_access_token", token_info["access_token"]),
            )
            if token_info.get("refresh_token"):
                conn.execute(
                    "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                    ("spotify_refresh_token", token_info["refresh_token"]),
                )
            conn.execute(
                "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                ("spotify_token_expiry", str(token_info.get("expires_at", 0))),
            )

    def _get_client(self) -> spotipy.Spotify:
        if self._sp:
            return self._sp

        token_info = self._load_token_info()
        if not token_info:
            raise RuntimeError("Spotify not authenticated. Visit /api/spotify/auth first.")

        # Check if token needs refresh
        if token_info["expires_at"] < time.time() + 60:
            oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
            )
            refreshed = oauth.refresh_access_token(token_info["refresh_token"])
            if refreshed:
                token_info = refreshed
                self._save_token_info(token_info)

        self._sp = spotipy.Spotify(auth=token_info["access_token"])
        return self._sp

    def get_token_status(self) -> dict:
        token_info = self._load_token_info()
        if not token_info:
            return {"authenticated": False, "valid": False}

        valid = token_info["expires_at"] > time.time()
        return {
            "authenticated": True,
            "valid": valid,
            "expires_at": token_info["expires_at"],
        }

    def get_liked_songs(self, since: str | None = None) -> list[dict]:
        """Fetch all liked songs, optionally filtering by added_at > since."""
        sp = self._get_client()
        all_items = []
        offset = 0
        limit = 50

        while True:
            results = sp.current_user_saved_tracks(limit=limit, offset=offset)
            items = results.get("items", [])
            if not items:
                break

            for item in items:
                # If we have a since timestamp and this track was added before it,
                # we can stop (Spotify returns newest first)
                if since and item.get("added_at", "") <= since:
                    return all_items

                all_items.append(item)

            if not results.get("next"):
                break

            offset += limit

        return all_items
