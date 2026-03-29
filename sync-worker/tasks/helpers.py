"""Shared helper functions for the sync worker."""

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime

import spotipy
from spotipy.oauth2 import SpotifyOAuth

log = logging.getLogger("worker.helpers")

TIDARR_URL = os.environ.get("TIDARR_URL", "http://tidarr:8484")
LEXICON_API_URL = os.environ.get("LEXICON_API_URL", "http://192.168.1.116:48624")
MUSIC_LIBRARY_PATH = os.environ.get("MUSIC_LIBRARY_PATH", "/music")


@contextmanager
def get_db(db_path: str):
    """Context manager for SQLite connection with WAL mode."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_config(db_path: str, key: str) -> str | None:
    """Read a single config value from app_config."""
    with get_db(db_path) as conn:
        row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None


def set_config(db_path: str, key: str, value: str):
    """Write a config value to app_config (upsert)."""
    with get_db(db_path) as conn:
        conn.execute(
            "INSERT INTO app_config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, value, value),
        )


def log_activity(db_path: str, event_type: str, track_id: int | None, message: str, details: dict | None = None):
    """Insert an entry into activity_log."""
    with get_db(db_path) as conn:
        conn.execute(
            "INSERT INTO activity_log (event_type, track_id, message, details) VALUES (?, ?, ?, ?)",
            (event_type, track_id, message, json.dumps(details) if details else None),
        )


def update_track(db_path: str, track_id: int, **fields):
    """Update arbitrary fields on a track row."""
    if not fields:
        return
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [track_id]
    with get_db(db_path) as conn:
        conn.execute(
            f"UPDATE tracks SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
            values,
        )


def get_tracks_by_stage(db_path: str, stage: str, limit: int = 10) -> list[dict]:
    """Fetch tracks at a given pipeline_stage.

    Includes a 60-second timing guard: tracks updated in the last 60 seconds
    are skipped to prevent two pipeline cycles from processing the same track.
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM tracks
            WHERE pipeline_stage = ?
              AND (updated_at IS NULL OR updated_at < datetime('now', '-60 seconds'))
            ORDER BY created_at ASC LIMIT ?""",
            (stage, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_spotify_client(db_path: str) -> spotipy.Spotify | None:
    """Create an authenticated Spotipy client from stored tokens.
    Returns None if no tokens are available.
    """
    access_token = get_config(db_path, "spotify_access_token")
    refresh_token = get_config(db_path, "spotify_refresh_token")
    token_expiry = get_config(db_path, "spotify_token_expiry")

    if not access_token:
        return None

    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/spotify/callback")

    # Check if token is expired and refresh if needed
    if token_expiry:
        try:
            expiry_ts = int(token_expiry)
            if time.time() >= expiry_ts - 60:  # refresh 60s before expiry
                access_token = refresh_spotify_token(db_path)
                if not access_token:
                    return None
        except (ValueError, TypeError):
            pass

    return spotipy.Spotify(auth=access_token)


def refresh_spotify_token(db_path: str) -> str | None:
    """Refresh the Spotify access token using the stored refresh token.
    Updates token in DB and returns the new access token.
    """
    refresh_token = get_config(db_path, "spotify_refresh_token")
    if not refresh_token:
        log.warning("No refresh token available")
        return None

    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/spotify/callback")

    if not client_id or not client_secret:
        log.error("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set")
        return None

    try:
        sp_oauth = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope="user-library-read",
        )
        token_info = sp_oauth.refresh_access_token(refresh_token)
        new_access = token_info["access_token"]
        new_refresh = token_info.get("refresh_token", refresh_token)
        new_expiry = str(token_info.get("expires_at", int(time.time()) + 3600))

        set_config(db_path, "spotify_access_token", new_access)
        set_config(db_path, "spotify_refresh_token", new_refresh)
        set_config(db_path, "spotify_token_expiry", new_expiry)

        log.info("Spotify token refreshed successfully")
        return new_access
    except Exception as e:
        log.error(f"Failed to refresh Spotify token: {e}")
        return None


def sanitize_filename(name: str) -> str:
    """Remove or replace characters that are problematic in filenames."""
    bad_chars = '<>:"/\\|?*'
    result = name
    for c in bad_chars:
        result = result.replace(c, "_")
    return result.strip(". ")
