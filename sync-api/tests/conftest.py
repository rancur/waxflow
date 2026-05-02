"""
Shared test fixtures for the WaxFlow sync-api test suite.

Sets SLS_DB_PATH to a temporary file before any app imports so that
get_connection() targets the test database throughout the session.
"""
import os
import tempfile

import pytest

# Must be set before importing db or main so DB_PATH is resolved to the temp file.
_db_fd, _db_path = tempfile.mkstemp(suffix=".db")
os.close(_db_fd)
os.environ["SLS_DB_PATH"] = _db_path

import db  # noqa: E402
import init_db  # noqa: E402
from main import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

# Create schema once for the whole session.
init_db.init()


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_db():
    """Wipe transient data before each test; keep app_config defaults."""
    with db.get_db() as conn:
        conn.executescript("""
            DELETE FROM activity_log;
            DELETE FROM fallback_attempts;
            DELETE FROM playlist_tracks;
            DELETE FROM download_queue;
            DELETE FROM tracks;
            DELETE FROM playlists;
        """)
    yield


# ---------------------------------------------------------------------------
# Helpers exposed to individual test modules
# ---------------------------------------------------------------------------

def insert_track(
    spotify_id: str = "sp1",
    title: str = "Test Track",
    artist: str = "Test Artist",
    album: str = "Test Album",
    match_status: str = "pending",
    pipeline_stage: str = "new",
    download_status: str = "pending",
    verify_status: str = "pending",
    lexicon_status: str = "pending",
    tidal_id: str | None = None,
    file_path: str | None = None,
    isrc: str | None = "USTEST000001",
    duration_ms: int = 210000,
) -> int:
    """Insert a minimal track row and return its id."""
    with db.get_db() as conn:
        cur = conn.execute(
            """INSERT INTO tracks
               (spotify_id, title, artist, album, match_status, pipeline_stage,
                download_status, verify_status, lexicon_status, tidal_id, file_path,
                isrc, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                spotify_id, title, artist, album, match_status, pipeline_stage,
                download_status, verify_status, lexicon_status, tidal_id, file_path,
                isrc, duration_ms,
            ),
        )
        return cur.lastrowid
