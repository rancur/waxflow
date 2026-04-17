"""Shared pytest fixtures for sync-api tests."""

import sqlite3
import sys
from pathlib import Path

# Make sync-api modules importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """Provision a fresh temp SQLite DB for each test and patch db.DB_PATH."""
    path = str(tmp_path / "test.db")
    import db as db_module
    monkeypatch.setattr(db_module, "DB_PATH", path)
    monkeypatch.setenv("SLS_DB_PATH", path)
    import init_db
    init_db.init()
    return path


@pytest.fixture
def client(db_path):
    """Return a TestClient wired to the FastAPI app with a fresh test DB."""
    from main import app
    with TestClient(app) as c:
        yield c


def make_track(conn: sqlite3.Connection, **kwargs) -> int:
    """Insert a minimal track row and return its id."""
    defaults = {
        "spotify_id": "sp_test_001",
        "title": "Test Track",
        "artist": "Test Artist",
        "album": "Test Album",
        "duration_ms": 180000,
        "isrc": "USTEST000001",
        "pipeline_stage": "new",
        "match_status": "pending",
        "download_status": "pending",
        "verify_status": "pending",
        "lexicon_status": "pending",
    }
    defaults.update(kwargs)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join("?" * len(defaults))
    conn.execute(
        f"INSERT INTO tracks ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
