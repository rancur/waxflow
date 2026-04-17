"""Tests for /api/admin/health endpoint."""

import json
import os
import shutil
import sys
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Add sync-api root to path so imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app  # noqa: E402

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn(rows: dict[str, str | None]):
    """Return a context-manager that yields a fake DB connection."""
    conn = MagicMock()

    def fetchone_for(key):
        val = rows.get(key)
        if val is None:
            return None
        return (val,)

    def execute_side(sql, *args, **kwargs):
        cursor = MagicMock()
        if "SELECT 1" in sql:
            cursor.fetchone.return_value = (1,)
        elif "spotify_access_token" in sql:
            cursor.fetchone.return_value = fetchone_for("spotify_access_token")
        elif "spotify_token_expiry" in sql:
            cursor.fetchone.return_value = fetchone_for("spotify_token_expiry")
        else:
            cursor.fetchone.return_value = None
        return cursor

    conn.execute.side_effect = execute_side
    return conn


@contextmanager
def _fake_db(rows: dict[str, str | None]):
    yield _mock_conn(rows)


def _disk_usage(total, free):
    return shutil.disk_usage.__class__  # placeholder — we use namedtuple below


# ---------------------------------------------------------------------------
# Tests — database subsystem
# ---------------------------------------------------------------------------

class TestHealthDatabase:
    def test_db_ok(self):
        rows = {"spotify_access_token": None}
        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", return_value=False), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 100, "total": 100})() ):
            resp = client.get("/api/admin/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["database"]["status"] == "ok"
        assert body["database"]["latency_ms"] is not None

    def test_db_error(self):
        def _fail():
            raise RuntimeError("disk full")
            yield  # make it a generator so contextmanager works

        from contextlib import contextmanager as cm

        @cm
        def _err_db():
            raise RuntimeError("disk full")
            yield  # unreachable but satisfies generator

        with patch("routes.admin.get_db", side_effect=_err_db), \
             patch("os.path.exists", return_value=False), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 100, "total": 100})()):
            resp = client.get("/api/admin/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["database"]["status"] == "error"
        assert body["status"] == "error"


# ---------------------------------------------------------------------------
# Tests — Spotify subsystem
# ---------------------------------------------------------------------------

class TestHealthSpotify:
    def _call(self, rows, tidal_exists=False, disk_ok=True):
        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", return_value=tidal_exists), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 90, "total": 100})()):
            return client.get("/api/admin/health").json()

    def test_no_token_is_unauthenticated(self):
        body = self._call({"spotify_access_token": None, "spotify_token_expiry": None})
        assert body["spotify"]["status"] == "unauthenticated"
        assert body["status"] == "degraded"

    def test_valid_token(self):
        future = str(int(time.time()) + 3600)
        body = self._call({
            "spotify_access_token": "tok",
            "spotify_token_expiry": future,
        })
        assert body["spotify"]["status"] == "ok"

    def test_expired_token(self):
        past = str(int(time.time()) - 100)
        body = self._call({
            "spotify_access_token": "tok",
            "spotify_token_expiry": past,
        })
        assert body["spotify"]["status"] == "expired"
        assert body["status"] == "degraded"

    def test_expiring_soon_token(self):
        soon = str(int(time.time()) + 60)
        body = self._call({
            "spotify_access_token": "tok",
            "spotify_token_expiry": soon,
        })
        assert body["spotify"]["status"] == "expiring_soon"


# ---------------------------------------------------------------------------
# Tests — Tidal subsystem
# ---------------------------------------------------------------------------

class TestHealthTidal:
    def _make_auth(self, expires_at: int) -> str:
        return json.dumps({"token": "t", "expires_at": expires_at})

    def _call(self, auth_content=None):
        rows = {"spotify_access_token": "tok", "spotify_token_expiry": str(int(time.time()) + 3600)}
        tidal_exists = auth_content is not None

        def fake_exists(path):
            return tidal_exists and path == "/app/data/tiddl-auth.json"

        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", side_effect=fake_exists), \
             patch("builtins.open", MagicMock(
                 return_value=MagicMock(
                     __enter__=lambda s: MagicMock(read=lambda: auth_content),
                     __exit__=MagicMock(return_value=False),
                 )
             ) if tidal_exists else patch("os.path.exists")), \
             patch("json.load", return_value=json.loads(auth_content) if auth_content else {}), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 90, "total": 100})()):
            return client.get("/api/admin/health").json()

    def test_no_auth_file(self):
        rows = {"spotify_access_token": "tok", "spotify_token_expiry": str(int(time.time()) + 3600)}
        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", return_value=False), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 90, "total": 100})()):
            body = client.get("/api/admin/health").json()
        assert body["tidal"]["status"] == "unauthenticated"

    def test_valid_tidal_token(self):
        future = int(time.time()) + 7200
        auth = json.dumps({"token": "t", "expires_at": future})
        body = self._call(auth)
        assert body["tidal"]["status"] == "ok"

    def test_expired_tidal_token(self):
        past = int(time.time()) - 100
        auth = json.dumps({"token": "t", "expires_at": past})
        body = self._call(auth)
        assert body["tidal"]["status"] == "expired"


# ---------------------------------------------------------------------------
# Tests — Disk subsystem
# ---------------------------------------------------------------------------

class TestHealthDisk:
    def _call_with_disk(self, free, total):
        rows = {"spotify_access_token": None}
        usage = type("u", (), {"free": free, "total": total})()
        db_dir = os.path.dirname(os.environ.get("SLS_DB_PATH", "/app/data/sync.db"))

        def fake_exists(path):
            # Return True for db_dir so disk check runs, False for tidal auth paths
            return path == db_dir

        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", side_effect=fake_exists), \
             patch("shutil.disk_usage", return_value=usage):
            return client.get("/api/admin/health").json()

    def test_ample_disk(self):
        body = self._call_with_disk(free=80, total=100)
        assert body["disk"]["status"] == "ok"

    def test_low_disk_degraded(self):
        body = self._call_with_disk(free=10, total=100)
        assert body["disk"]["status"] == "degraded"

    def test_critical_disk_error(self):
        body = self._call_with_disk(free=3, total=100)
        assert body["disk"]["status"] == "error"
        assert body["status"] == "error"


# ---------------------------------------------------------------------------
# Tests — response shape
# ---------------------------------------------------------------------------

class TestHealthResponseShape:
    def test_required_fields_present(self):
        rows = {"spotify_access_token": None}
        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", return_value=False), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 90, "total": 100})()):
            resp = client.get("/api/admin/health")
        assert resp.status_code == 200
        body = resp.json()
        for key in ("status", "uptime_seconds", "database", "spotify", "tidal", "disk"):
            assert key in body, f"missing key: {key}"
        for sub in ("database", "spotify", "tidal", "disk"):
            assert "status" in body[sub], f"missing status in {sub}"

    def test_uptime_is_positive(self):
        rows = {"spotify_access_token": None}
        with patch("routes.admin.get_db", side_effect=lambda: _fake_db(rows)), \
             patch("os.path.exists", return_value=False), \
             patch("shutil.disk_usage", return_value=type("u", (), {"free": 90, "total": 100})()):
            body = client.get("/api/admin/health").json()
        assert body["uptime_seconds"] >= 0
