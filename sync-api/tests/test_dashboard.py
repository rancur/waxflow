"""Endpoint tests for /api/dashboard.

WHY THIS EXISTS
    The dashboard is the app's front page and had no test. A change that added a
    service-health entry reading from app_config shipped a 500 --
    "Cannot operate on a closed database" -- because the service-health section of
    the handler runs AFTER the `with get_db() as conn:` block has exited, so the
    connection still in lexical scope there is already closed.

    Nothing caught it: the unit tests passed, the containers came up healthy, and
    /api/admin/health was fine, because none of them actually requested the page.
    The cheapest possible guard is a test that calls the endpoint and asserts 200.

Self-contained: temp DB seeded with the columns the handler reads, no network --
the outbound Lexicon/Tidal probes are patched out.
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

SYNC_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_API_DIR not in sys.path:
    sys.path.insert(0, SYNC_API_DIR)

_DB = os.path.join(tempfile.mkdtemp(prefix="waxflow-dash-"), "sync.db")
os.environ.setdefault("SLS_DB_PATH", _DB)

import db as db_mod  # noqa: E402
from routes import dashboard as dash_mod  # noqa: E402


def _seed(path: str, *, soulseek: dict | None = None) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY,
            title TEXT, artist TEXT,
            spotify_added_at TEXT,
            pipeline_stage TEXT, match_status TEXT,
            download_status TEXT, verify_status TEXT, lexicon_status TEXT
        );
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY, event_type TEXT, track_id INTEGER,
            message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany(
        """INSERT INTO tracks (id, title, artist, spotify_added_at, pipeline_stage,
                               match_status, download_status, verify_status, lexicon_status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            (1, "A", "X", "2026-03-04T00:00:00Z", "complete", "matched", "complete", "pass", "synced"),
            (2, "B", "X", "2026-03-09T00:00:00Z", "error", "failed", "failed", "fail", "pending"),
            (3, "C", "Y", "2026-04-01T00:00:00Z", "complete", "matched", "complete", "pass", "synced"),
        ],
    )
    conn.execute(
        "INSERT INTO activity_log (event_type, message, details) VALUES (?,?,?)",
        ("pipeline_bulk_retry", "2 tracks retried", json.dumps({"count": 2})),
    )
    for key, value in (soulseek or {}).items():
        conn.execute("INSERT INTO app_config (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


class _NoNetwork:
    """Stand-in for httpx.AsyncClient: every outbound probe simply fails."""

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, *a, **kw):
        raise OSError("no network in tests")


class DashboardEndpointTest(unittest.TestCase):
    soulseek_config: dict = {}

    @classmethod
    def setUpClass(cls):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app = FastAPI()
        app.include_router(dash_mod.router)
        cls.client = TestClient(app)

    def setUp(self):
        self._saved = db_mod.DB_PATH
        db_mod.DB_PATH = _DB
        self.addCleanup(lambda: setattr(db_mod, "DB_PATH", self._saved))
        if os.path.exists(_DB):
            os.remove(_DB)
        _seed(_DB, soulseek=self.soulseek_config)
        patcher = mock.patch.object(dash_mod.httpx, "AsyncClient", _NoNetwork)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _get(self):
        r = self.client.get("/api/dashboard")
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    # -- the regression itself ---------------------------------------------- #

    def test_dashboard_returns_200_not_500(self):
        self._get()

    def test_returns_200_even_when_every_probe_fails(self):
        # Unreachable Lexicon/Tidal must degrade to an "error" service row, never
        # take the whole page down.
        body = self._get()
        by_name = {s["name"]: s for s in body["services"]}
        self.assertEqual(by_name["lexicon"]["status"], "error")

    def test_soulseek_row_is_present(self):
        body = self._get()
        self.assertIn("soulseek", {s["name"] for s in body["services"]})

    # -- content ------------------------------------------------------------ #

    def test_counts_are_correct(self):
        body = self._get()
        self.assertEqual(body["spotify_total"], 3)
        self.assertEqual(body["lexicon_synced"], 2)
        self.assertEqual(body["by_pipeline_stage"]["complete"], 2)
        self.assertEqual(body["by_pipeline_stage"]["error"], 1)

    def test_monthly_endpoint_reconciles_with_the_month_filter(self):
        # The drill-down links here, so these two must agree.
        r = self.client.get("/api/dashboard/monthly")
        self.assertEqual(r.status_code, 200, r.text)
        months = {m["month"]: m for m in r.json()["months"]}
        self.assertEqual(months["2026-03"]["total"], 2)
        self.assertEqual(months["2026-03"]["complete"], 1)
        self.assertEqual(months["2026-03"]["errors"], 1)


class SoulseekHealthRowTest(DashboardEndpointTest):
    """The soulseek row reflects whatever the worker last persisted."""

    soulseek_config = {
        "soulseek_health": "ok",
        "soulseek_detail": "connected to vps.slsknet.org",
        "soulseek_latency_ms": "67.5",
    }

    def test_ok_is_reported_with_latency(self):
        body = self._get()
        row = next(s for s in body["services"] if s["name"] == "soulseek")
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["latency_ms"], 67.5)

    def test_no_recorded_check_reads_as_unknown_not_ok(self):
        conn = sqlite3.connect(_DB)
        conn.execute("DELETE FROM app_config WHERE key = 'soulseek_health'")
        conn.commit()
        conn.close()
        body = self._get()
        row = next(s for s in body["services"] if s["name"] == "soulseek")
        self.assertEqual(row["status"], "unknown")

    def test_stale_check_does_not_keep_reporting_ok(self):
        # A worker that died must not leave a permanently green light.
        conn = sqlite3.connect(_DB)
        conn.execute(
            "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
            ("soulseek_checked_at", "2020-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()
        body = self._get()
        row = next(s for s in body["services"] if s["name"] == "soulseek")
        self.assertEqual(row["status"], "unknown")
        self.assertIn("stalled", (row["error"] or ""))

    def test_disabled_is_not_an_error(self):
        conn = sqlite3.connect(_DB)
        conn.execute(
            "INSERT OR REPLACE INTO app_config (key, value) VALUES ('soulseek_health', 'disabled')"
        )
        conn.commit()
        conn.close()
        body = self._get()
        row = next(s for s in body["services"] if s["name"] == "soulseek")
        self.assertEqual(row["status"], "disabled")


if __name__ == "__main__":
    unittest.main()
