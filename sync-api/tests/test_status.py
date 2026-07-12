"""Tests for WaxFlow v3 Feature 7 — the read-only health/parity dashboard.

Proves behavior (not just shape):
  * build_status computes the CORRECT aggregates on a fully-seeded DB
    (parity %, currently-sourcing count/stages, wanted/error/import-queue counts,
    per-source stats, backup-throttle, mac-availability, direct-write mode).
  * the TRMNL e-ink HTML renders within the byte budget, carries no <script> and
    references no external asset, and actually contains the live numbers.
  * the endpoints DEGRADE GRACEFULLY when signals are missing — a bare DB yields
    "unknown"s + a signals_missing list and NEVER raises / NEVER 500s.

Self-contained: seeds temp SQLite files, no network, no live DB.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_API_DIR not in sys.path:
    sys.path.insert(0, SYNC_API_DIR)

# Point the app's DB layer at a seeded temp file BEFORE importing anything that
# reads SLS_DB_PATH at import time (db.py captures it into a module global).
_ENDPOINT_DB = tempfile.mktemp(suffix=".db")
os.environ["SLS_DB_PATH"] = _ENDPOINT_DB

from routes import status as status_mod  # noqa: E402


# --------------------------------------------------------------------------- #
# Seeding helpers.
# --------------------------------------------------------------------------- #

def _seed_full(path: str) -> None:
    """A realistically-populated DB exercising every signal the dashboard reads."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            title TEXT, artist TEXT,
            lexicon_status TEXT NOT NULL DEFAULT 'pending',
            pipeline_stage TEXT NOT NULL DEFAULT 'new',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE wanted (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER,
            state TEXT NOT NULL DEFAULT 'wanted'
        );
        CREATE TABLE import_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER,
            state TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE source_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER,
            source TEXT NOT NULL, status TEXT NOT NULL,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE mac_availability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reachable INTEGER, smb_mounted INTEGER, api_ok INTEGER,
            detail TEXT, sampled_at TEXT
        );
        """
    )
    # 12 tracks: 6 synced, 2 error, 4 actively sourcing (1 match, 2 dl, 1 verify).
    rows = []
    for i in range(6):
        rows.append((f"s{i}", f"T{i}", f"A{i}", "synced", "complete"))
    for i in range(6, 8):
        rows.append((f"s{i}", f"T{i}", f"A{i}", "pending", "error"))
    rows.append(("s8", "T8", "A8", "pending", "matching"))
    rows.append(("s9", "T9", "A9", "pending", "downloading"))
    rows.append(("s10", "T10", "A10", "pending", "downloading"))
    rows.append(("s11", "T11", "A11", "pending", "verifying"))
    conn.executemany(
        "INSERT INTO tracks (spotify_id,title,artist,lexicon_status,pipeline_stage) "
        "VALUES (?,?,?,?,?)",
        rows,
    )
    conn.executemany(
        "INSERT INTO app_config (key,value) VALUES (?,?)",
        [
            ("last_spotify_poll", "2026-07-12T08:00:00Z"),
            ("backup_throttle_enabled", "1"),
            ("nas_backup_active", "0"),
            ("nas_iowait_pct", "12.5"),
            ("iowait_throttle_pct", "35"),
            ("downloads_paused", "0"),
            ("direct_write_mode", "validate"),
        ],
    )
    conn.executemany(
        "INSERT INTO wanted (state) VALUES (?)",
        [("wanted",), ("wanted",), ("sourcing",)],
    )
    conn.executemany(
        "INSERT INTO import_queue (state) VALUES (?)",
        [("pending",), ("held",)],
    )
    conn.executemany(
        "INSERT INTO source_attempts (source,status) VALUES (?,?)",
        [("tidal", "success"), ("tidal", "success"),
         ("tidal", "no_match"), ("soulseek", "success")],
    )
    conn.execute(
        "INSERT INTO mac_availability (reachable,smb_mounted,api_ok,detail,sampled_at) "
        "VALUES (1,1,0,'api down','2026-07-12T07:59:00Z')"
    )
    conn.commit()
    conn.close()


def _seed_minimal(path: str) -> None:
    """Only a tracks table — every v3 signal table/config absent."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            title TEXT, artist TEXT,
            lexicon_status TEXT NOT NULL DEFAULT 'pending',
            pipeline_stage TEXT NOT NULL DEFAULT 'new',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO tracks (spotify_id,lexicon_status,pipeline_stage)
            VALUES ('a','synced','complete'), ('b','pending','new');
        """
    )
    conn.commit()
    conn.close()


def _conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c


# --------------------------------------------------------------------------- #
# Aggregate correctness.
# --------------------------------------------------------------------------- #

class TestAggregates(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mktemp(suffix=".db")
        _seed_full(self.path)
        self.conn = _conn(self.path)
        self.status = status_mod.build_status(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_parity(self):
        p = self.status["parity"]
        self.assertEqual(p["spotify_likes"], 12)
        self.assertEqual(p["lexicon_synced"], 6)
        self.assertEqual(p["parity_pct"], 50.0)

    def test_last_sync(self):
        self.assertEqual(self.status["last_sync"], "2026-07-12T08:00:00Z")

    def test_counts(self):
        c = self.status["counts"]
        self.assertEqual(c["errors"], 2)
        self.assertEqual(c["wanted"], 3)
        self.assertEqual(c["wanted_by_state"], {"wanted": 2, "sourcing": 1})
        self.assertEqual(c["import_queue"], 2)
        self.assertEqual(c["import_queue_by_state"], {"pending": 1, "held": 1})

    def test_currently_sourcing(self):
        s = self.status["currently_sourcing"]
        self.assertEqual(s["count"], 4)
        self.assertEqual(s["stages"], {"matching": 1, "downloading": 2, "verifying": 1})
        self.assertEqual(len(s["sample"]), 4)

    def test_per_source(self):
        ps = self.status["per_source"]
        self.assertEqual(ps["tidal"], {"success": 2, "no_match": 1})
        self.assertEqual(ps["soulseek"], {"success": 1})

    def test_backup_throttle(self):
        t = self.status["backup_throttle"]
        self.assertIs(t["enabled"], True)
        self.assertIs(t["nas_backup_active"], False)
        self.assertEqual(t["iowait_pct"], 12.5)
        self.assertEqual(t["threshold_pct"], 35.0)
        self.assertIs(t["downloads_paused"], False)

    def test_mac_availability(self):
        m = self.status["mac_availability"]
        self.assertIs(m["reachable"], True)
        self.assertIs(m["smb_mounted"], True)
        self.assertIs(m["api_ok"], False)
        self.assertEqual(m["sampled_at"], "2026-07-12T07:59:00Z")

    def test_direct_write(self):
        self.assertEqual(self.status["direct_write"]["mode"], "validate")

    def test_no_signals_missing_when_fully_seeded(self):
        self.assertEqual(self.status["signals_missing"], [])


# --------------------------------------------------------------------------- #
# Graceful degradation.
# --------------------------------------------------------------------------- #

class TestGracefulMissing(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mktemp(suffix=".db")
        _seed_minimal(self.path)
        self.conn = _conn(self.path)

    def tearDown(self):
        self.conn.close()

    def test_does_not_raise_and_parity_still_computes(self):
        st = status_mod.build_status(self.conn)  # must not raise
        self.assertEqual(st["parity"]["parity_pct"], 50.0)

    def test_missing_signals_marked_unknown(self):
        st = status_mod.build_status(self.conn)
        self.assertEqual(st["last_sync"], "unknown")
        self.assertEqual(st["counts"]["wanted"], "unknown")
        self.assertEqual(st["counts"]["import_queue"], "unknown")
        self.assertEqual(st["mac_availability"], "unknown")
        self.assertEqual(st["direct_write"]["mode"], "unknown")
        self.assertEqual(st["per_source"], {})
        for sig in ("last_sync", "wanted", "import_queue",
                    "per_source", "mac_availability", "direct_write"):
            self.assertIn(sig, st["signals_missing"])

    def test_empty_db_no_tables_never_raises(self):
        empty = tempfile.mktemp(suffix=".db")
        c = _conn(empty)
        try:
            st = status_mod.build_status(c)  # not even a tracks table
            self.assertEqual(st["parity"]["parity_pct"], "unknown")
            self.assertIn("parity", st["signals_missing"])
        finally:
            c.close()


# --------------------------------------------------------------------------- #
# TRMNL e-ink rendering.
# --------------------------------------------------------------------------- #

class TestTrmnlRender(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mktemp(suffix=".db")
        _seed_full(self.path)
        self.conn = _conn(self.path)
        self.status = status_mod.build_status(self.conn)
        self.html = status_mod.render_trmnl_html(self.status)

    def tearDown(self):
        self.conn.close()

    def test_within_eink_byte_budget(self):
        self.assertLessEqual(
            len(self.html.encode("utf-8")), status_mod.TRMNL_MAX_BYTES
        )

    def test_no_script_and_no_external_assets(self):
        low = self.html.lower()
        self.assertNotIn("<script", low)
        self.assertNotIn("http://", low)
        self.assertNotIn("https://", low)
        self.assertNotIn("//", self.html.replace("<!doctype", ""))  # no // asset refs

    def test_contains_live_numbers(self):
        # parity string "50.0%  (6/12)" and sourcing count should be present.
        self.assertIn("50.0%", self.html)
        self.assertIn("6/12", self.html)

    def test_renders_when_signals_missing(self):
        mp = tempfile.mktemp(suffix=".db")
        _seed_minimal(mp)
        c = _conn(mp)
        try:
            html = status_mod.render_trmnl_html(status_mod.build_status(c))
            self.assertIn("unknown", html)
            self.assertLessEqual(len(html.encode("utf-8")), status_mod.TRMNL_MAX_BYTES)
        finally:
            c.close()


# --------------------------------------------------------------------------- #
# Endpoint smoke — never 500, correct content types.
# --------------------------------------------------------------------------- #

class TestEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _seed_full(_ENDPOINT_DB)  # the file SLS_DB_PATH points at
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        # Mount ONLY the status router (proves it stands alone and keeps the test
        # free of the heavier route deps like spotipy). Same handlers main.py uses.
        app = FastAPI()
        app.include_router(status_mod.router)
        cls.client = TestClient(app)

    def test_status_json_ok(self):
        r = self.client.get("/api/status.json")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["parity"]["parity_pct"], 50.0)
        self.assertEqual(body["counts"]["errors"], 2)

    def test_status_trmnl_ok(self):
        r = self.client.get("/api/status/trmnl")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.headers["content-type"].startswith("text/html"))
        self.assertIn("WaxFlow", r.text)

    def test_status_browser_ok(self):
        r = self.client.get("/api/status")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.headers["content-type"].startswith("text/html"))
        self.assertIn("parity", r.text.lower())


if __name__ == "__main__":
    unittest.main()
