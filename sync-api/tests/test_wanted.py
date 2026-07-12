"""Tests for the Phase 4 wanted/buy-links API (routes/wanted.py).

Proves the endpoint surfaces the hunter's wanted ledger WITH each track's buy-links,
filters by state, degrades gracefully on a bare DB, and never advertises auto-
purchase. Self-contained: seeds a temp SQLite file, no network, no live DB.
"""

import asyncio
import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_API_DIR not in sys.path:
    sys.path.insert(0, SYNC_API_DIR)

_DB = tempfile.mktemp(suffix=".db")
os.environ["SLS_DB_PATH"] = _DB

import db as db_mod  # noqa: E402
from routes import wanted as wanted_mod  # noqa: E402


def _seed(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT, title TEXT, artist TEXT, album TEXT, isrc TEXT,
            pipeline_stage TEXT DEFAULT 'error'
        );
        CREATE TABLE wanted (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER, state TEXT,
            attempts INTEGER DEFAULT 0, reason TEXT, last_source TEXT,
            last_attempt_at TEXT, next_retry_at TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE purchase_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER, source TEXT,
            url TEXT, format_hint TEXT, price TEXT, confidence REAL, dedup_key TEXT,
            status TEXT DEFAULT 'active',
            first_generated_at TEXT DEFAULT (datetime('now')),
            last_refreshed_at TEXT DEFAULT (datetime('now'))
        );
        INSERT INTO tracks (id, spotify_id, title, artist, isrc) VALUES
            (1, 'sp1', 'One More Time', 'Daft Punk', 'ISRC1'),
            (2, 'sp2', 'Resolved Song', 'Someone', 'ISRC2');
        INSERT INTO wanted (track_id, state, attempts, reason) VALUES
            (1, 'wanted', 2, 'unsourced:error'),
            (2, 'resolved', 1, 'unsourced:error');
        INSERT INTO purchase_links (track_id, source, url, dedup_key) VALUES
            (1, 'beatport', 'https://www.beatport.com/search/tracks?q=Daft+Punk+One+More+Time', 'beatport:isrc1'),
            (1, 'qobuz', 'https://www.qobuz.com/us-en/search/tracks/Daft%20Punk', 'qobuz:isrc1'),
            (1, 'bandcamp', 'https://bandcamp.com/search?q=Daft+Punk&item_type=t', 'bandcamp:isrc1');
        """
    )
    conn.commit()
    conn.close()


class TestWantedApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # db.py captures DB_PATH at import; another test module importing db first
        # would fix it to ITS temp DB. Point the module global at our seeded DB so
        # this suite is order-independent.
        cls._orig_db_path = db_mod.DB_PATH
        db_mod.DB_PATH = _DB
        _seed(_DB)

    @classmethod
    def tearDownClass(cls):
        db_mod.DB_PATH = cls._orig_db_path
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(_DB + suffix)
            except OSError:
                pass

    def test_lists_open_wanted_with_links(self):
        out = asyncio.run(wanted_mod.list_wanted(state=None, limit=200))
        # Default excludes 'resolved' -> only track 1.
        self.assertEqual(out["count"], 1)
        item = out["items"][0]
        self.assertEqual(item["track_id"], 1)
        self.assertEqual(item["artist"], "Daft Punk")
        self.assertEqual({l["source"] for l in item["links"]}, {"beatport", "qobuz", "bandcamp"})
        self.assertIn("never auto-purchase", out["buy_links_note"].lower())

    def test_filter_by_state_resolved(self):
        out = asyncio.run(wanted_mod.list_wanted(state="resolved", limit=200))
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["items"][0]["track_id"], 2)
        self.assertEqual(out["items"][0]["links"], [])

    def test_links_endpoint(self):
        out = asyncio.run(wanted_mod.wanted_links(track_id=1))
        self.assertEqual(out["count"], 3)
        for l in out["links"]:
            self.assertTrue(l["url"].startswith("https://"))

    def test_links_empty_for_unknown_track(self):
        out = asyncio.run(wanted_mod.wanted_links(track_id=999))
        self.assertEqual(out["count"], 0)
        self.assertEqual(out["links"], [])


if __name__ == "__main__":
    unittest.main()
