"""Tests for the source-plugin registry + shared backoff (Phase A foundation).

Covers: registry membership + priority ordering, capability filtering,
enable/disable via app_config, and the SourceBackoff exponential-backoff math and
its source_attempts bookkeeping.

No network / no external deps.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import v3_schema  # noqa: E402
from tasks.sources import registry  # noqa: E402
from tasks.sources.base import Source, SourceBackoff, SourceCapability, TrackQuery  # noqa: E402
from tasks.sources.tidal import TidalSource  # noqa: E402
from tasks.sources.soulseek import SoulseekSource  # noqa: E402


def _db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL, title TEXT, artist TEXT
        );
        CREATE TABLE playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT, playlist_name TEXT, year INTEGER, month INTEGER
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER, source TEXT,
            status TEXT, error TEXT, search_query TEXT, result_count INTEGER,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        INSERT INTO tracks (spotify_id, title, artist) VALUES ('sp1', 'T', 'A');
        """
    )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)
    return path


def _set_config(path: str, key: str, value: str):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO app_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = ?",
        (key, value, value),
    )
    conn.commit()
    conn.close()


class TestRegistry(unittest.TestCase):
    def test_all_sources_present(self):
        names = {s.name for s in registry.all_sources()}
        self.assertEqual(names, {"tidal", "soulseek"})

    def test_acquire_sources_priority_order(self):
        ordered = registry.acquire_sources()
        self.assertEqual([s.name for s in ordered], ["tidal", "soulseek"])
        # priorities strictly ascending
        prios = [s.priority for s in ordered]
        self.assertEqual(prios, sorted(prios))
        self.assertLess(prios[0], prios[1])

    def test_link_sources_empty_in_phase_a(self):
        # No SEARCH_LINK sources yet (Beatport/Qobuz/Bandcamp are Phase B).
        self.assertEqual(registry.link_sources(), [])

    def test_capabilities(self):
        for s in registry.all_sources():
            self.assertIn(SourceCapability.ACQUIRE, s.capabilities)
            self.assertIn(SourceCapability.LOSSLESS, s.capabilities)

    def test_get_source(self):
        self.assertIsInstance(registry.get_source("tidal"), TidalSource)
        self.assertIsInstance(registry.get_source("soulseek"), SoulseekSource)
        self.assertIsNone(registry.get_source("nope"))


class TestEnableDisable(unittest.TestCase):
    def setUp(self):
        self.db = _db()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + suffix)
            except OSError:
                pass

    def test_tidal_default_on(self):
        self.assertTrue(TidalSource().is_enabled(self.db))

    def test_tidal_disable_via_config(self):
        _set_config(self.db, "source_tidal_enabled", "0")
        self.assertFalse(TidalSource().is_enabled(self.db))
        _set_config(self.db, "source_tidal_enabled", "1")
        self.assertTrue(TidalSource().is_enabled(self.db))

    def test_soulseek_default_on_and_toggle(self):
        self.assertTrue(SoulseekSource().is_enabled(self.db))
        _set_config(self.db, "soulseek_fallback_enabled", "off")
        self.assertFalse(SoulseekSource().is_enabled(self.db))

    def test_enabled_acquire_sources_respects_toggle(self):
        # Tidal availability depends on the tiddl CLI probe; force-enable both by
        # disabling tidal so soulseek (which is_available == True) remains.
        _set_config(self.db, "source_tidal_enabled", "0")
        enabled = registry.enabled_acquire_sources(self.db)
        self.assertNotIn("tidal", [s.name for s in enabled])
        self.assertIn("soulseek", [s.name for s in enabled])


class TestSourceBackoff(unittest.TestCase):
    def setUp(self):
        self.db = _db()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + suffix)
            except OSError:
                pass

    def test_delay_math_exponential(self):
        self.assertEqual(SourceBackoff.delay_for(1), 60)
        self.assertEqual(SourceBackoff.delay_for(2), 120)
        self.assertEqual(SourceBackoff.delay_for(3), 240)
        self.assertEqual(SourceBackoff.delay_for(4), 480)

    def test_delay_capped_at_max(self):
        self.assertEqual(SourceBackoff.delay_for(100), SourceBackoff.MAX_SECONDS)
        self.assertLessEqual(SourceBackoff.delay_for(50), SourceBackoff.MAX_SECONDS)

    def test_delay_floor_at_one(self):
        self.assertEqual(SourceBackoff.delay_for(0), 60)
        self.assertEqual(SourceBackoff.delay_for(-5), 60)

    def test_record_increments_attempt_no_and_writes_row(self):
        self.assertEqual(SourceBackoff.attempt_count(self.db, 1, "tidal"), 0)
        r1 = SourceBackoff.record(self.db, 1, "tidal", "no_match", search_query="A T")
        self.assertEqual(r1["attempt_no"], 1)
        self.assertEqual(r1["backoff_seconds"], 60)
        self.assertEqual(SourceBackoff.attempt_count(self.db, 1, "tidal"), 1)
        r2 = SourceBackoff.record(self.db, 1, "tidal", "error", error="boom")
        self.assertEqual(r2["attempt_no"], 2)
        self.assertEqual(r2["backoff_seconds"], 120)
        # rows really landed in source_attempts with the recorded fields
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            "SELECT source, status, error, search_query, attempt_no, backoff_seconds, next_retry_at "
            "FROM source_attempts WHERE track_id = 1 ORDER BY id"
        ).fetchall()
        conn.close()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "tidal")
        self.assertEqual(rows[0][1], "no_match")
        self.assertEqual(rows[0][3], "A T")
        self.assertEqual(rows[0][4], 1)
        self.assertEqual(rows[1][2], "boom")
        self.assertEqual(rows[1][4], 2)
        self.assertIsNotNone(rows[0][6])  # next_retry_at populated

    def test_per_source_isolation(self):
        SourceBackoff.record(self.db, 1, "tidal", "no_match")
        self.assertEqual(SourceBackoff.attempt_count(self.db, 1, "tidal"), 1)
        self.assertEqual(SourceBackoff.attempt_count(self.db, 1, "soulseek"), 0)

    def test_is_ready(self):
        # No prior attempt => ready.
        self.assertTrue(SourceBackoff.is_ready(self.db, 1, "tidal"))
        # After a record, the next_retry_at is in the future => not ready.
        SourceBackoff.record(self.db, 1, "tidal", "no_match")
        self.assertFalse(SourceBackoff.is_ready(self.db, 1, "tidal"))
        # Force the window into the past => ready again.
        conn = sqlite3.connect(self.db)
        conn.execute(
            "UPDATE source_attempts SET next_retry_at = '2000-01-01T00:00:00+00:00' WHERE track_id = 1"
        )
        conn.commit()
        conn.close()
        self.assertTrue(SourceBackoff.is_ready(self.db, 1, "tidal"))


class TestTrackQuery(unittest.TestCase):
    def test_from_row(self):
        q = TrackQuery.from_row({
            "artist": "A", "title": "T", "album": "Al", "isrc": "US123",
            "duration_ms": 200000, "spotify_id": "sp1", "extra": "ignored",
        })
        self.assertEqual(q.artist, "A")
        self.assertEqual(q.title, "T")
        self.assertEqual(q.isrc, "US123")
        self.assertEqual(q.duration_ms, 200000)
        self.assertEqual(q.spotify_id, "sp1")

    def test_from_row_handles_missing(self):
        q = TrackQuery.from_row({})
        self.assertEqual(q.artist, "")
        self.assertEqual(q.title, "")
        self.assertIsNone(q.isrc)


if __name__ == "__main__":
    unittest.main()
