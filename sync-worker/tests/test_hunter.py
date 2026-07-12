"""Tests for the Phase 4 missing-track HUNTER.

Exercises the whole loop against seeded temp SQLite DBs with INJECTED fake acquire
sources (no network):

  * enqueue unsourced (error-stage) tracks into ``wanted`` — idempotent.
  * reconcile resolved once a track reaches ``complete``.
  * re-attempt HIT  -> track re-armed to the normal pipeline + wanted 'sourcing'.
  * re-attempt MISS -> wanted backs off (attempts++/next_retry_at) + buy-links
    generated into ``purchase_links`` (deduped, refreshable).
  * attempt cap    -> wanted 'exhausted'.
  * backoff gating -> a just-missed item is not re-attempted until its window.
  * idempotency + restart-safety, disabled-by-default flag.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import hunter, v3_schema  # noqa: E402
from tasks.sources import registry  # noqa: E402
from tasks.sources.base import SourceResult  # noqa: E402


# --------------------------------------------------------------------------- #
# Fake acquire source for injection.
# --------------------------------------------------------------------------- #
class FakeAcquire:
    def __init__(self, name="fake", hits=False):
        self.name = name
        self._hits = hits
        self.searched = 0

    def search(self, db_path, q):
        self.searched += 1
        if self._hits:
            return [SourceResult(source=self.name, confidence=1.0, kind="acquire",
                                 external_id="x1", format_hint="lossless")]
        return []


def _db(with_error_tracks=1) -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL, title TEXT, artist TEXT,
            album TEXT, isrc TEXT, duration_ms INTEGER,
            match_status TEXT DEFAULT 'pending',
            pipeline_error TEXT,
            pipeline_stage TEXT NOT NULL DEFAULT 'new',
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT, playlist_name TEXT, year INTEGER, month INTEGER
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, track_id INTEGER,
            message TEXT, details TEXT, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER, source TEXT,
            status TEXT, attempted_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    for i in range(with_error_tracks):
        conn.execute(
            "INSERT INTO tracks (spotify_id, title, artist, isrc, pipeline_stage) "
            "VALUES (?, ?, ?, ?, 'error')",
            (f"sp{i}", f"Title {i}", f"Artist {i}", f"ISRC{i:08d}"),
        )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)
    return path


def _set(path, key, value):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO app_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=?",
        (key, value, value),
    )
    conn.commit()
    conn.close()


def _rows(path, sql, params=()):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    out = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return out


class HunterTestBase(unittest.TestCase):
    def setUp(self):
        self.db = _db(with_error_tracks=1)
        self._orig_acq = registry.enabled_acquire_sources

    def tearDown(self):
        registry.enabled_acquire_sources = self._orig_acq
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + suffix)
            except OSError:
                pass

    def _inject(self, sources):
        registry.enabled_acquire_sources = lambda db_path: sources


class TestEnqueue(HunterTestBase):
    def test_enqueue_creates_wanted(self):
        n = hunter.enqueue_from_failures(self.db)
        self.assertEqual(n, 1)
        w = _rows(self.db, "SELECT * FROM wanted")
        self.assertEqual(len(w), 1)
        self.assertEqual(w[0]["state"], "wanted")
        self.assertTrue(w[0]["reason"].startswith("unsourced:error"))

    def test_enqueue_idempotent(self):
        hunter.enqueue_from_failures(self.db)
        n2 = hunter.enqueue_from_failures(self.db)
        self.assertEqual(n2, 0)
        self.assertEqual(len(_rows(self.db, "SELECT * FROM wanted")), 1)


class TestReconcile(HunterTestBase):
    def test_resolved_when_complete(self):
        hunter.enqueue_from_failures(self.db)
        # Track later completes (pipeline finished it).
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET pipeline_stage='complete' WHERE id=1")
        conn.commit()
        conn.close()
        n = hunter.reconcile_resolved(self.db)
        self.assertEqual(n, 1)
        self.assertEqual(_rows(self.db, "SELECT state FROM wanted")[0]["state"], "resolved")


class TestReattempt(HunterTestBase):
    def test_hit_rearms_track_and_sources(self):
        hunter.enqueue_from_failures(self.db)
        fake = FakeAcquire(hits=True)
        self._inject([fake])
        counts = hunter.process_wanted(self.db, max_attempts=8, batch_size=25)
        self.assertEqual(counts["sourced"], 1)
        self.assertEqual(fake.searched, 1)
        # Track re-armed for the normal pipeline.
        t = _rows(self.db, "SELECT pipeline_stage, match_status FROM tracks WHERE id=1")[0]
        self.assertEqual(t["pipeline_stage"], "new")
        self.assertEqual(t["match_status"], "pending")
        # Wanted moved to 'sourcing'.
        w = _rows(self.db, "SELECT state, last_source FROM wanted WHERE track_id=1")[0]
        self.assertEqual(w["state"], "sourcing")
        self.assertEqual(w["last_source"], "fake")

    def test_miss_backs_off_and_generates_buylinks(self):
        hunter.enqueue_from_failures(self.db)
        self._inject([FakeAcquire(hits=False)])
        counts = hunter.process_wanted(self.db, max_attempts=8, batch_size=25)
        self.assertEqual(counts["sourced"], 0)
        self.assertEqual(counts["still_wanting"], 1)
        w = _rows(self.db, "SELECT state, attempts, next_retry_at FROM wanted WHERE track_id=1")[0]
        self.assertEqual(w["state"], "wanted")
        self.assertEqual(w["attempts"], 1)
        self.assertIsNotNone(w["next_retry_at"])
        # Buy-links generated for all 3 stores (real link sources).
        links = _rows(self.db, "SELECT source, url FROM purchase_links WHERE track_id=1")
        self.assertEqual({l["source"] for l in links}, {"qobuz", "beatport", "bandcamp"})
        for l in links:
            self.assertTrue(l["url"].startswith("https://"))

    def test_buylinks_dedup_on_repeat(self):
        hunter.enqueue_from_failures(self.db)
        self._inject([FakeAcquire(hits=False)])
        hunter.process_wanted(self.db, max_attempts=8, batch_size=25)
        # Force the backoff window open, run again -> links refreshed, not duplicated.
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE wanted SET next_retry_at='2000-01-01T00:00:00+00:00'")
        conn.commit()
        conn.close()
        hunter.process_wanted(self.db, max_attempts=8, batch_size=25)
        links = _rows(self.db, "SELECT source FROM purchase_links WHERE track_id=1")
        self.assertEqual(len(links), 3)  # still 3, deduped by dedup_key

    def test_backoff_gate_skips_until_window(self):
        hunter.enqueue_from_failures(self.db)
        self._inject([FakeAcquire(hits=False)])
        hunter.process_wanted(self.db, max_attempts=8, batch_size=25)  # attempt 1, backs off
        # Immediately re-run: next_retry_at is in the future -> not attempted.
        counts2 = hunter.process_wanted(self.db, max_attempts=8, batch_size=25)
        self.assertEqual(counts2["attempted"], 0)

    def test_attempt_cap_exhausts(self):
        hunter.enqueue_from_failures(self.db)
        self._inject([FakeAcquire(hits=False)])
        # max_attempts=2: first miss backs off, second miss exhausts.
        hunter.process_wanted(self.db, max_attempts=2, batch_size=25)
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE wanted SET next_retry_at='2000-01-01T00:00:00+00:00'")
        conn.commit()
        conn.close()
        counts = hunter.process_wanted(self.db, max_attempts=2, batch_size=25)
        self.assertEqual(counts["exhausted"], 1)
        self.assertEqual(_rows(self.db, "SELECT state FROM wanted")[0]["state"], "exhausted")


class TestRunHunter(HunterTestBase):
    def test_disabled_by_default(self):
        out = hunter.run_hunter(self.db)
        self.assertEqual(out["status"], "disabled")
        self.assertEqual(_rows(self.db, "SELECT COUNT(*) c FROM wanted")[0]["c"], 0)

    def test_full_pass_when_enabled(self):
        _set(self.db, "hunter_enabled", "1")
        self._inject([FakeAcquire(hits=False)])
        out = hunter.run_hunter(self.db)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["enqueued"], 1)
        self.assertEqual(out["still_wanting"], 1)
        self.assertGreaterEqual(out["links_written"], 3)
        # Heartbeat written.
        self.assertTrue(os.path.exists(hunter._heartbeat_path(self.db)))
        self.assertIsNotNone(
            _rows(self.db, "SELECT value FROM app_config WHERE key='hunter_last_run'")
        )

    def test_end_to_end_hit_resolves(self):
        _set(self.db, "hunter_enabled", "1")
        self._inject([FakeAcquire(hits=True)])
        hunter.run_hunter(self.db)  # sources it -> track re-armed, wanted 'sourcing'
        # Simulate the normal pipeline completing the re-armed track.
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET pipeline_stage='complete' WHERE id=1")
        conn.commit()
        conn.close()
        out = hunter.run_hunter(self.db)  # reconcile -> resolved
        self.assertEqual(out["resolved_reconciled"], 1)
        self.assertEqual(_rows(self.db, "SELECT state FROM wanted")[0]["state"], "resolved")


if __name__ == "__main__":
    unittest.main()
