"""Tests for the Phase 3 offline import queue (sleep-tolerant sync).

Proves the sleep/wake contract:
  * enqueue-on-unavailable is idempotent (one active row per organizing track,
    repeated calls while asleep add nothing),
  * the queue is durable across a simulated worker restart (persisted in sync.db),
  * drain-on-return applies each item through the safe organize path in order and
    marks it done; a re-drain is a pure no-op (never double-applies),
  * a per-track failure retries with backoff (next_retry_at set), and a mid-drain
    loss of Lexicon stops the drain cleanly leaving the remainder queued.

The organize path is stubbed (a fake organize_fn) so the test is pure/offline; the
REAL organize writes are already covered as idempotent by the direct-write tests.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import offline_queue, v3_schema  # noqa: E402
from tasks.helpers import get_db  # noqa: E402
from tasks.process_pipeline import LexiconImportEmpty  # noqa: E402


def _db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT, title TEXT, artist TEXT, album TEXT,
            spotify_added_at TEXT, file_path TEXT, match_source TEXT,
            lexicon_track_id INTEGER, lexicon_playlist_id INTEGER,
            lexicon_status TEXT, pipeline_stage TEXT DEFAULT 'organizing',
            pipeline_error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT
        );
        CREATE TABLE playlists (id INTEGER PRIMARY KEY AUTOINCREMENT);
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT, track_id INTEGER, message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)
    return path


def _add_track(db, spotify_id, added_at="2026-07-10T00:00:00Z", stage="organizing",
               match_source=None, lexicon_track_id=None):
    with get_db(db) as conn:
        conn.execute(
            """INSERT INTO tracks (spotify_id, spotify_added_at, file_path, pipeline_stage,
               match_source, lexicon_track_id) VALUES (?,?,?,?,?,?)""",
            (spotify_id, added_at, f"/music/{spotify_id}.flac", stage, match_source, lexicon_track_id),
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _pending(db):
    with get_db(db) as conn:
        return conn.execute("SELECT COUNT(*) FROM import_queue WHERE state='pending'").fetchone()[0]


def _states(db):
    with get_db(db) as conn:
        rows = conn.execute("SELECT state, COUNT(*) c FROM import_queue GROUP BY state").fetchall()
    return {r["state"]: r["c"] for r in rows}


class OfflineQueueTest(unittest.TestCase):
    def setUp(self):
        self.db = _db()

    def tearDown(self):
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_enqueue_is_idempotent(self):
        _add_track(self.db, "a")
        _add_track(self.db, "b")
        self.assertEqual(offline_queue.enqueue_organizing(self.db, "asleep"), 2)
        # Second call while still asleep must not duplicate.
        self.assertEqual(offline_queue.enqueue_organizing(self.db, "asleep"), 0)
        self.assertEqual(_pending(self.db), 2)

    def test_op_link_vs_import(self):
        _add_track(self.db, "imp")  # no lexicon id -> import
        _add_track(self.db, "lnk", match_source="lexicon_existing", lexicon_track_id=42)
        offline_queue.enqueue_organizing(self.db, "asleep")
        with get_db(self.db) as conn:
            ops = dict(conn.execute(
                "SELECT t.spotify_id, q.op FROM import_queue q JOIN tracks t ON t.id=q.track_id"
            ).fetchall())
        self.assertEqual(ops["imp"], "import")
        self.assertEqual(ops["lnk"], "link")

    def test_queue_survives_restart(self):
        _add_track(self.db, "a")
        offline_queue.enqueue_organizing(self.db, "asleep")
        # "Restart": drop all in-memory refs, reopen the on-disk DB fresh.
        with sqlite3.connect(self.db) as conn:
            n = conn.execute("SELECT COUNT(*) FROM import_queue WHERE state='pending'").fetchone()[0]
        self.assertEqual(n, 1)

    def test_unavailable_then_back_drains_correctly(self):
        # KEY proof: unavailable -> queued -> Lexicon back -> drained, idempotent.
        tid = _add_track(self.db, "a")
        offline_queue.enqueue_organizing(self.db, "asleep")
        self.assertEqual(_pending(self.db), 1)

        calls = []

        def fake_organize(db, track):
            calls.append(track["id"])
            # Mirror the real safe path's end state.
            with get_db(db) as conn:
                conn.execute(
                    "UPDATE tracks SET pipeline_stage='complete', lexicon_status='synced' WHERE id=?",
                    (track["id"],),
                )

        counts = offline_queue.drain(self.db, fake_organize)
        self.assertEqual(counts["drained"], 1)
        self.assertEqual(_states(self.db).get("done"), 1)
        self.assertEqual(calls, [tid])

        # Re-drain: nothing pending -> pure no-op (no double-apply).
        counts2 = offline_queue.drain(self.db, fake_organize)
        self.assertEqual(counts2["drained"], 0)
        self.assertEqual(len(calls), 1)

    def test_empty_import_keeps_pending_with_backoff(self):
        _add_track(self.db, "a")
        offline_queue.enqueue_organizing(self.db, "asleep")

        def raises_empty(db, track):
            raise LexiconImportEmpty("file not synced yet")

        counts = offline_queue.drain(self.db, raises_empty)
        self.assertEqual(counts["still_pending"], 1)
        self.assertEqual(counts["drained"], 0)
        with get_db(self.db) as conn:
            row = conn.execute(
                "SELECT attempts, next_retry_at FROM import_queue WHERE state='pending'"
            ).fetchone()
        self.assertEqual(row["attempts"], 1)
        self.assertIsNotNone(row["next_retry_at"])  # backoff scheduled

    def test_connection_loss_stops_drain_early(self):
        _add_track(self.db, "a")
        _add_track(self.db, "b")
        offline_queue.enqueue_organizing(self.db, "asleep")

        def raises_conn(db, track):
            raise ConnectionError("lexicon went to sleep mid-drain")

        counts = offline_queue.drain(self.db, raises_conn)
        self.assertTrue(counts["stopped_early"])
        self.assertEqual(counts["drained"], 0)
        # Both items remain pending for the next wake.
        self.assertEqual(_pending(self.db), 2)


if __name__ == "__main__":
    unittest.main()
