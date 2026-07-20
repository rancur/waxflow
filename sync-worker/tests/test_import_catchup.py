"""Tests for the Phase 3 sleep-tolerance catch-up pass (tasks/import_catchup.py).

Proves the sleep/wake recovery contract:
  * a downloaded-but-not-imported track stranded in 'error' by a TRANSIENT
    Lexicon/Mac-unavailability failure (db-locked / timed-out / empty-import) is
    re-armed to the correct earlier stage when Lexicon is available again,
  * a non-transient error (not-lossless, fingerprint-too-low) is left ALONE,
  * a track whose file has vanished from disk is NOT re-armed,
  * the pass is a no-op while Lexicon is unavailable (on-wake gate),
  * revival is bounded by catchup_attempts (no infinite loop),
  * a track already in Lexicon (lexicon_track_id set) with a TRANSIENT error is
    re-armed to 'organizing' (idempotent bookkeeping retry, no file check);
    with a non-transient error it is left alone.

The availability probe and the filesystem check are stubbed so the test is pure/
offline; the real import writes are covered as idempotent elsewhere.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import import_catchup, v3_schema  # noqa: E402
from tasks.helpers import get_db  # noqa: E402


def _db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT, title TEXT, artist TEXT, album TEXT,
            spotify_added_at TEXT, file_path TEXT, match_source TEXT,
            download_status TEXT, lexicon_track_id INTEGER, lexicon_status TEXT,
            pipeline_stage TEXT DEFAULT 'error', pipeline_error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now', '-1 hour'))
        );
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
    v3_schema.ensure_v3_schema(path)  # adds catchup_attempts + v3 tables
    return path


def _add(db, *, artist="A", title="T", download_status="complete",
         lexicon_track_id=None, stage="error", error="Lexicon sync error: database is locked",
         file_path="/music/A/A - T.flac", catchup_attempts=0, age="-1 hour"):
    with get_db(db) as conn:
        conn.execute(
            """INSERT INTO tracks (artist, title, spotify_added_at, file_path,
               download_status, lexicon_track_id, pipeline_stage, pipeline_error,
               catchup_attempts, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?, datetime('now', ?))""",
            (artist, title, "2026-07-10T00:00:00Z", file_path, download_status,
             lexicon_track_id, stage, error, catchup_attempts, age),
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _track(db, tid):
    with get_db(db) as conn:
        return dict(conn.execute("SELECT * FROM tracks WHERE id=?", (tid,)).fetchone())


class _StubAvail:
    def __init__(self, available):
        self.state = "available" if available else "asleep"

    @property
    def lexicon_available(self):
        return self.state == "available"


class ImportCatchupTest(unittest.TestCase):
    def setUp(self):
        self.db = _db()
        # Stub the availability probe (available) and the filesystem existence check
        # (file present) unless a test overrides them.
        self._orig_probe = None
        import tasks.mac_availability as ma
        self._ma = ma
        self._orig_probe = ma.probe
        ma.probe = lambda db_path, **kw: _StubAvail(True)
        self._orig_exists = os.path.exists
        os.path.exists = lambda p: True

    def tearDown(self):
        self._ma.probe = self._orig_probe
        os.path.exists = self._orig_exists
        try:
            os.remove(self.db)
        except OSError:
            pass

    def test_revives_transient_orphan_to_organizing(self):
        tid = _add(self.db, error="Lexicon sync error: database is locked")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 1)
        t = _track(self.db, tid)
        self.assertEqual(t["pipeline_stage"], "organizing")
        self.assertEqual(t["lexicon_status"], "pending")
        self.assertEqual(t["catchup_attempts"], 1)

    def test_verify_error_reenters_at_verifying(self):
        tid = _add(self.db, error="Verification error: database is locked")
        import_catchup.run_catchup(self.db)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "verifying")

    def test_empty_import_signature_revived(self):
        tid = _add(self.db, error="[lexicon_import_empty] Lexicon returned 0 tracks (mount down)")
        import_catchup.run_catchup(self.db)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "organizing")

    def test_nontransient_error_left_alone(self):
        tid = _add(self.db, error="not lossless: codec=aac, sr=44100")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 0)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")

    def test_missing_file_not_revived(self):
        os.path.exists = lambda p: False
        tid = _add(self.db, error="Lexicon sync error: timed out")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 0)
        self.assertEqual(counts["missing_file"], 1)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")

    def test_noop_when_lexicon_unavailable(self):
        self._ma.probe = lambda db_path, **kw: _StubAvail(False)
        tid = _add(self.db, error="Lexicon sync error: database is locked")
        counts = import_catchup.run_catchup(self.db)
        self.assertIn("skipped", counts)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")

    def test_bookkeeping_orphan_revived_without_file_check(self):
        # Import succeeded (lexicon_track_id set) but a post-import step failed as
        # the Mac slept ("database is locked"). Revived to organizing, which is
        # idempotent for linked tracks — even if the file has since been
        # organized/renamed (os.path.exists False).
        os.path.exists = lambda p: False
        tid = _add(self.db, lexicon_track_id=42, error="Lexicon sync error: database is locked")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 1)
        self.assertEqual(counts["missing_file"], 0)
        t = _track(self.db, tid)
        self.assertEqual(t["pipeline_stage"], "organizing")
        self.assertEqual(t["catchup_attempts"], 1)

    def test_already_in_lexicon_nontransient_untouched(self):
        tid = _add(self.db, lexicon_track_id=42, error="not lossless: codec=aac, sr=44100")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 0)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")

    def test_bounded_by_max_attempts(self):
        tid = _add(self.db, catchup_attempts=6, error="Lexicon sync error: timed out")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 0)
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")

    def test_recent_error_not_yet_revived(self):
        # Error settled only seconds ago -> below min_age -> skipped this pass.
        tid = _add(self.db, error="Lexicon sync error: timed out", age="-10 seconds")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts["revived"], 0)

    def test_disabled_flag_is_noop(self):
        with get_db(self.db) as conn:
            conn.execute("INSERT INTO app_config(key,value) VALUES('import_catchup_enabled','0')")
        tid = _add(self.db, error="Lexicon sync error: database is locked")
        counts = import_catchup.run_catchup(self.db)
        self.assertEqual(counts.get("skipped"), "disabled")
        self.assertEqual(_track(self.db, tid)["pipeline_stage"], "error")


if __name__ == "__main__":
    unittest.main()
