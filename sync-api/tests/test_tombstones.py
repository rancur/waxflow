"""API contract for deleted-in-Lexicon tombstones (2.19.0).

  * /tracks/errors splits tombstoned tracks out of 'ignored' into deleted_in_lexicon
  * retry / unignore / reject refuse a tombstoned track (409) — a tombstone is terminal
  * bulk-retry skips it silently (it is in 'ignored')
  * /restore un-trashes the file, closes the tombstone and re-arms from 'new'
  * a plain user 'ignore' (no tombstone) still un-ignores normally
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
from fastapi import HTTPException  # noqa: E402
from routes import matching as matching_mod  # noqa: E402
from routes import tracks as tracks_mod  # noqa: E402


def _seed(path: str, trashed: str, original: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        f"""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT, title TEXT, artist TEXT, album TEXT, isrc TEXT,
            tidal_id TEXT, file_path TEXT, match_source TEXT, match_confidence REAL,
            fingerprint_match_score REAL, match_status TEXT DEFAULT 'matched',
            download_status TEXT DEFAULT 'complete', download_error TEXT,
            download_attempts INTEGER DEFAULT 0, verify_status TEXT DEFAULT 'pass',
            verify_codec TEXT, verify_is_genuine_lossless INTEGER,
            lexicon_status TEXT DEFAULT 'synced', lexicon_track_id TEXT,
            pipeline_stage TEXT DEFAULT 'complete', pipeline_error TEXT,
            is_protected INTEGER DEFAULT 0, spotify_added_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, track_id INTEGER,
            message TEXT, details TEXT, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE fallback_attempts (id INTEGER PRIMARY KEY, track_id INTEGER, source TEXT, status TEXT, error TEXT, search_query TEXT, result_count INTEGER);
        CREATE TABLE source_attempts (id INTEGER PRIMARY KEY, track_id INTEGER);
        CREATE TABLE tombstones (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER, spotify_id TEXT,
            isrc TEXT, tidal_id TEXT, lexicon_track_id TEXT, file_path TEXT,
            file_hash_sha256 TEXT, reason TEXT NOT NULL, trashed_path TEXT,
            purge_after TEXT, purged_at TEXT, restored_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO tracks (id, spotify_id, title, artist, pipeline_stage, is_protected, lexicon_track_id, pipeline_error)
        VALUES (1, 'sp1', 'Crap Match', 'Someone', 'ignored', 1, '9999', 'deleted_in_lexicon:2026-09-01T00:00:00Z (deleted_in_lexicon)'),
               (2, 'sp2', 'Dismissed', 'Someone', 'ignored', 1, NULL, NULL),
               (3, 'sp3', 'Broken', 'Someone', 'error', 0, NULL, 'Download failed');
        INSERT INTO tombstones (track_id, spotify_id, tidal_id, lexicon_track_id, file_path, reason, trashed_path, purge_after)
        VALUES (1, 'sp1', 'T1', '9999', '{original}', 'deleted_in_lexicon', '{trashed}', '2099-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()


def _run(coro):
    return asyncio.run(coro)


class TestTombstoneApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._orig_db_path = db_mod.DB_PATH
        db_mod.DB_PATH = _DB
        cls.root = tempfile.mkdtemp()
        cls.original = os.path.join(cls.root, "Database", "Someone", "Crap Match.flac")
        cls.trashed = os.path.join(cls.root, "#recycle", "Database", "Someone", "Crap Match.flac")
        os.makedirs(os.path.dirname(cls.trashed))
        with open(cls.trashed, "wb") as f:
            f.write(b"FLAC")
        _seed(_DB, cls.trashed, cls.original)

    @classmethod
    def tearDownClass(cls):
        db_mod.DB_PATH = cls._orig_db_path

    def _stage(self, tid):
        with db_mod.get_db() as conn:
            return conn.execute("SELECT pipeline_stage FROM tracks WHERE id = ?", (tid,)).fetchone()[0]

    def test_errors_splits_tombstones_from_ignored(self):
        out = _run(tracks_mod.get_error_tracks())
        self.assertEqual([t["id"] for t in out["deleted_in_lexicon"]], [1])
        self.assertEqual([t["id"] for t in out["ignored"]], [2])
        self.assertEqual(out["total_deleted_in_lexicon"], 1)
        self.assertEqual(out["total_ignored"], 1)
        self.assertEqual(out["deleted_in_lexicon"][0]["tombstone"]["reason"], "deleted_in_lexicon")

    def test_retry_unignore_reject_refuse_tombstone(self):
        for fn in (tracks_mod.retry_track, tracks_mod.unignore_track, matching_mod.reject_match):
            with self.assertRaises(HTTPException) as cm:
                _run(fn(1))
            self.assertEqual(cm.exception.status_code, 409, fn.__name__)
        self.assertEqual(self._stage(1), "ignored")

    def test_bulk_retry_skips_tombstone(self):
        from models import BulkRetryRequest
        out = _run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(track_ids=[1, 3])))
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["skipped"], 1)
        self.assertEqual(self._stage(1), "ignored")
        self.assertEqual(self._stage(3), "new")

    def test_plain_ignore_still_unignores(self):
        _run(tracks_mod.unignore_track(2))
        self.assertEqual(self._stage(2), "new")

    def test_zz_restore_untrashes_and_rearms(self):  # runs last: it consumes the tombstone
        out = _run(tracks_mod.restore_track(1))
        self.assertEqual(out["restored_file"], self.original)
        self.assertTrue(os.path.exists(self.original))
        self.assertFalse(os.path.exists(self.trashed))
        self.assertEqual(self._stage(1), "new")
        with db_mod.get_db() as conn:
            row = conn.execute("SELECT is_protected, file_path, lexicon_track_id FROM tracks WHERE id = 1").fetchone()
            self.assertEqual(tuple(row), (0, self.original, None))
            self.assertIsNotNone(conn.execute("SELECT restored_at FROM tombstones WHERE track_id = 1").fetchone()[0])
        # Second restore: nothing live to restore.
        with self.assertRaises(HTTPException) as cm:
            _run(tracks_mod.restore_track(1))
        self.assertEqual(cm.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
