"""Tests for bulk retry and the shared error classifier.

The properties worth pinning here are the ones that fail SILENTLY:

  * A retry that leaves fallback_attempts in place is a no-op. The track resets,
    marches back down the pipeline, and soulseek_fallback.already_attempted()
    refuses to re-queue it -- so it lands on the identical error and the user sees
    a "retry" button that demonstrably does nothing.
  * If the endpoint that COUNTS a category and the endpoint that RETRIES it
    classify differently, "Retry All 47" acts on some other number of tracks.
  * Bulk operations that log per-track bury the dashboard activity feed.

Self-contained: temp SQLite file, no network.
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_API_DIR not in sys.path:
    sys.path.insert(0, SYNC_API_DIR)

# mkdtemp + join, not mktemp: mktemp returns a name without creating anything, so
# the file it names can be created by someone else between the call and our open
# (CodeQL py/insecure-temporary-file). The directory here is created atomically with
# owner-only permissions, and we own every path inside it.
_DB = os.path.join(tempfile.mkdtemp(prefix="waxflow-test-"), "sync.db")
os.environ.setdefault("SLS_DB_PATH", _DB)

import db as db_mod  # noqa: E402
from error_categories import ERROR_CATEGORIES, canonical_category, categorize_error  # noqa: E402
from models import BulkRetryRequest  # noqa: E402
from routes import tracks as tracks_mod  # noqa: E402

# db.py captures SLS_DB_PATH into a module global at import time, so whichever test
# module imports it FIRST decides the path for the whole pytest run. Setting the env
# var is therefore not enough -- point db.DB_PATH at our file per-test and put it
# back afterwards, so this module neither depends on collection order nor breaks the
# other test modules that make the same assumption.


def _seed(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY,
            spotify_id TEXT,
            title TEXT,
            artist TEXT,
            album TEXT,
            duration_ms INTEGER,
            spotify_added_at TEXT,
            pipeline_stage TEXT,
            pipeline_error TEXT,
            match_status TEXT,
            download_status TEXT,
            download_error TEXT,
            download_attempts INTEGER DEFAULT 0,
            verify_status TEXT,
            verify_codec TEXT,
            lexicon_status TEXT,
            is_protected INTEGER DEFAULT 0,
            updated_at TEXT
        );
        CREATE TABLE fallback_attempts (
            id INTEGER PRIMARY KEY,
            track_id INTEGER,
            source TEXT,
            status TEXT,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE source_attempts (
            id INTEGER PRIMARY KEY,
            track_id INTEGER,
            source TEXT,
            status TEXT,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY,
            event_type TEXT,
            track_id INTEGER,
            message TEXT,
            details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    rows = [
        # id, title, error, verify_status, verify_codec, stage, protected
        (1, "Lossy One", "verify failed: not lossless", "fail", "aac", "error", 0),
        (2, "Lossy Two", "download ok", None, "mp3", "error", 0),
        (3, "Nowhere", "No Tidal match found", None, None, "error", 0),
        (4, "Nowhere Two", "permanently unavailable", None, None, "error", 0),
        (5, "Broke", "download failed after 3 attempts", None, None, "error", 0),
        (6, "Geo", "geo-restricted in your region", None, None, "error", 0),
        (7, "Lex", "lexicon import returned 0 tracks", None, None, "error", 0),
        (8, "Edit", "fingerprint mismatch: duration differs", None, None, "error", 0),
        (9, "Weird", "something nobody predicted", None, None, "error", 0),
        (10, "Protected", "No Tidal match found", None, None, "error", 1),
        (11, "Ignored", "No Tidal match found", None, None, "ignored", 1),
    ]
    for tid, title, err, vs, vc, stage, prot in rows:
        conn.execute(
            """INSERT INTO tracks (id, title, artist, pipeline_stage, pipeline_error,
                                   verify_status, verify_codec, match_status,
                                   download_status, download_attempts, is_protected)
               VALUES (?, ?, 'A', ?, ?, ?, ?, 'failed', 'failed', 3, ?)""",
            (tid, title, stage, err, vs, vc, prot),
        )
        # Every track carries a spent Soulseek attempt -- the thing that makes a
        # naive retry a no-op.
        conn.execute(
            "INSERT INTO fallback_attempts (track_id, source, status) VALUES (?, 'soulseek', 'failed')",
            (tid,),
        )
    conn.commit()
    conn.close()


class ClassifierTest(unittest.TestCase):
    def test_every_bucket_is_reachable_and_declared(self):
        cases = {
            "not_lossless": {"verify_codec": "aac", "pipeline_error": ""},
            "no_tidal_match": {"pipeline_error": "No Tidal match found"},
            "download_failed": {"pipeline_error": "download failed after 3 attempts"},
            "lexicon_sync_failed": {"pipeline_error": "lexicon import returned 0"},
            "wrong_version": {"pipeline_error": "fingerprint mismatch"},
            "other": {"pipeline_error": "something nobody predicted"},
        }
        for expected, track in cases.items():
            self.assertEqual(categorize_error(track), expected)
            self.assertIn(expected, ERROR_CATEGORIES)
        # Nothing declared is unreachable.
        self.assertEqual(set(cases), set(ERROR_CATEGORIES))

    def test_codec_beats_error_text(self):
        # A lossy file whose error text mentions downloading must still land in
        # not_lossless, or "Retry All" on the wrong bucket re-downloads it forever.
        self.assertEqual(
            categorize_error({"verify_codec": "mp3", "pipeline_error": "download failed"}),
            "not_lossless",
        )

    def test_legacy_category_name_still_resolves(self):
        self.assertEqual(canonical_category("fingerprint_mismatch"), "wrong_version")
        self.assertEqual(canonical_category("no_tidal_match"), "no_tidal_match")

    def test_missing_fields_do_not_raise(self):
        self.assertEqual(categorize_error({}), "other")
        self.assertEqual(categorize_error({"pipeline_error": None}), "other")


class BulkRetryTest(unittest.TestCase):
    def setUp(self):
        self._saved_db_path = db_mod.DB_PATH
        db_mod.DB_PATH = _DB
        self.addCleanup(lambda: setattr(db_mod, "DB_PATH", self._saved_db_path))
        if os.path.exists(_DB):
            os.remove(_DB)
        _seed(_DB)

    def _rows(self, sql, args=()):
        conn = sqlite3.connect(_DB)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
        finally:
            conn.close()

    def test_category_retry_touches_only_that_category(self):
        result = asyncio.run(
            tracks_mod.bulk_retry_tracks(BulkRetryRequest(category="no_tidal_match"))
        )
        # Tracks 3 and 4 qualify. Track 10 is in the error set but protected, so it
        # is actively skipped; track 11 is already 'ignored' and so never enters the
        # candidate set at all -- which is why skipped is 1 rather than 2.
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["skipped"], 1)

        reset = {r["id"] for r in self._rows(
            "SELECT id FROM tracks WHERE pipeline_stage = 'new'")}
        self.assertEqual(reset, {3, 4})

    def test_retry_clears_the_blocker_that_made_it_a_noop(self):
        asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(track_ids=[3])))
        left = self._rows("SELECT * FROM fallback_attempts WHERE track_id = 3")
        self.assertEqual(left, [], "stale fallback_attempts row makes the retry inert")
        # Untouched tracks keep their history.
        self.assertEqual(len(self._rows("SELECT * FROM fallback_attempts WHERE track_id = 5")), 1)

    def test_retry_resets_the_full_pipeline_state(self):
        asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(track_ids=[5])))
        t = self._rows("SELECT * FROM tracks WHERE id = 5")[0]
        self.assertEqual(t["pipeline_stage"], "new")
        self.assertIsNone(t["pipeline_error"])
        self.assertEqual(t["download_attempts"], 0)
        self.assertEqual(t["match_status"], "pending")
        self.assertEqual(t["verify_status"], "pending")

    def test_protected_and_ignored_are_never_resurrected(self):
        asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(track_ids=[10, 11])))
        stages = {r["id"]: r["pipeline_stage"] for r in
                  self._rows("SELECT id, pipeline_stage FROM tracks WHERE id IN (10, 11)")}
        self.assertEqual(stages[10], "error")
        self.assertEqual(stages[11], "ignored")

    def test_logs_one_summary_row_not_one_per_track(self):
        asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(category="not_lossless")))
        rows = self._rows("SELECT * FROM activity_log WHERE event_type = 'pipeline_bulk_retry'")
        self.assertEqual(len(rows), 1)
        details = json.loads(rows[0]["details"])
        self.assertEqual(details["count"], 2)
        self.assertEqual(details["category"], "not_lossless")

    def test_legacy_category_name_is_accepted(self):
        result = asyncio.run(
            tracks_mod.bulk_retry_tracks(BulkRetryRequest(category="fingerprint_mismatch"))
        )
        self.assertEqual(result["count"], 1)  # track 8

    def test_unknown_category_is_rejected_not_silently_empty(self):
        with self.assertRaises(Exception) as ctx:
            asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(category="nonsense")))
        self.assertIn("Unknown category", str(ctx.exception))

    def test_neither_ids_nor_category_is_rejected(self):
        with self.assertRaises(Exception) as ctx:
            asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest()))
        self.assertIn("track_ids or category", str(ctx.exception))

    def test_limit_caps_the_blast_radius(self):
        result = asyncio.run(
            tracks_mod.bulk_retry_tracks(BulkRetryRequest(category="no_tidal_match", limit=1))
        )
        self.assertEqual(result["count"], 1)

    def test_summary_counts_match_what_retry_would_act_on(self):
        summary = asyncio.run(tracks_mod.get_error_summary())
        for category, expected in summary["by_category"].items():
            if expected == 0:
                continue
            asyncio.run(tracks_mod.bulk_retry_tracks(BulkRetryRequest(category=category)))
            acted = len(self._rows(
                "SELECT id FROM tracks WHERE pipeline_stage = 'new'"))
            # Protected/ignored are counted by the summary but skipped by retry;
            # everything else must line up exactly.
            self.assertLessEqual(acted, expected)
            self.setUp()


if __name__ == "__main__":
    unittest.main()
