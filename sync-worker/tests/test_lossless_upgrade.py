"""Tests for the lossy-only auto-upgrade re-check (no network required).

Covers: the pending marker + schema add, the per-track re-check throttle, the
swap-on-verified-lossless-found path, and the HARD never-remove-without-replacement
guard (both when nothing lossless is found and when the Lexicon relocate can't be
confirmed).
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import lossless_upgrade as lu  # noqa: E402


def _make_db(with_markers: bool = False):
    """Temp DB with the tracks/app_config/activity_log tables the module touches.

    When with_markers is False the marker columns are absent, so ensure_schema() has
    real work to do (mirrors an existing pre-feature DB)."""
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    marker_cols = (
        ", lossless_upgrade_pending INTEGER NOT NULL DEFAULT 0, last_upgrade_check TEXT"
        if with_markers else ""
    )
    conn.executescript(
        f"""
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
            track_id INTEGER, message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY,
            artist TEXT, title TEXT, duration_ms INTEGER,
            file_path TEXT, download_source TEXT, match_source TEXT,
            verify_status TEXT, verify_is_genuine_lossless INTEGER,
            lexicon_track_id TEXT, is_protected INTEGER NOT NULL DEFAULT 0,
            pipeline_stage TEXT NOT NULL DEFAULT 'complete', pipeline_error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')){marker_cols}
        );
        """
    )
    conn.commit()
    conn.close()
    return path


def _insert(db, **kw):
    conn = sqlite3.connect(db)
    cols = ",".join(kw)
    qs = ",".join("?" * len(kw))
    conn.execute(f"INSERT INTO tracks ({cols}) VALUES ({qs})", tuple(kw.values()))
    conn.commit()
    conn.close()


def _row(db, track_id):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    r = conn.execute("SELECT * FROM tracks WHERE id=?", (track_id,)).fetchone()
    conn.close()
    return dict(r)


class TestSchema(unittest.TestCase):
    def test_ensure_schema_adds_columns_idempotently(self):
        db = _make_db(with_markers=False)
        try:
            lu.ensure_schema(db)
            cols = {r[1] for r in sqlite3.connect(db).execute("PRAGMA table_info(tracks)")}
            self.assertIn("lossless_upgrade_pending", cols)
            self.assertIn("last_upgrade_check", cols)
            # second call is a no-op (must not raise)
            lu.ensure_schema(db)
        finally:
            os.remove(db)


class TestMarkPending(unittest.TestCase):
    def setUp(self):
        self.db = _make_db(with_markers=True)

    def tearDown(self):
        os.remove(self.db)

    def test_marks_verified_lossy_and_lossy_extension(self):
        _insert(self.db, id=1, artist="Mob Tactics", title="Labyrinth",
                file_path="/music/Mob Tactics/Mob Tactics - Labyrinth.m4a",
                verify_is_genuine_lossless=None, pipeline_stage="complete")   # lossy ext
        _insert(self.db, id=2, artist="Annix", title="Shai Hulud VIP",
                file_path="/music/Annix/Annix - Shai Hulud VIP.flac",
                verify_is_genuine_lossless=0, pipeline_stage="complete")      # verified lossy
        n = lu.mark_pending(self.db)
        self.assertEqual(n, 2)
        self.assertEqual(_row(self.db, 1)["lossless_upgrade_pending"], 1)
        self.assertEqual(_row(self.db, 2)["lossless_upgrade_pending"], 1)

    def test_does_not_mark_lossless_or_protected_or_incomplete(self):
        _insert(self.db, id=1, artist="A", title="Genuine FLAC",
                file_path="/music/A/A - Genuine FLAC.flac",
                verify_is_genuine_lossless=1, pipeline_stage="complete")      # lossless
        _insert(self.db, id=2, artist="B", title="Protected lossy",
                file_path="/music/B/B - x.mp3", is_protected=1,
                verify_is_genuine_lossless=0, pipeline_stage="complete")      # protected
        _insert(self.db, id=3, artist="C", title="Errored lossy",
                file_path="/music/C/C - x.mp3",
                verify_is_genuine_lossless=0, pipeline_stage="error")         # not complete
        _insert(self.db, id=4, artist="D", title="Unknown container",
                file_path="/music/D/D - x.flac",
                verify_is_genuine_lossless=None, pipeline_stage="complete")   # looks lossless, unknown
        n = lu.mark_pending(self.db)
        self.assertEqual(n, 0)
        for tid in (1, 2, 3, 4):
            self.assertEqual(_row(self.db, tid)["lossless_upgrade_pending"], 0)

    def test_marking_is_idempotent(self):
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/A/A - B.mp3",
                verify_is_genuine_lossless=0, pipeline_stage="complete")
        self.assertEqual(lu.mark_pending(self.db), 1)
        self.assertEqual(lu.mark_pending(self.db), 0)  # already marked -> not re-marked


class TestThrottle(unittest.TestCase):
    def setUp(self):
        self.db = _make_db(with_markers=True)

    def tearDown(self):
        os.remove(self.db)

    def test_never_checked_is_due(self):
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                pipeline_stage="complete", lossless_upgrade_pending=1)
        due = lu.due_tracks(self.db, 10)
        self.assertEqual([t["id"] for t in due], [1])

    def test_recently_checked_is_not_due(self):
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                pipeline_stage="complete", lossless_upgrade_pending=1)
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET last_upgrade_check = datetime('now') WHERE id=1")
        conn.commit(); conn.close()
        self.assertEqual(lu.due_tracks(self.db, 10), [])  # inside the throttle window

    def test_old_check_is_due_again(self):
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                pipeline_stage="complete", lossless_upgrade_pending=1)
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET last_upgrade_check = datetime('now','-30 days') WHERE id=1")
        conn.commit(); conn.close()
        self.assertEqual([t["id"] for t in lu.due_tracks(self.db, 10)], [1])

    def test_only_pending_complete_tracks_are_due(self):
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                pipeline_stage="complete", lossless_upgrade_pending=0)        # not marked
        _insert(self.db, id=2, artist="C", title="D", file_path="/music/y.mp3",
                pipeline_stage="error", lossless_upgrade_pending=1)           # not complete
        self.assertEqual(lu.due_tracks(self.db, 10), [])


class TestAttemptUpgrade(unittest.TestCase):
    def setUp(self):
        self.db = _make_db(with_markers=True)
        _insert(self.db, id=1, artist="Mob Tactics", title="Labyrinth",
                file_path="/music/Mob Tactics/Mob Tactics - Labyrinth.m4a",
                download_source="tidal", verify_is_genuine_lossless=0,
                lexicon_track_id="10971", pipeline_stage="complete",
                lossless_upgrade_pending=1)

    def tearDown(self):
        os.remove(self.db)

    def test_swap_on_verified_lossless_found(self):
        new_path = "/music/Mob Tactics/Mob Tactics - Labyrinth.flac"
        with mock.patch.object(lu, "_lexicon_can_relocate", return_value=True), \
             mock.patch.object(lu, "_source_verified_lossless", return_value=(new_path, "soulseek")), \
             mock.patch.object(lu, "_relocate_in_lexicon", return_value=True):
            result = lu._attempt_upgrade(self.db, _row(self.db, 1))
        self.assertEqual(result, "upgraded")
        row = _row(self.db, 1)
        self.assertEqual(row["file_path"], new_path)               # swapped to lossless
        self.assertEqual(row["download_source"], "soulseek")
        self.assertEqual(row["verify_is_genuine_lossless"], 1)
        self.assertEqual(row["lossless_upgrade_pending"], 0)       # marker cleared
        self.assertEqual(row["pipeline_stage"], "complete")        # stays complete
        self.assertIsNotNone(row["last_upgrade_check"])

    def test_never_removes_lossy_when_nothing_found(self):
        with mock.patch.object(lu, "_lexicon_can_relocate", return_value=True), \
             mock.patch.object(lu, "_source_verified_lossless", return_value=None):
            result = lu._attempt_upgrade(self.db, _row(self.db, 1))
        self.assertEqual(result, "none")
        row = _row(self.db, 1)
        # lossy is untouched; only the throttle timestamp advanced
        self.assertTrue(row["file_path"].endswith(".m4a"))
        self.assertEqual(row["verify_is_genuine_lossless"], 0)
        self.assertEqual(row["lossless_upgrade_pending"], 1)       # still pending
        self.assertEqual(row["pipeline_stage"], "complete")
        self.assertIsNotNone(row["last_upgrade_check"])

    def test_never_upgrades_when_relocate_unconfirmed(self):
        # A verified-lossless copy is sourced but Lexicon relocate can't be confirmed:
        # the new copy must be discarded and the lossy kept (no false 'upgraded' state).
        staged = tempfile.mktemp(suffix=".flac")
        with open(staged, "wb") as f:
            f.write(b"fake-flac-bytes")
        try:
            with mock.patch.object(lu, "_lexicon_can_relocate", return_value=True), \
                 mock.patch.object(lu, "_source_verified_lossless", return_value=(staged, "tidal")), \
                 mock.patch.object(lu, "_relocate_in_lexicon", return_value=False):
                result = lu._attempt_upgrade(self.db, _row(self.db, 1))
            self.assertEqual(result, "staged")
            self.assertFalse(os.path.exists(staged))               # orphan discarded
            row = _row(self.db, 1)
            self.assertTrue(row["file_path"].endswith(".m4a"))     # lossy kept
            self.assertEqual(row["verify_is_genuine_lossless"], 0)
            self.assertEqual(row["lossless_upgrade_pending"], 1)   # still pending -> retry later
        finally:
            if os.path.exists(staged):
                os.remove(staged)

    def test_blocked_when_lexicon_cannot_relocate(self):
        # Lexicon exposes no editable location: never source/download a copy we cannot
        # install. Keep the lossy untouched, advance only the throttle, no swap.
        with mock.patch.object(lu, "_lexicon_can_relocate", return_value=False), \
             mock.patch.object(lu, "_source_verified_lossless") as src:
            result = lu._attempt_upgrade(self.db, _row(self.db, 1))
        self.assertEqual(result, "blocked")
        src.assert_not_called()                                    # no wasted download
        row = _row(self.db, 1)
        self.assertTrue(row["file_path"].endswith(".m4a"))         # lossy kept
        self.assertEqual(row["verify_is_genuine_lossless"], 0)
        self.assertEqual(row["lossless_upgrade_pending"], 1)       # still pending
        self.assertIsNotNone(row["last_upgrade_check"])            # throttle advanced


class TestIterTracksEnvelopes(unittest.TestCase):
    def test_singular_track_envelope(self):
        # Live GET /v1/track?id=<n> returns {"data": {"track": {...}}} (SINGULAR).
        data = {"data": {"track": {"id": 42, "location": "/Volumes/music/x.flac"}}}
        got = list(lu._iter_tracks(data))
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["id"], 42)

    def test_plural_tracks_envelope(self):
        data = {"data": {"tracks": [{"id": 1}, {"id": 2}]}}
        self.assertEqual([t["id"] for t in lu._iter_tracks(data)], [1, 2])

    def test_location_confirm_reads_back_singular(self):
        # _lexicon_location_is must confirm a match from the singular envelope.
        class _Resp:
            status_code = 200
            def json(self):
                return {"data": {"track": {"id": 7, "location": "/Volumes/music/y.flac"}}}
        class _Client:
            def get(self, *a, **k):
                return _Resp()
        self.assertTrue(lu._lexicon_location_is(_Client(), "7", "/Volumes/music/y.flac"))
        self.assertFalse(lu._lexicon_location_is(_Client(), "7", "/Volumes/music/other.flac"))


class TestRunLoopGuards(unittest.TestCase):
    def setUp(self):
        self.db = _make_db(with_markers=False)  # exercises ensure_schema inside run

    def tearDown(self):
        os.remove(self.db)

    def test_scan_mode_is_noop(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO app_config (key, value) VALUES ('sync_mode','scan')")
        conn.commit(); conn.close()
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                verify_is_genuine_lossless=0, pipeline_stage="complete")
        with mock.patch.object(lu, "_attempt_upgrade") as m:
            lu.run_lossless_upgrade(self.db)
        m.assert_not_called()
        # schema was still ensured, but no marking/sourcing happened in scan mode
        self.assertEqual(_row(self.db, 1)["lossless_upgrade_pending"], 0)

    def test_disabled_is_noop(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO app_config (key, value) VALUES ('lossless_upgrade_enabled','0')")
        conn.commit(); conn.close()
        with mock.patch.object(lu, "_attempt_upgrade") as m:
            lu.run_lossless_upgrade(self.db)
        m.assert_not_called()

    def test_full_mode_marks_and_processes(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO app_config (key, value) VALUES ('sync_mode','full')")
        conn.commit(); conn.close()
        _insert(self.db, id=1, artist="A", title="B", file_path="/music/x.mp3",
                verify_is_genuine_lossless=0, pipeline_stage="complete")
        with mock.patch.object(lu, "_attempt_upgrade", return_value="none") as m:
            lu.run_lossless_upgrade(self.db)
        # marked pending then handed to _attempt_upgrade
        self.assertEqual(_row(self.db, 1)["lossless_upgrade_pending"], 1)
        self.assertEqual(m.call_count, 1)


if __name__ == "__main__":
    unittest.main()
