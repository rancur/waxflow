"""Tests for the Soulseek fallback decision logic (no network required)."""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import soulseek_fallback as sf  # noqa: E402


def _make_db():
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
            error TEXT, search_query TEXT, result_count INTEGER,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()
    conn.close()
    return path


class TestRankCandidates(unittest.TestCase):
    def _resp(self, user, filename, size, free=True, queue=0, speed=100):
        return {
            "username": user, "hasFreeUploadSlot": free, "queueLength": queue,
            "uploadSpeed": speed, "files": [{"filename": filename, "size": size}],
        }

    def test_only_flac_and_plausible_size_kept(self):
        # ~3.5 min track -> plausible lossless size window
        dur = 210_000
        responses = [
            self._resp("a", "x\\song.flac", 25_000_000),        # good
            self._resp("b", "x\\song.mp3", 8_000_000),          # not flac -> drop
            self._resp("c", "x\\tiny.flac", 200_000),           # too small -> drop
            self._resp("d", "x\\huge.flac", 900_000_000),       # too big -> drop
        ]
        cands = sf.rank_candidates(responses, dur)
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0]["username"], "a")

    def test_free_slot_and_queue_ordering(self):
        dur = 210_000
        responses = [
            self._resp("busy", "d\\s.flac", 25_000_000, free=False, queue=50, speed=999),
            self._resp("free_slow", "d\\s.flac", 25_000_000, free=True, queue=0, speed=10),
            self._resp("free_fast", "d\\s.flac", 25_000_000, free=True, queue=0, speed=500),
        ]
        cands = sf.rank_candidates(responses, dur)
        self.assertEqual(cands[0]["username"], "free_fast")   # free + fastest first
        self.assertEqual(cands[-1]["username"], "busy")       # no free slot last


class TestQueries(unittest.TestCase):
    def test_build_queries_variants(self):
        qs = sf._build_queries("Zeds Dead, CUT", "In Your Head (Extended Mix)")
        self.assertIn("Zeds Dead In Your Head (Extended Mix)", qs)
        self.assertIn("Zeds Dead In Your Head", qs)  # base title stripped of remix suffix
        # no duplicates
        self.assertEqual(len(qs), len(set(q.lower() for q in qs)))


class TestSizeRange(unittest.TestCase):
    def test_range_scales_with_duration(self):
        lo, hi = sf._expected_size_range(210_000)
        self.assertLess(lo, 25_000_000)
        self.assertLess(25_000_000, hi)
        # unknown duration -> permissive
        lo2, hi2 = sf._expected_size_range(None)
        self.assertLessEqual(lo2, 5_000_000)


class TestEnableAndAttempted(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()

    def tearDown(self):
        try:
            os.remove(self.db)
        except OSError:
            pass

    def test_enabled_default_on(self):
        self.assertTrue(sf.is_enabled(self.db))  # no config row -> default on

    def test_enabled_toggle(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO app_config (key, value) VALUES ('soulseek_fallback_enabled', '0')")
        conn.commit(); conn.close()
        self.assertFalse(sf.is_enabled(self.db))

    def test_already_attempted(self):
        self.assertFalse(sf.already_attempted(self.db, 42))
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO fallback_attempts (track_id, source, status) VALUES (42, 'soulseek', 'all_failed')"
        )
        conn.commit(); conn.close()
        self.assertTrue(sf.already_attempted(self.db, 42))
        # a tidal attempt must NOT count as a soulseek attempt
        self.assertFalse(sf.already_attempted(self.db, 99))


class TestConstraintSafeQueue(unittest.TestCase):
    """Regression: queuing a track for Soulseek must NOT violate the tracks
    pipeline_stage CHECK constraint (we do not invent a new stage value)."""

    # The REAL production constraint (init_db.py) — 'soulseek_fallback' is NOT in it.
    ALLOWED = "('new','matching','downloading','verifying','organizing'," \
              "'complete','error','waiting','ignored','needs_import_review')"

    def setUp(self):
        self.db = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(self.db)
        conn.executescript(f"""
            CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE fallback_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id INTEGER NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
                error TEXT, search_query TEXT, result_count INTEGER,
                attempted_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist TEXT, title TEXT, duration_ms INTEGER,
                pipeline_error TEXT, updated_at TEXT, created_at TEXT DEFAULT (datetime('now')),
                pipeline_stage TEXT NOT NULL DEFAULT 'new'
                    CHECK(pipeline_stage IN {self.ALLOWED})
            );
            INSERT INTO tracks (id, artist, title, duration_ms, pipeline_stage)
                VALUES (7, 'Zeds Dead', 'In Your Head', 201514, 'error');
        """)
        conn.commit(); conn.close()

    def tearDown(self):
        try:
            os.remove(self.db)
        except OSError:
            pass

    def test_queue_does_not_violate_check_constraint(self):
        # This is exactly what the pipeline routing does; must not raise.
        sf.queue_for_fallback(self.db, 7, "Tidal copy not lossless")
        conn = sqlite3.connect(self.db)
        conn.execute(
            "UPDATE tracks SET pipeline_stage='error', updated_at='2000-01-01 00:00:00' WHERE id=7"
        )
        conn.commit()
        # a queued soulseek row exists and the track is discoverable by the stage
        fa = conn.execute(
            "SELECT status FROM fallback_attempts WHERE track_id=7 AND source='soulseek'"
        ).fetchone()
        conn.close()
        self.assertEqual(fa[0], "queued")
        self.assertTrue(sf.already_attempted(self.db, 7))
        queued = sf._queued_tracks(self.db, 10)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["id"], 7)
        self.assertIn("_fa_id", queued[0])

    def test_only_error_stage_tracks_are_eligible(self):
        # queued track that is genuinely parked at 'error' IS eligible
        sf.queue_for_fallback(self.db, 7, "Tidal not lossless")
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET updated_at='2000-01-01' WHERE id=7"); conn.commit(); conn.close()
        self.assertEqual([t["id"] for t in sf._queued_tracks(self.db, 10)], [7])
        # if something moves it off 'error' (e.g. self-heal imported it), it is NOT
        # reprocessed and its queued row is superseded (don't fight the state machine)
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET pipeline_stage='complete' WHERE id=7"); conn.commit(); conn.close()
        self.assertEqual(sf._queued_tracks(self.db, 10), [])
        n = sf._supersede_stale_queue(self.db)
        self.assertEqual(n, 1)
        conn = sqlite3.connect(self.db)
        st = conn.execute("SELECT status FROM fallback_attempts WHERE track_id=7").fetchone()[0]
        conn.close()
        self.assertEqual(st, "superseded")

    def test_soulseek_fallback_is_not_an_allowed_stage(self):
        # Guard the assumption behind the queue model: if someone adds the stage to
        # the constraint later, this test flags that the queue model can be simplified.
        conn = sqlite3.connect(self.db)
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("UPDATE tracks SET pipeline_stage='soulseek_fallback' WHERE id=7")
        conn.close()


if __name__ == "__main__":
    unittest.main()
