"""Tests for the quality rechecker.

An upgrade loop has three ways to go wrong, and all three are silent:

  * It never terminates -- without a cutoff, "find something better" chases hi-res
    forever and re-downloads the library every cycle.
  * It churns -- accepting a sideways move re-downloads the same track endlessly.
  * It strands files -- finding a better copy while nothing can rewrite Lexicon's
    Track.location leaves the upgrade on disk, unreferenced, while the old file
    keeps playing.

Each is pinned below.
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

from tasks import quality  # noqa: E402
from tasks import quality_upgrade as qu  # noqa: E402

FLAC = {"verify_codec": "flac", "verify_sample_rate": 44100,
        "verify_bit_depth": 16, "quality_bit_rate": 900_000}
MP3 = {"verify_codec": "mp3", "verify_sample_rate": 44100,
       "verify_bit_depth": 0, "quality_bit_rate": 320_000}
HIRES = {"verify_codec": "flac", "verify_sample_rate": 96000,
         "verify_bit_depth": 24, "quality_bit_rate": 4_600_000}


def _seed(path, tracks):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT, track_id INTEGER, message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY, artist TEXT, title TEXT, duration_ms INTEGER,
            file_path TEXT, lexicon_track_id TEXT, pipeline_stage TEXT,
            quality_tier TEXT, quality_score INTEGER, quality_bit_rate INTEGER,
            verify_codec TEXT, verify_sample_rate INTEGER, verify_bit_depth INTEGER,
            quality_checked_at TEXT, below_target INTEGER DEFAULT 0,
            upgrade_state TEXT, upgrade_attempts INTEGER DEFAULT 0,
            upgrade_checked_at TEXT, updated_at TEXT);
        CREATE TABLE relocation_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT, track_id INTEGER NOT NULL,
            lexicon_track_id TEXT, old_path TEXT, new_path TEXT NOT NULL,
            old_score INTEGER, new_score INTEGER, old_tier TEXT, new_tier TEXT,
            state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER DEFAULT 0,
            error TEXT, created_at TEXT DEFAULT (datetime('now')), applied_at TEXT);
        CREATE UNIQUE INDEX idx_relocation_one_pending
            ON relocation_queue(track_id) WHERE state = 'pending';
    """)
    for i, (name, q) in enumerate(tracks, start=1):
        conn.execute(
            """INSERT INTO tracks (id, artist, title, duration_ms, file_path,
                   lexicon_track_id, pipeline_stage, verify_codec,
                   verify_sample_rate, verify_bit_depth, quality_bit_rate)
               VALUES (?,?,?,?,?,?, 'complete', ?,?,?,?)""",
            (i, "A", name, 200_000, f"/music/{name}.x", str(1000 + i),
             q["verify_codec"], q["verify_sample_rate"], q["verify_bit_depth"],
             q["quality_bit_rate"]))
    conn.commit()
    conn.close()


class CandidateSelectionTest(unittest.TestCase):
    def setUp(self):
        self.db = os.path.join(tempfile.mkdtemp(prefix="qu_"), "t.db")
        _seed(self.db, [("mp3only", MP3), ("already_flac", FLAC), ("hires", HIRES)])
        self.profile = quality.resolve_profile(lambda k: None)   # cutoff = lossless

    def test_only_below_cutoff_tracks_are_hunted(self):
        got = qu.find_candidates(self.db, self.profile, 10)
        self.assertEqual([t["title"] for t in got], ["mp3only"],
                         "anything at or above the cutoff must be left alone")

    def test_a_hires_cutoff_keeps_hunting_past_lossless(self):
        chasing = quality.resolve_profile(
            lambda k: "hi-res" if k == "quality_cutoff_tier" else None)
        titles = {t["title"] for t in qu.find_candidates(self.db, chasing, 10)}
        self.assertIn("mp3only", titles)
        self.assertIn("already_flac", titles)
        self.assertNotIn("hires", titles, "the top of the ladder is always done")

    def test_exhausted_tracks_are_not_retried(self):
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET upgrade_state='exhausted' WHERE title='mp3only'")
        conn.commit(); conn.close()
        self.assertEqual(qu.find_candidates(self.db, self.profile, 10), [])

    def test_a_pending_relocation_blocks_re_staging(self):
        # Otherwise the rechecker stages the same swap on every cycle until the
        # relocator drains.
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO relocation_queue (track_id, new_path) VALUES (1, '/n.flac')")
        conn.commit(); conn.close()
        self.assertEqual(qu.find_candidates(self.db, self.profile, 10), [])

    def test_the_partial_index_enforces_one_pending_per_track(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO relocation_queue (track_id, new_path) VALUES (1, '/a.flac')")
        conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO relocation_queue (track_id, new_path) VALUES (1, '/b.flac')")
            conn.commit()
        conn.close()

    def test_applied_rows_do_not_block_a_future_upgrade(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO relocation_queue (track_id, new_path, state)"
                     " VALUES (1, '/a.flac', 'applied')")
        conn.commit(); conn.close()
        self.assertEqual(len(qu.find_candidates(self.db, self.profile, 10)), 1)


class SafetyGateTest(unittest.TestCase):
    def setUp(self):
        self.db = os.path.join(tempfile.mkdtemp(prefix="qug_"), "t.db")
        _seed(self.db, [("mp3only", MP3)])

    def _set(self, **kw):
        conn = sqlite3.connect(self.db)
        for k, v in kw.items():
            conn.execute("INSERT OR REPLACE INTO app_config (key,value) VALUES (?,?)", (k, str(v)))
        conn.commit(); conn.close()

    def test_stages_nothing_when_relocation_is_off(self):
        # THE stranding guard: finding an upgrade that nothing can install leaves a
        # better file on disk while Lexicon keeps playing the worse one.
        self._set(relocation_enabled=0)
        result = qu.run(self.db)
        self.assertIn("relocation_enabled", result["skipped"])
        self.assertEqual(result["staged"], 0)

    def test_disabled_outright_does_nothing(self):
        self._set(quality_upgrade_enabled=0, relocation_enabled=1)
        self.assertIn("quality_upgrade_enabled", qu.run(self.db)["skipped"])

    def test_low_disk_stops_the_cycle(self):
        self._set(relocation_enabled=1, quality_upgrade_min_free_gb=999999)
        with mock.patch.object(qu.shutil, "disk_usage") as du:
            du.return_value = mock.Mock(free=1e9)
            self.assertIn("free", qu.run(self.db)["skipped"])


class UpgradeDecisionTest(unittest.TestCase):
    """What actually gets staged."""

    def setUp(self):
        self.db = os.path.join(tempfile.mkdtemp(prefix="qud_"), "t.db")
        _seed(self.db, [("mp3only", MP3)])
        self.track = qu.find_candidates(
            self.db, quality.resolve_profile(lambda k: None), 1)[0]

    def _run(self, candidate_probe, gate_passed=True):
        staged = {}
        cands = [{"username": "peer", "filename": "x.flac", "size": 30_000_000}]
        fake_sf = mock.MagicMock()
        fake_sf.search_best_available.return_value = (quality.TIER_LOSSLESS, cands, "q")
        fake_sf.MAX_CANDIDATES = 5
        fake_sf.download_candidate.return_value = "/tmp/dl.flac"
        fake_sf._move_into_library.return_value = "/music/new.flac"
        with mock.patch.dict(sys.modules, {"tasks.soulseek_fallback": fake_sf}), \
             mock.patch("tasks.lossless_verify.verify_lossless",
                        return_value={"passed": gate_passed, "reasons": [], "checks": {}}), \
             mock.patch.object(quality, "score_file",
                               return_value=quality.score_probe(candidate_probe)), \
             mock.patch.object(qu, "_stage_replacement",
                               side_effect=lambda *a, **k: staged.update(done=True)):
            import tasks
            with mock.patch.object(tasks, "soulseek_fallback", fake_sf, create=True):
                result = qu.process_one(self.db, self.track, mock.Mock(),
                                        quality.resolve_profile(lambda k: None))
        return result, staged

    def test_a_genuine_improvement_is_staged(self):
        ok, staged = self._run({"codec": "flac", "sample_rate": 44100,
                                "bit_depth": 16, "bit_rate": 900_000})
        self.assertTrue(ok)
        self.assertTrue(staged.get("done"))

    def test_a_sideways_move_is_not_staged(self):
        # Same tier as what we already have. Re-downloading it is pure churn.
        ok, staged = self._run({"codec": "mp3", "sample_rate": 44100,
                                "bit_depth": 0, "bit_rate": 320_000})
        self.assertFalse(ok)
        self.assertFalse(staged.get("done"))

    def test_a_fake_flac_is_rejected_by_the_lossless_gate(self):
        ok, _ = self._run({"codec": "flac", "sample_rate": 44100,
                           "bit_depth": 16, "bit_rate": 900_000}, gate_passed=False)
        self.assertFalse(ok, "a lossless tier that fails the lossless gate is a fake")

    def test_attempts_are_recorded_so_the_hunt_terminates(self):
        self._run({"codec": "mp3", "sample_rate": 44100, "bit_depth": 0,
                   "bit_rate": 320_000})
        conn = sqlite3.connect(self.db)
        n = conn.execute("SELECT upgrade_attempts FROM tracks WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)


if __name__ == "__main__":
    unittest.main()


class ScoreBackfillTest(unittest.TestCase):
    """Scoring only happens at verification, so a library built before the ladder
    existed is invisible to the rechecker -- it cannot evaluate what it cannot see.

    Measured live: 4,978 of 5,163 complete tracks had no score at all.
    """

    def setUp(self):
        self.db = os.path.join(tempfile.mkdtemp(prefix="qsb_"), "t.db")
        _seed(self.db, [("a", MP3), ("b", FLAC)])
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET quality_tier=NULL")
        conn.commit(); conn.close()

    def _tiers(self):
        conn = sqlite3.connect(self.db)
        try:
            return dict(conn.execute("SELECT title, quality_tier FROM tracks"))
        finally:
            conn.close()

    def test_a_missing_file_is_marked_not_retried_forever(self):
        # The seeded paths do not exist. Re-probing them every cycle would burn the
        # batch and hide the real problem, which is a stale path.
        qu.backfill_scores(self.db, 10)
        self.assertEqual(set(self._tiers().values()), {"missing-file"})

    def test_a_real_file_is_scored(self):
        import subprocess as sp, shutil as sh
        tmp = tempfile.mkdtemp(prefix="qsbf_")
        self.addCleanup(sh.rmtree, tmp, True)
        flac = os.path.join(tmp, "real.flac")
        sp.run(["ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                "-i", "anoisesrc=d=2:c=pink:r=44100", "-ac", "2",
                "-ar", "44100", "-sample_fmt", "s16", flac], check=True)
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET file_path=? WHERE title='a'", (flac,))
        conn.commit(); conn.close()

        self.assertEqual(qu.backfill_scores(self.db, 10), 1)
        self.assertEqual(self._tiers()["a"], "lossless")

    def test_scoring_runs_even_when_upgrades_are_gated_off(self):
        # It is read-only, and it is what makes the Settings histogram meaningful
        # before anything is switched on.
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT OR REPLACE INTO app_config (key,value) VALUES ('relocation_enabled','0')")
        conn.commit(); conn.close()
        result = qu.run(self.db)
        self.assertIn("relocation_enabled", result["skipped"])
        self.assertIn("scored", result)

    def test_already_scored_tracks_are_not_rescored(self):
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET quality_tier='lossless'")
        conn.commit(); conn.close()
        self.assertEqual(qu.backfill_scores(self.db, 10), 0)


class InPlaceReplacementTest(unittest.TestCase):
    """The seamless path: same container means Lexicon's location never changes.

    That is what removes the "quit Lexicon" requirement for the common case. On
    this library 845 of 939 scored tracks are lossless climbing within their own
    container, so no database write is involved at all -- the bytes at the existing
    path simply get better.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="inplace_")
        self.db = os.path.join(self.dir, "t.db")
        self.old = os.path.join(self.dir, "song.flac")
        with open(self.old, "wb") as f:
            f.write(b"OLD" * 1000)
        _seed(self.db, [("song", MP3)])
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE tracks SET file_path=? WHERE id=1", (self.old,))
        conn.commit(); conn.close()
        self.better = os.path.join(self.dir, "better.flac")
        with open(self.better, "wb") as f:
            f.write(b"NEW" * 2000)

    def _scores(self):
        return (quality.score_probe(MP3),
                quality.score_probe({"codec": "flac", "sample_rate": 44100,
                                     "bit_depth": 16, "bit_rate": 900_000}))

    def test_same_extension_is_detected(self):
        self.assertTrue(qu._same_container("/a/x.flac", "/tmp/y.flac"))
        self.assertTrue(qu._same_container("/a/x.FLAC", "/tmp/y.flac"))
        self.assertFalse(qu._same_container("/a/x.m4a", "/tmp/y.flac"))

    def test_the_file_is_replaced_at_the_same_path(self):
        old_score, new_score = self._scores()
        ok = qu.replace_in_place(self.db, {"id": 1, "file_path": self.old},
                                 self.better, old_score, new_score)
        self.assertTrue(ok)
        self.assertEqual(open(self.old, "rb").read(), b"NEW" * 2000)
        self.assertTrue(os.path.exists(self.old), "the path itself must not move")

    def test_the_original_is_kept_so_it_is_reversible(self):
        old_score, new_score = self._scores()
        qu.replace_in_place(self.db, {"id": 1, "file_path": self.old},
                            self.better, old_score, new_score)
        kept = os.path.join(self.dir, ".superseded", "song.flac")
        self.assertTrue(os.path.exists(kept))
        self.assertEqual(open(kept, "rb").read(), b"OLD" * 1000)

    def test_no_staging_file_is_left_behind(self):
        old_score, new_score = self._scores()
        qu.replace_in_place(self.db, {"id": 1, "file_path": self.old},
                            self.better, old_score, new_score)
        self.assertEqual([f for f in os.listdir(self.dir) if f.endswith(".incoming")], [])

    def test_quality_is_recorded_and_the_hunt_ends(self):
        old_score, new_score = self._scores()
        qu.replace_in_place(self.db, {"id": 1, "file_path": self.old},
                            self.better, old_score, new_score)
        conn = sqlite3.connect(self.db)
        tier, bt, state = conn.execute(
            "SELECT quality_tier, below_target, upgrade_state FROM tracks WHERE id=1"
        ).fetchone()
        conn.close()
        self.assertEqual(tier, "lossless")
        self.assertEqual(bt, 0)
        self.assertEqual(state, "resolved")

    def test_a_missing_original_is_refused(self):
        old_score, new_score = self._scores()
        self.assertFalse(qu.replace_in_place(
            self.db, {"id": 1, "file_path": "/nope/gone.flac"},
            self.better, old_score, new_score))
