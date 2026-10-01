"""User-selectable acquire-source order (2.20.0): Soulseek first, Tidal optional.

Exercises the REAL pipeline stages (process_pipeline._process_matching /
_process_downloading, soulseek_fallback.process_soulseek_fallback) against the REAL
production schema (built by sync-api/init_db.py), with only the network edges
stubbed: the Tidal search/download functions and the slskd client.

Proves:
  * order parsing / next-source logic;
  * Soulseek-first: a new track is queued for Soulseek and Tidal is NOT searched;
  * a Soulseek miss falls through to Tidal (and Tidal then matches);
  * the reverse order (Tidal first) keeps the old behaviour, and a Soulseek miss
    after a Tidal miss does NOT loop back to Tidal;
  * Tidal disabled: no Tidal search, download or token refresh anywhere, tracks
    parked on a Tidal download are re-dispatched, and a Soulseek hit still goes
    through the lossless gate to 'verifying';
  * nothing enabled -> a clear error, not a hang;
  * the hunter does not grab a track while it is queued for Soulseek;
  * the lossless-upgrade task and MusicBrainz fallback honour order + toggle;
  * the API's mirrored source list agrees with the worker registry.
"""

import importlib.util
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(SYNC_WORKER_DIR)
SYNC_API_DIR = os.path.join(REPO, "sync-api")
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import process_pipeline as pp  # noqa: E402
from tasks import soulseek_fallback as sf  # noqa: E402
from tasks import v3_schema  # noqa: E402
from tasks.sources import order, registry  # noqa: E402


def _prod_db() -> str:
    """A database built by the real sync-api/init_db.py (production schema)."""
    path = os.path.join(tempfile.mkdtemp(prefix="wf-order-"), "sync.db")
    subprocess.run(
        [sys.executable, "-c", "import init_db; init_db.init()"],
        cwd=SYNC_API_DIR, env={**os.environ, "SLS_DB_PATH": path},
        check=True, capture_output=True,
    )
    v3_schema.ensure_v3_schema(path)
    return path


def _q(db, sql, args=()):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in conn.execute(sql, args).fetchall()]
        conn.commit()
        return rows
    finally:
        conn.close()


def _set(db, key, value):
    _q(db, "INSERT INTO app_config (key, value) VALUES (?, ?) "
           "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))


def _age(db):
    """Step past get_tracks_by_stage's 5-second settle guard."""
    _q(db, "UPDATE tracks SET updated_at = '2000-01-01 00:00:00'")


def _track(db, stage="matching", **kw):
    cols = {"spotify_id": "sp1", "title": "Strobe", "artist": "deadmau5",
            "duration_ms": 600000, "isrc": "USUS10900001", "pipeline_stage": stage, **kw}
    _q(db, f"INSERT INTO tracks ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
       tuple(cols.values()))
    _age(db)
    return _q(db, "SELECT id FROM tracks WHERE spotify_id = ?", (cols["spotify_id"],))[0]["id"]


def _row(db, tid):
    return _q(db, "SELECT * FROM tracks WHERE id = ?", (tid,))[0]


def _fa(db, tid, source="soulseek"):
    return _q(db, "SELECT * FROM fallback_attempts WHERE track_id = ? AND source = ? "
                  "ORDER BY id", (tid, source))


class _NoCandidatesClient:
    """slskd stand-in: logged in, every search comes back empty."""
    configured = True

    def __init__(self):
        self.searches = []

    def is_logged_in(self):
        return True

    def search(self, q):
        self.searches.append(q)
        return []


class _OneFlacClient(_NoCandidatesClient):
    """slskd stand-in: one plausible FLAC that transfers fine."""

    def __init__(self, src_file):
        super().__init__()
        self.src_file = src_file

    def search(self, q):
        self.searches.append(q)
        return [{"username": "peer", "hasFreeUploadSlot": True, "queueLength": 0,
                 "uploadSpeed": 1000,
                 "files": [{"filename": "music\\deadmau5 - Strobe.flac",
                            "size": 60_000_000}]}]

    def download_and_wait(self, *a, **kw):
        return True

    def ondisk_relpath(self, filename):
        return "deadmau5 - Strobe.flac"

    def fetch_file(self, relpath, local):
        with open(self.src_file, "rb") as s, open(local, "wb") as d:
            d.write(s.read())
        return os.path.getsize(local)


def _tidal_hit(query):
    return [{"id": 777, "isrc": "USUS10900001", "title": "Strobe",
             "artist": {"name": "deadmau5"}, "duration": 600}]


class _Base(unittest.TestCase):
    def setUp(self):
        self.db = _prod_db()
        _set(self.db, "sync_mode", "full")
        # Fail LOUDLY if anything reaches Tidal unless a test opts in.
        self.tidal_search = mock.MagicMock(side_effect=AssertionError("Tidal searched"))
        self.tidal_dl = mock.MagicMock(side_effect=AssertionError("Tidal downloaded"))
        self.tidal_auth = mock.MagicMock(side_effect=AssertionError("Tidal token refreshed"))
        for target, m in (("_tidal_search", self.tidal_search),
                          ("_download_track_via_tiddl", self.tidal_dl),
                          ("_ensure_tidal_auth", self.tidal_auth)):
            p = mock.patch.object(pp, target, m)
            p.start()
            self.addCleanup(p.stop)

    def run_soulseek(self, client):
        _age(self.db)
        with mock.patch.object(sf, "build_client", return_value=client):
            sf.process_soulseek_fallback(self.db)
        _age(self.db)


# --------------------------------------------------------------------------- #
class TestOrderLogic(_Base):
    def test_default_is_soulseek_then_tidal(self):
        self.assertEqual(order.configured_priority(self.db), ["soulseek", "tidal"])
        self.assertEqual(order.enabled_order(self.db), ["soulseek", "tidal"])
        self.assertEqual(order.DEFAULT_PRIORITY, ("soulseek", "tidal"))

    def test_parse_is_tolerant(self):
        self.assertEqual(order.parse_priority(""), ["soulseek", "tidal"])
        self.assertEqual(order.parse_priority("tidal"), ["tidal", "soulseek"])
        self.assertEqual(order.parse_priority("qobuz,nope,TIDAL,tidal"), ["tidal", "soulseek"])

    def test_toggles_and_next_after(self):
        _set(self.db, "source_priority", "tidal,soulseek")
        self.assertEqual(order.enabled_order(self.db), ["tidal", "soulseek"])
        self.assertEqual(order.next_after(self.db, "tidal"), "soulseek")
        self.assertIsNone(order.next_after(self.db, "soulseek"))
        _set(self.db, "source_tidal_enabled", "0")
        self.assertEqual(order.enabled_order(self.db), ["soulseek"])
        self.assertFalse(order.tidal_allowed(self.db))
        self.assertIsNone(order.next_after(self.db, "soulseek"))
        _set(self.db, "soulseek_fallback_enabled", "0")
        self.assertEqual(order.enabled_order(self.db), [])

    def test_registry_enabled_sources_follow_configured_order(self):
        with mock.patch.object(pp, "_TIDDL_AVAILABLE", True):
            self.assertEqual([s.name for s in registry.enabled_acquire_sources(self.db)],
                             ["soulseek", "tidal"])
            _set(self.db, "source_priority", "tidal,soulseek")
            self.assertEqual([s.name for s in registry.enabled_acquire_sources(self.db)],
                             ["tidal", "soulseek"])


# --------------------------------------------------------------------------- #
class TestSoulseekFirst(_Base):
    def test_new_track_goes_to_soulseek_and_tidal_is_not_searched(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        self.tidal_search.assert_not_called()
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")      # the queue's holding stage
        self.assertEqual(r["match_status"], "pending")      # not a failed match
        fa = _fa(self.db, tid)
        self.assertEqual([f["status"] for f in fa], ["queued"])
        self.assertTrue(fa[0]["search_query"].startswith(order.SOULSEEK_FIRST_REASON))

    def test_soulseek_miss_falls_through_to_tidal(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        client = _NoCandidatesClient()
        self.run_soulseek(client)
        self.assertTrue(client.searches)
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "matching")
        self.assertEqual(_fa(self.db, tid)[0]["status"], "no_candidates")
        acts = _q(self.db, "SELECT event_type FROM activity_log WHERE track_id = ?", (tid,))
        self.assertIn("source_fallthrough", [a["event_type"] for a in acts])

        # Next matching pass: Soulseek has been tried, so Tidal is asked — and matches.
        self.tidal_search.side_effect = _tidal_hit
        pp._process_matching(self.db)
        self.tidal_search.assert_called()
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "downloading")
        self.assertEqual(r["tidal_id"], "777")

    def test_tidal_miss_after_soulseek_miss_ends_at_error_without_looping(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        self.run_soulseek(_NoCandidatesClient())
        self.tidal_search.side_effect = lambda q: []
        pp._process_matching(self.db)
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")
        self.assertIn("No Tidal match", r["pipeline_error"])
        # Soulseek was NOT re-queued (one attempt per source).
        self.assertEqual(len(_fa(self.db, tid)), 1)

    def test_soulseek_hit_never_touches_tidal(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        src = os.path.join(os.path.dirname(self.db), "src.flac")
        with open(src, "wb") as f:
            f.write(b"fLaC" + b"\0" * 1000)
        lib = os.path.join(os.path.dirname(self.db), "music")
        with mock.patch.object(sf, "verify_lossless",
                               return_value={"passed": True, "reasons": [], "checks": {}}), \
                mock.patch.object(sf, "MUSIC_LIBRARY_PATH", lib), \
                mock.patch.object(sf.os, "chown"):
            self.run_soulseek(_OneFlacClient(src))
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "verifying")
        self.assertEqual(r["download_source"], "soulseek")
        self.assertTrue(os.path.exists(r["file_path"]))
        self.tidal_search.assert_not_called()
        self.tidal_dl.assert_not_called()

    def test_failed_lossless_gate_is_a_miss_not_an_import(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        src = os.path.join(os.path.dirname(self.db), "src.flac")
        with open(src, "wb") as f:
            f.write(b"fake")
        with mock.patch.object(sf, "verify_lossless",
                               return_value={"passed": False, "reasons": ["transcode"],
                                             "checks": {}}):
            self.run_soulseek(_OneFlacClient(src))
        r = _row(self.db, tid)
        self.assertNotEqual(r["download_source"], "soulseek")
        self.assertIsNone(r["file_path"])
        self.assertEqual(r["pipeline_stage"], "matching")   # handed on to Tidal
        self.assertEqual(_fa(self.db, tid)[0]["status"], "all_failed")

    def test_unconfigured_slskd_falls_through(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        client = _NoCandidatesClient()
        client.configured = False
        self.run_soulseek(client)
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "matching")
        self.assertEqual(_fa(self.db, tid)[0]["status"], "unavailable")

    def test_logged_out_slskd_keeps_track_queued(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        client = _NoCandidatesClient()
        client.is_logged_in = lambda: False
        self.run_soulseek(client)
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "error")
        self.assertEqual(_fa(self.db, tid)[0]["status"], "queued")

    def test_tombstoned_isrc_is_held_for_review(self):
        # A DIFFERENT like, same ISRC, that the user deleted from Lexicon.
        gone = _track(self.db, stage="ignored", spotify_id="sp-deleted")
        _q(self.db, "INSERT INTO tombstones (track_id, isrc, reason) "
                    "VALUES (?, 'USUS10900001', 'deleted_in_lexicon')", (gone,))
        tid = _track(self.db)
        pp._process_matching(self.db)
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "needs_import_review")
        self.assertEqual(_fa(self.db, tid), [])


# --------------------------------------------------------------------------- #
class TestTidalFirst(_Base):
    def setUp(self):
        super().setUp()
        _set(self.db, "source_priority", "tidal,soulseek")

    def test_tidal_searched_first(self):
        self.tidal_search.side_effect = _tidal_hit
        tid = _track(self.db)
        pp._process_matching(self.db)
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "downloading")
        self.assertEqual(_fa(self.db, tid), [])

    def test_tidal_miss_goes_to_soulseek_and_soulseek_miss_does_not_loop(self):
        self.tidal_search.side_effect = lambda q: []
        tid = _track(self.db)
        pp._process_matching(self.db)
        fa = _fa(self.db, tid)
        self.assertEqual(fa[0]["status"], "queued")
        self.assertFalse(fa[0]["search_query"].startswith(order.SOULSEEK_FIRST_REASON))
        self.run_soulseek(_NoCandidatesClient())
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")      # no fall-back to Tidal again

    def test_tiddl_giving_up_routes_to_soulseek(self):
        self.tidal_search.side_effect = _tidal_hit
        tid = _track(self.db)
        pp._process_matching(self.db)
        _q(self.db, "UPDATE tracks SET download_attempts = 4 WHERE id = ?", (tid,))
        _age(self.db)
        self.tidal_auth.side_effect = None
        self.tidal_dl.side_effect = RuntimeError("tiddl stalled")
        with mock.patch.object(pp, "_TIDDL_AVAILABLE", True), \
                mock.patch.object(pp, "_cleanup_stale_downloads"), \
                mock.patch.object(pp, "_find_downloaded_file_broad", return_value=None):
            pp._process_downloading(self.db)
        self.assertEqual(_fa(self.db, tid)[0]["status"], "queued")


# --------------------------------------------------------------------------- #
class TestTidalDisabled(_Base):
    def setUp(self):
        super().setUp()
        _set(self.db, "source_tidal_enabled", "0")
        _set(self.db, "source_priority", "tidal,soulseek")   # order must not matter

    def test_no_tidal_call_and_soulseek_miss_ends_at_error(self):
        tid = _track(self.db)
        pp._process_matching(self.db)
        self.assertEqual(_fa(self.db, tid)[0]["status"], "queued")
        self.run_soulseek(_NoCandidatesClient())
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")
        self.assertIn("Soulseek", r["pipeline_error"])
        pp._process_matching(self.db)                         # nothing left to try
        with mock.patch.object(pp, "_TIDDL_AVAILABLE", True):
            pp._process_downloading(self.db)
        self.tidal_search.assert_not_called()
        self.tidal_dl.assert_not_called()
        self.tidal_auth.assert_not_called()

    def test_track_waiting_on_tidal_download_is_redispatched(self):
        tid = _track(self.db, stage="downloading", tidal_id="777", download_status="pending")
        with mock.patch.object(pp, "_TIDDL_AVAILABLE", True):
            pp._process_downloading(self.db)
        self.tidal_auth.assert_not_called()
        self.tidal_dl.assert_not_called()
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "matching")
        _age(self.db)
        pp._process_matching(self.db)
        self.assertEqual(_fa(self.db, tid)[0]["status"], "queued")

    def test_match_track_refuses_tidal_directly(self):
        tid = _track(self.db)
        pp._match_track(self.db, _row(self.db, tid))
        self.tidal_search.assert_not_called()

    def test_retry_unmatched_track_does_not_reach_tidal(self):
        tid = _track(self.db)
        _q(self.db, "INSERT INTO fallback_attempts (track_id, source, status) "
                    "VALUES (?, 'soulseek', 'no_candidates')", (tid,))
        pp._process_matching(self.db)
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")
        self.assertIn("Not found on any enabled source", r["pipeline_error"])
        self.tidal_search.assert_not_called()

    def test_metadata_fallback_skips_tidal(self):
        from tasks import metadata_fallback as mf
        tid = _track(self.db)
        with mock.patch("tasks.sources.tidal.search_raw",
                        side_effect=AssertionError("Tidal searched")):
            out = mf._recover(self.db, _row(self.db, tid),
                              {"isrcs": ["GBXXX0000001"], "title": "Zzz", "artist": "Nobody"})
        self.assertIsNone(out)

    def test_lossless_upgrade_skips_tidal(self):
        from tasks import lossless_upgrade as lu
        with mock.patch.object(lu, "_source_via_tidal",
                               side_effect=AssertionError("Tidal used")), \
                mock.patch.object(lu, "_source_via_soulseek", return_value="/x.flac"):
            self.assertEqual(lu._source_verified_lossless(self.db, {"id": 1}),
                             ("/x.flac", "soulseek"))


class TestNothingEnabled(_Base):
    def test_clear_error_not_a_hang(self):
        _set(self.db, "source_tidal_enabled", "0")
        _set(self.db, "soulseek_fallback_enabled", "0")
        tid = _track(self.db)
        pp._process_matching(self.db)
        r = _row(self.db, tid)
        self.assertEqual(r["pipeline_stage"], "error")
        self.assertIn("No acquire source is enabled", r["pipeline_error"])


class TestUpgradeOrder(_Base):
    def test_lossless_upgrade_follows_order(self):
        from tasks import lossless_upgrade as lu
        calls = []

        def via(name):
            def f(db, t):
                calls.append(name)
                return None
            return f
        with mock.patch.object(lu, "_source_via_tidal", via("tidal")), \
                mock.patch.object(lu, "_source_via_soulseek", via("soulseek")):
            lu._source_verified_lossless(self.db, {"id": 1})
            self.assertEqual(calls, ["soulseek", "tidal"])
            calls.clear()
            _set(self.db, "source_priority", "tidal,soulseek")
            lu._source_verified_lossless(self.db, {"id": 1})
            self.assertEqual(calls, ["tidal", "soulseek"])


class TestHunterLeavesQueuedTracksAlone(_Base):
    def test_queued_for_soulseek_is_not_wanted(self):
        from tasks import hunter
        tid = _track(self.db)
        pp._process_matching(self.db)                         # queued, parked at error
        self.assertEqual(hunter.enqueue_from_failures(self.db), 0)
        self.run_soulseek(_NoCandidatesClient())             # miss -> back to matching
        _set(self.db, "source_tidal_enabled", "0")
        _age(self.db)
        pp._process_matching(self.db)                         # nothing left -> error
        self.assertEqual(_row(self.db, tid)["pipeline_stage"], "error")
        self.assertEqual(hunter.enqueue_from_failures(self.db), 1)


class TestApiMirror(unittest.TestCase):
    """sync-api cannot import worker code, so it mirrors the source list in
    sync-api/sources_config.py. They must agree."""

    def test_api_mirror_matches_registry(self):
        spec = importlib.util.spec_from_file_location(
            "api_sources_config", os.path.join(SYNC_API_DIR, "sources_config.py"))
        api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api)
        self.assertEqual(tuple(api.DEFAULT_PRIORITY), order.DEFAULT_PRIORITY)
        self.assertEqual(set(api.ACQUIRE_SOURCES), {s.name for s in registry.acquire_sources()})
        self.assertEqual(set(api.LINK_SOURCES), {s.name for s in registry.link_sources()})
        self.assertEqual(api.TOGGLE_KEYS, {s.name: s.toggle_key for s in registry.all_sources()})
        self.assertEqual(api.PRIORITY_KEY, order.CONFIG_KEY)


if __name__ == "__main__":
    unittest.main()
