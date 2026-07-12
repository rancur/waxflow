"""Tests for the Lexicon import-health detection + proactive canary.

Covers the class of failure where Lexicon's host has lost the NAS music mount:
POST /v1/tracks returns HTTP 200 with an EMPTY tracks array — it imports nothing
but looks like success. WaxFlow must treat this as a distinct, LOUD failure, not
success and not a generic error.

  A) Empty-import detection: `_lexicon_find_or_import` raises `LexiconImportEmpty`
     (tagged with the machine-parseable reason) when a confidently-absent track's
     import returns 0 tracks, instead of silently returning None.
  B) Canary: `run_canary` re-probes an already-indexed mount file and classifies
     ok / mount_down / lexicon_unreachable / no_reference / skipped_scan, driving
     the shared health recorder (app_config + activity + webhook).

Run inside the worker image (deps: httpx): python -m pytest sync-worker/tests
"""

import os
import sys
import time
import unittest
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import process_pipeline as pp  # noqa: E402
from tasks import lexicon_health as lh  # noqa: E402


class _Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


# ---------------------------------------------------------------------------
# A) Empty-import detection in _lexicon_find_or_import
# ---------------------------------------------------------------------------

class _EmptyImportClient:
    """Search returns nothing (confidently absent) so the code path reaches the
    POST /v1/tracks import; POST returns HTTP 200 with an EMPTY tracks array —
    the exact silent-failure signature of a downed NAS mount."""

    def __init__(self, post_status=200, post_tracks=None):
        self._post_status = post_status
        self._post_tracks = post_tracks if post_tracks is not None else []
        self.post_calls = []

    def get(self, path, params=None):
        return _Resp(200, {"data": {"tracks": []}})  # confidently absent

    def post(self, path, json=None):
        self.post_calls.append({"path": path, "json": json})
        return _Resp(self._post_status, {"data": {"tracks": self._post_tracks}})


class TestEmptyImportDetection(unittest.TestCase):
    def test_empty_import_raises_distinct_error_not_none(self):
        client = _EmptyImportClient(post_status=200, post_tracks=[])
        track = {"title": "New Song", "artist": "Artist", "isrc": "US1", "match_source": "search"}
        with self.assertRaises(pp.LexiconImportEmpty) as ctx:
            pp._lexicon_find_or_import(client, "/Volumes/music/x.flac", track, db_path=None)
        # Must carry the machine-parseable reason token AND a human message.
        self.assertIn(pp.LEXICON_IMPORT_EMPTY_REASON, str(ctx.exception))
        self.assertIn("imported 0 tracks", str(ctx.exception))
        self.assertEqual(len(client.post_calls), 1, "import must have been attempted exactly once")

    def test_import_returning_track_without_id_is_also_empty_failure(self):
        # 200 with a track object that has no usable id can't be verified -> fail.
        client = _EmptyImportClient(post_status=200, post_tracks=[{"location": "x"}])
        track = {"title": "New Song", "artist": "Artist", "isrc": "US1", "match_source": "search"}
        with self.assertRaises(pp.LexiconImportEmpty):
            pp._lexicon_find_or_import(client, "/Volumes/music/x.flac", track, db_path=None)

    def test_successful_import_still_returns_id(self):
        # Regression: a real import (non-empty) must still succeed normally.
        client = _EmptyImportClient(post_status=201, post_tracks=[{"id": 4242}])
        track = {"title": "New Song", "artist": "Artist", "isrc": "US1", "match_source": "search"}
        result = pp._lexicon_find_or_import(client, "/Volumes/music/x.flac", track, db_path=None)
        self.assertEqual(result, "4242")


class TestContainerToMacPath(unittest.TestCase):
    """Delivery-path contract: the worker writes to container /music (== NAS
    /volume1/music); the Lexicon Mac reads that tree over SMB at /Volumes/music.
    A regression here is exactly the Apr/Jun bug (downloads never reached Lexicon),
    so pin the mapping."""

    def test_music_file_maps_to_smb_library_path(self):
        mac = pp._container_to_mac_path(
            "/music/AC Slater/AC Slater - Connect.m4a",
            "/Volumes/music", "/Volumes/music/Input", "/downloads",
        )
        self.assertEqual(mac, "/Volumes/music/AC Slater/AC Slater - Connect.m4a")

    def test_trailing_slash_on_library_path_is_normalized(self):
        mac = pp._container_to_mac_path(
            "/music/Artist/Song.flac",
            "/Volumes/music/", "/Volumes/music/Input", "/downloads",
        )
        self.assertEqual(mac, "/Volumes/music/Artist/Song.flac")

    def test_downloads_file_maps_to_input_path(self):
        mac = pp._container_to_mac_path(
            "/downloads/tracks/Artist/Song.flac",
            "/Volumes/music", "/Volumes/music/Input", "/downloads",
        )
        self.assertEqual(mac, "/Volumes/music/Input/tracks/Artist/Song.flac")

    def test_music_prefix_wins_when_not_under_downloads(self):
        # Files under /music must NEVER be mapped through the Input prefix.
        mac = pp._container_to_mac_path(
            "/music/Bonobo/Bonobo - Me and You.flac",
            "/Volumes/music", "/Volumes/music/Input", "/downloads",
        )
        self.assertTrue(mac.startswith("/Volumes/music/Bonobo/"))


class TestOrganizeRoutesEmptyImport(unittest.TestCase):
    """_process_organizing treats an empty import in TWO phases:
      1. TRANSIENT (within the grace window) — the SMB mount on the Lexicon host
         Mac is momentarily down/remounting, so the just-written file is not
         readable YET. Keep retrying in 'organizing' (status 'pending'); do NOT
         fire the loud error path.
      2. OUTAGE (grace exhausted) — escalate to the distinct error state with the
         reason token, a loud activity event, and a call into the shared
         import-health recorder."""

    def _run(self, track):
        captured = {}
        events = []

        def fake_update(db_path, tid, **kw):
            captured.update(kw)

        def fake_log_activity(db_path, ev, tid, msg, details=None):
            events.append(ev)

        with mock.patch.object(pp, "get_config", return_value="full"), \
             mock.patch.object(pp, "get_tracks_by_stage", return_value=[track]), \
             mock.patch.object(pp, "_organize_track", side_effect=pp.LexiconImportEmpty(
                 f"[{pp.LEXICON_IMPORT_EMPTY_REASON}] Lexicon imported 0 tracks")), \
             mock.patch.object(pp, "update_track", side_effect=fake_update), \
             mock.patch.object(pp, "log_activity", side_effect=fake_log_activity), \
             mock.patch("tasks.lexicon_health.note_empty_import") as note, \
             mock.patch.object(pp, "_trigger_lexicon_post_processing_batch"):
            pp._process_organizing("/tmp/x.db")
        return captured, events, note

    def test_first_empty_import_is_transient_retry_not_loud_error(self):
        # No prior marker -> first sight of an empty import -> treat as sync lag.
        track = {"id": 7, "artist": "A", "title": "T", "file_path": "/x.flac"}
        captured, events, note = self._run(track)
        self.assertEqual(captured.get("lexicon_status"), "pending")
        self.assertEqual(captured.get("pipeline_stage"), "organizing")
        # Persists the first-seen marker so the window survives restarts.
        self.assertIsNotNone(pp._parse_empty_since(captured.get("pipeline_error", "")))
        # NOT the loud path: no health note, no loud activity event yet.
        self.assertNotIn("lexicon_import_empty", events)
        note.assert_not_called()

    def test_empty_past_grace_sets_distinct_error_and_records_health(self):
        # An old marker (well beyond the default grace) -> genuine outage.
        old = int(time.time()) - (pp._DEFAULT_EMPTY_IMPORT_GRACE_SECONDS + 2000)
        track = {"id": 7, "artist": "A", "title": "T", "file_path": "/x.flac",
                 "pipeline_error": f"[empty_since:{old}] awaiting sync"}
        captured, events, note = self._run(track)
        self.assertEqual(captured.get("lexicon_status"), "error")
        self.assertEqual(captured.get("pipeline_stage"), "error")
        self.assertIn(pp.LEXICON_IMPORT_EMPTY_REASON, captured.get("pipeline_error", ""))
        self.assertIn("lexicon_import_empty", events)
        note.assert_called_once()


# ---------------------------------------------------------------------------
# B) Canary (watch-folder flow: staging-dir writability + Lexicon reachability)
# ---------------------------------------------------------------------------

class _CanaryHarness:
    """Patches config + the two check helpers + record_import_health for canary
    tests, so no real filesystem or network is touched."""

    def __init__(self, write_ok=True, lexicon_ok=True, config=None):
        self.write_ok = write_ok
        self.lexicon_ok = lexicon_ok
        self.config = {
            "lexicon_api_url": "http://lex.test",
            "lexicon_watch_dir": "/downloads",
            "sync_mode": "full",
        }
        if config:
            self.config.update(config)
        self.recorded = []

    def get_config(self, db_path, key):
        return self.config.get(key)

    def record_import_health(self, db_path, status, detail, *, ok, source, notify=True):
        self.recorded.append({"status": status, "ok": ok, "source": source})

    def run(self):
        with mock.patch.object(lh, "get_config", side_effect=self.get_config), \
             mock.patch.object(lh, "record_import_health", side_effect=self.record_import_health), \
             mock.patch.object(lh, "_check_watch_dir_writable",
                               return_value=(self.write_ok, "watch detail")), \
             mock.patch.object(lh, "_check_lexicon_reachable",
                               return_value=(self.lexicon_ok, "lexicon detail")):
            return lh.run_canary("/tmp/x.db")


class TestCanary(unittest.TestCase):
    def test_healthy_when_writable_and_reachable(self):
        out = _CanaryHarness(write_ok=True, lexicon_ok=True).run()
        self.assertEqual(out["status"], "ok")
        self.assertTrue(out["ok"])

    def test_watch_dir_unwritable_is_critical(self):
        # WaxFlow can't stage downloads for Synology Drive to sync -> imports fail.
        h = _CanaryHarness(write_ok=False, lexicon_ok=True)
        out = h.run()
        self.assertEqual(out["status"], "watch_dir_unwritable")
        self.assertFalse(out["ok"])
        self.assertIn("watch_dir_unwritable", lh.CRITICAL_STATUSES)
        self.assertEqual(h.recorded[-1]["ok"], False)

    def test_lexicon_unreachable_is_critical(self):
        h = _CanaryHarness(write_ok=True, lexicon_ok=False)
        out = h.run()
        self.assertEqual(out["status"], "lexicon_unreachable")
        self.assertFalse(out["ok"])
        self.assertEqual(h.recorded[-1]["ok"], False)

    def test_writability_check_is_evaluated_before_lexicon(self):
        # If the staging dir is unwritable, that is reported even when Lexicon is
        # also down (the more actionable/local failure wins).
        out = _CanaryHarness(write_ok=False, lexicon_ok=False).run()
        self.assertEqual(out["status"], "watch_dir_unwritable")


class TestWatchDirWritable(unittest.TestCase):
    """Real filesystem behaviour of the staging-dir write check."""

    def test_writable_dir_passes_and_leaves_no_file(self):
        import tempfile
        d = tempfile.mkdtemp()
        ok, detail = lh._check_watch_dir_writable(d)
        self.assertTrue(ok, detail)
        self.assertEqual(os.listdir(d), [], "canary must clean up its probe file")

    def test_missing_dir_fails(self):
        ok, _ = lh._check_watch_dir_writable("/no/such/dir/waxflow-canary")
        self.assertFalse(ok)


class TestRecordImportHealth(unittest.TestCase):
    """record_import_health persists the signal and pages only on transition."""

    def _harness(self, prev_status):
        store = {lh.STATUS_KEY: prev_status}

        def gc(db, k):
            return store.get(k)

        def sc(db, k, v):
            store[k] = v

        return store, gc, sc

    def test_critical_pages_on_transition_only(self):
        store, gc, sc = self._harness(prev_status="ok")
        posts = []
        with mock.patch.object(lh, "get_config", side_effect=gc), \
             mock.patch.object(lh, "set_config", side_effect=sc), \
             mock.patch.object(lh, "log_activity"), \
             mock.patch.object(lh, "_post_webhook", side_effect=lambda *a, **k: posts.append(a)):
            lh.record_import_health("/tmp/x.db", "mount_down", "down", ok=False, source="t")
            self.assertEqual(store[lh.MOUNT_OK_KEY], "0")
            self.assertEqual(store[lh.STATUS_KEY], "mount_down")
            self.assertEqual(len(posts), 1, "first critical transition pages once")
            # Second consecutive critical must NOT re-page.
            lh.record_import_health("/tmp/x.db", "mount_down", "still down", ok=False, source="t")
            self.assertEqual(len(posts), 1, "persistent outage does not re-page every cycle")

    def test_recovery_records_and_notifies_once(self):
        store, gc, sc = self._harness(prev_status="mount_down")
        posts = []
        with mock.patch.object(lh, "get_config", side_effect=gc), \
             mock.patch.object(lh, "set_config", side_effect=sc), \
             mock.patch.object(lh, "log_activity"), \
             mock.patch.object(lh, "_post_webhook", side_effect=lambda *a, **k: posts.append(a)):
            lh.record_import_health("/tmp/x.db", "ok", "back", ok=True, source="t")
            self.assertEqual(store[lh.MOUNT_OK_KEY], "1")
            self.assertEqual(store[lh.STATUS_KEY], "ok")
            self.assertEqual(len(posts), 1, "recovery notifies once")


if __name__ == "__main__":
    unittest.main()
