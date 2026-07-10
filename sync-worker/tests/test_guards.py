"""Tests for the Lexicon duplicate-safety guards.

Covers the three guards added to make WaxFlow's Lexicon sync duplicate-safe:

  Guard 1: `scan` mode is genuinely read-only — `_process_organizing` early-returns
           in scan mode and never touches Lexicon.
  Guard 2: the worker no longer silently auto-escalates `sync_mode` from scan to
           full (regression check on worker.py source).
  Guard 3: `_lexicon_find_or_import` refuses to create a new Lexicon track
           (POST /v1/tracks) unless the track has a HARD, ISRC-confirmed match;
           fuzzy-only matches raise ImportNeedsReview instead.

Run inside the worker image (deps: httpx):  python -m pytest sync-worker/tests
or:  python -m unittest discover -s sync-worker/tests
"""

import os
import sys
import time
import unittest
from unittest import mock

# Make `tasks` importable when tests are run from the repo root.
SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import process_pipeline as pp  # noqa: E402


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _FakeLexiconClient:
    """Stand-in for httpx.Client. Search returns no matches (simulating the
    fuzzy-miss that would otherwise trigger a duplicate import); POST is recorded."""

    def __init__(self, search_tracks=None):
        self._search_tracks = search_tracks or []
        self.post_calls = []

    def get(self, path, params=None):
        return _FakeResponse(200, {"data": {"tracks": self._search_tracks}})

    def post(self, path, json=None):
        self.post_calls.append({"path": path, "json": json})
        return _FakeResponse(201, {"data": {"tracks": [{"id": 999}]}})


class TestGuard3ImportGate(unittest.TestCase):
    """Guard 3: gate POST /v1/tracks on a hard ISRC match."""

    def test_fuzzy_match_source_is_routed_to_review_not_imported(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {
            "title": "Some Song", "artist": "Some Artist",
            "isrc": "US1234567890", "match_source": "search",  # fuzzy Tidal text match
        }
        with self.assertRaises(pp.ImportNeedsReview):
            pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(client.post_calls, [], "must NOT POST /v1/tracks for a fuzzy match")

    def test_no_isrc_is_routed_to_review(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "isrc": "", "match_source": "library_existing"}
        with self.assertRaises(pp.ImportNeedsReview):
            pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(client.post_calls, [])

    def test_isrc_match_source_is_imported(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "isrc": "US1234567890", "match_source": "isrc"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")
        self.assertEqual(len(client.post_calls), 1, "hard ISRC match should import exactly once")

    def test_file_index_isrc_match_source_is_imported(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "isrc": "US1234567890", "match_source": "file_index_isrc"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")

    def test_manual_import_approval_bypasses_gate(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "isrc": "", "match_source": "manual_import_approved"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")

    def test_known_lexicon_id_short_circuits_without_search_or_import(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "match_source": "search", "lexicon_track_id": "4242"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "4242")
        self.assertEqual(client.post_calls, [], "known id must not trigger an import")

    def test_hard_sources_frozenset_contents(self):
        self.assertEqual(
            pp._HARD_IMPORT_MATCH_SOURCES,
            frozenset({"isrc", "file_index_isrc", "manual_import_approved"}),
        )


class _FakeDBCtx:
    """Context-manager stand-in for get_db(...).__enter__ returning a conn."""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        return None


class _FakeClientCtx:
    def __init__(self, client):
        self._client = client

    def __enter__(self):
        return self._client

    def __exit__(self, *a):
        return False


class TestOrganizeAlreadyInLexicon(unittest.TestCase):
    """Regression: tracks already present in Lexicon (match_source='lexicon_existing'
    with a real lexicon_track_id) carry a host-side file_path the worker container
    cannot see (e.g. /Volumes/Macintosh HD/...). Organizing them must NOT raise
    FileNotFoundError — they only need to be filed into the monthly playlist using
    the existing lexicon_track_id. This was the cause of ~331 stuck 'error' tracks."""

    def _run_organize(self, track):
        captured = {}

        def fake_update_track(db_path, tid, **kw):
            captured.update(kw)

        def fake_get_config(db_path, key):
            return {
                "lexicon_library_path": "/music/library",
                "lexicon_input_path": "/music/downloads",
                "lexicon_api_url": "http://lexicon.test",
                "downloads_path": "/downloads",
                "auto_analyze_enabled": "0",
            }.get(key)

        fake_client = mock.MagicMock()
        with mock.patch.object(pp, "update_track", side_effect=fake_update_track), \
             mock.patch.object(pp, "get_config", side_effect=fake_get_config), \
             mock.patch.object(pp, "_ensure_playlist", return_value={
                 "id": 1, "lexicon_folder_id": 10, "lexicon_playlist_id": 20}), \
             mock.patch.object(pp, "httpx") as fake_httpx, \
             mock.patch.object(pp, "_lexicon_find_or_import") as find_import, \
             mock.patch.object(pp, "_lexicon_track_in_playlist", return_value=False), \
             mock.patch.object(pp, "_lexicon_add_to_playlist"), \
             mock.patch.object(pp, "_lexicon_tag_track"), \
             mock.patch.object(pp, "get_db", return_value=_FakeDBCtx()), \
             mock.patch.object(pp, "log_activity"), \
             mock.patch.object(pp, "_notify_sync_complete"):
            fake_httpx.Client.return_value = _FakeClientCtx(fake_client)
            pp._organize_track("/tmp/x.db", track)
            return captured, find_import

    def test_already_in_lexicon_missing_file_does_not_raise_and_syncs(self):
        track = {
            "id": 1, "spotify_id": "sp1",
            "file_path": "/Volumes/Macintosh HD/Users/willcurran/Music/nope.flac",
            "spotify_added_at": "2026-04-15T00:00:00Z",
            "artist": "A", "title": "T",
            "match_source": "lexicon_existing", "lexicon_track_id": "2315",
        }
        captured, find_import = self._run_organize(track)
        self.assertEqual(captured.get("lexicon_status"), "synced")
        self.assertEqual(captured.get("pipeline_stage"), "complete")
        self.assertEqual(str(captured.get("lexicon_track_id")), "2315")
        find_import.assert_not_called()  # must not re-search/import an existing track

    def test_downloaded_track_with_missing_file_still_raises(self):
        track = {
            "id": 2, "spotify_id": "sp2",
            "file_path": "/downloads/gone.flac",
            "spotify_added_at": "2026-04-15T00:00:00Z",
            "artist": "A", "title": "T",
            "match_source": "isrc", "lexicon_track_id": None,
        }
        with self.assertRaises(FileNotFoundError):
            self._run_organize(track)


class TestTidalTokenRefreshBypassesTiddlModel(unittest.TestCase):
    """Regression: Tidal's token-refresh response no longer includes
    user.facebookUid, which bundled tiddl marks as a required pydantic field, so
    tiddl's own refresh raises ValidationError and never writes the new token.
    _refresh_tidal_token must parse the response directly and succeed anyway."""

    def test_refresh_parses_response_without_facebookuid(self):
        payload = {
            "access_token": "NEW_ACCESS_TOKEN",
            "expires_in": 14400,
            "refresh_token": "SAME_REFRESH",
            "user": {  # note: NO facebookUid — the field Tidal dropped
                "userId": 173461359, "countryCode": "US",
                "email": "someone@example.com", "acceptedEULA": True,
            },
        }

        class _R:
            status_code = 200
            text = "ok"

            def json(self):
                return payload

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def post(self, *a, **k):
                return _R()

        with mock.patch.object(pp.httpx, "Client", return_value=_C()):
            out = pp._refresh_tidal_token("SAME_REFRESH")
        self.assertIsNotNone(out)
        self.assertEqual(out["access_token"], "NEW_ACCESS_TOKEN")
        self.assertEqual(out["expires_in"], 14400)
        self.assertEqual(out["user_id"], "173461359")
        self.assertEqual(out["country_code"], "US")

    def test_refresh_returns_none_on_http_error(self):
        class _R:
            status_code = 401
            text = "unauthorized"

            def json(self):
                return {}

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def post(self, *a, **k):
                return _R()

        with mock.patch.object(pp.httpx, "Client", return_value=_C()):
            self.assertIsNone(pp._refresh_tidal_token("bad"))


class TestWorkerHeartbeat(unittest.TestCase):
    """Per-stage liveness heartbeat: written next to the DB, readable as an epoch
    timestamp. Prevents a busy worker (long cycle during backlog) being misread as
    'stalled' and needlessly restarted by the self-heal monitor."""

    def test_touch_and_read_heartbeat(self):
        import tempfile
        d = tempfile.mkdtemp()
        db = os.path.join(d, "sync.db")
        pp.touch_worker_heartbeat(db)
        hb = pp._worker_heartbeat_path(db)
        self.assertTrue(os.path.exists(hb))
        val = float(open(hb).read().strip())
        self.assertLessEqual(abs(time.time() - val), 5)


class TestGuard1ScanReadOnly(unittest.TestCase):
    """Guard 1: _process_organizing is a no-op in scan mode."""

    def test_scan_mode_does_not_read_or_write(self):
        with mock.patch.object(pp, "get_config", return_value="scan"), \
             mock.patch.object(pp, "get_tracks_by_stage") as gts, \
             mock.patch.object(pp, "_organize_track") as org:
            pp._process_organizing("/tmp/does-not-matter.db")
            gts.assert_not_called()
            org.assert_not_called()

    def test_full_mode_processes_tracks(self):
        with mock.patch.object(pp, "get_config", return_value="full"), \
             mock.patch.object(pp, "get_tracks_by_stage", return_value=[]) as gts:
            pp._process_organizing("/tmp/does-not-matter.db")
            gts.assert_called_once()


class TestGuard2NoAutoEscalation(unittest.TestCase):
    """Guard 2: worker.py must not silently flip sync_mode to 'full'."""

    def test_worker_source_has_no_auto_flip(self):
        worker_path = os.path.join(SYNC_WORKER_DIR, "worker.py")
        with open(worker_path, encoding="utf-8") as f:
            src = f.read()
        self.assertNotIn("'sync_mode', 'full'", src)
        self.assertNotIn('"sync_mode", "full"', src)
        self.assertIn("auto-escalation disabled", src)


if __name__ == "__main__":
    unittest.main()
