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
