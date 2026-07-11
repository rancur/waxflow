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
    """Stand-in for httpx.Client. Every search returns the same candidate list
    (`search_tracks`); POST /v1/tracks is recorded and returns a new id. `status`
    controls the search HTTP status (use 500 to simulate an unavailable Lexicon)."""

    def __init__(self, search_tracks=None, status=200):
        self._search_tracks = search_tracks or []
        self._status = status
        self.post_calls = []

    def get(self, path, params=None):
        return _FakeResponse(self._status, {"data": {"tracks": self._search_tracks}})

    def post(self, path, json=None):
        self.post_calls.append({"path": path, "json": json})
        return _FakeResponse(201, {"data": {"tracks": [{"id": 999}]}})


class TestImportGuardLinkImportReview(unittest.TestCase):
    """Refined guard: at the Lexicon-import step a track either LINKs to an
    existing Lexicon track (never duplicating Will's large library), AUTO-IMPORTs
    when confidently absent, or is routed to review only when genuinely ambiguous.
    Lexicon exposes no ISRC, so the decision uses a robust title+artist existence
    check (_classify_lexicon_presence), not a hard ISRC gate."""

    def test_confidently_absent_new_track_is_imported(self):
        # Lexicon search returns nothing -> confidently new -> AUTO-IMPORT.
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "Brand New Song", "artist": "Some Artist",
                 "isrc": "US1234567890", "match_source": "search"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")
        self.assertEqual(len(client.post_calls), 1, "confidently-absent track must import exactly once")

    def test_no_isrc_but_absent_is_imported(self):
        # No ISRC no longer blocks import when the song is confidently absent.
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "New One", "artist": "Artist", "isrc": "", "match_source": "search"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")

    def test_existing_track_is_linked_not_imported(self):
        # Confident title+artist match in Lexicon -> LINK, no new track.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 314, "title": "My Song", "artist": "Cool Artist"}])
        track = {"title": "My Song", "artist": "Cool Artist",
                 "isrc": "US1", "match_source": "search"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "314")
        self.assertEqual(client.post_calls, [], "an existing Lexicon track must be LINKed, never imported")

    def test_ambiguous_same_artist_near_title_is_routed_to_review(self):
        # Same artist, near-identical title ("Runaway" vs "Run Away") that is NOT a
        # confident title match -> genuinely ambiguous -> review, do not import.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 77, "title": "Run Away", "artist": "Kanye West"}])
        track = {"title": "Runaway", "artist": "Kanye West",
                 "isrc": "US2", "match_source": "search"}
        with self.assertRaises(pp.ImportNeedsReview):
            pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(client.post_calls, [], "ambiguous near-collision must not import")

    def test_search_unavailable_is_routed_to_review(self):
        # Lexicon search errors (HTTP 500) -> cannot prove absence -> fail safe to
        # review rather than risk a duplicate.
        client = _FakeLexiconClient(search_tracks=[], status=500)
        track = {"title": "Whatever", "artist": "Artist", "isrc": "US3", "match_source": "search"}
        with self.assertRaises(pp.ImportNeedsReview):
            pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(client.post_calls, [], "inconclusive check must not import")

    def test_manual_approval_forces_import_even_when_ambiguous(self):
        # Explicit human approval overrides the ambiguous->review routing.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 77, "title": "Run Away", "artist": "Kanye West"}])
        track = {"title": "Runaway", "artist": "Kanye West",
                 "isrc": "", "match_source": "manual_import_approved"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "999")
        self.assertEqual(len(client.post_calls), 1)

    def test_known_lexicon_id_short_circuits_without_search_or_import(self):
        client = _FakeLexiconClient(search_tracks=[])
        track = {"title": "T", "artist": "A", "match_source": "search", "lexicon_track_id": "4242"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "4242")
        self.assertEqual(client.post_calls, [], "known id must not trigger an import")

    def test_exact_path_match_links_even_without_artist_match(self):
        # Lexicon already indexes this exact file -> LINK by path.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 55, "title": "X", "artist": "Y", "location": "/music/library/x.flac"}])
        track = {"title": "X", "artist": "Y", "isrc": "US4", "match_source": "search"}
        result = pp._lexicon_find_or_import(client, "/music/library/x.flac", track, db_path=None)
        self.assertEqual(result, "55")
        self.assertEqual(client.post_calls, [])

    def test_force_import_sources_is_manual_only(self):
        self.assertEqual(
            pp._FORCE_IMPORT_MATCH_SOURCES,
            frozenset({"manual_import_approved"}),
        )


class TestClassifyLexiconPresence(unittest.TestCase):
    """Direct coverage of the link/absent/ambiguous/unknown existence classifier."""

    def test_link_on_confident_title_and_artist(self):
        client = _FakeLexiconClient(search_tracks=[
            {"id": 10, "title": "Outgrown", "artist": "Bonobo"}])
        self.assertEqual(
            pp._classify_lexicon_presence(client, {"title": "Outgrown", "artist": "Bonobo"}),
            ("link", "10"))

    def test_absent_when_no_candidates(self):
        client = _FakeLexiconClient(search_tracks=[])
        self.assertEqual(
            pp._classify_lexicon_presence(client, {"title": "Nope", "artist": "Nobody"}),
            ("absent", None))

    def test_absent_when_same_title_different_artist(self):
        # Title collision with an unrelated artist is a different song -> import.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 3, "title": "Alright", "artist": "Different Person"}])
        self.assertEqual(
            pp._classify_lexicon_presence(client, {"title": "Alright", "artist": "Some Band"}),
            ("absent", None))

    def test_ambiguous_same_artist_near_title(self):
        client = _FakeLexiconClient(search_tracks=[
            {"id": 4, "title": "Run Away", "artist": "Kanye West"}])
        self.assertEqual(
            pp._classify_lexicon_presence(client, {"title": "Runaway", "artist": "Kanye West"}),
            ("ambiguous", None))

    def test_unknown_when_search_unavailable(self):
        client = _FakeLexiconClient(search_tracks=[], status=500)
        self.assertEqual(
            pp._classify_lexicon_presence(client, {"title": "T", "artist": "A"}),
            ("unknown", None))

    def test_drift_not_linked_to_drifting_same_artist(self):
        # Regression tie-in: "Drift" must NOT link to "Drifting" (word-boundary
        # matcher); with ratio 0.77 < 0.85 it is treated as absent -> importable.
        client = _FakeLexiconClient(search_tracks=[
            {"id": 9, "title": "Drifting", "artist": "Bonobo"}])
        decision, _ = pp._classify_lexicon_presence(client, {"title": "Drift", "artist": "Bonobo"})
        self.assertNotEqual(decision, "link")


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


class TestTitleMatchNoMidWordSubstring(unittest.TestCase):
    """Regression: `_titles_match` must not treat a shorter title as a match when
    it only appears as a MID-WORD fragment of the longer one. The 2026 Bonobo
    single "Drift" (ISRC GBCFB2600207) wrongly matched the Lazarus soundtrack track
    "Drifting" (ISRC USQX92501692) because raw substring containment made
    "drift" ⊂ "drifting" True. Lexicon exposes no ISRC, so title matching is the
    only place this can be caught."""

    def test_drift_does_not_match_drifting(self):
        self.assertFalse(pp._titles_match("Drift", "Drifting"))
        self.assertFalse(pp._titles_match("Drifting", "Drift"))

    def test_word_boundary_helper(self):
        self.assertFalse(pp._contains_at_word_boundary("drift", "drifting"))
        self.assertTrue(pp._contains_at_word_boundary("drift", "tokyo drift"))
        self.assertTrue(pp._contains_at_word_boundary("me and you", "me and you reprise"))

    def test_single_shared_word_is_not_enough(self):
        # "Drift" vs "Tokyo Drift" share one whole word but are different songs;
        # the word-overlap strategy must not fully satisfy on a single shared word.
        self.assertFalse(pp._titles_match("Drift", "Tokyo Drift"))

    def test_legit_matches_still_pass(self):
        # Exact, suffix-variant, and whole-word-contained titles must still match.
        self.assertTrue(pp._titles_match("Drift", "Drift"))
        self.assertTrue(pp._titles_match("Drift", "Drift (Original Mix)"))
        self.assertTrue(pp._titles_match("Sun Will Rise", "Sun Will Rise"))
        self.assertTrue(pp._titles_match("Outgrown", "Outgrown"))
        self.assertTrue(pp._titles_match("Hey Now - Bonobo Remix", "Hey Now (Bonobo Remix)"))


class _FileIndexConn:
    """Fake DB conn for _check_existing_by_isrc: reports file_index exists, no ISRC
    hit, and returns the given rows for the fuzzy title+artist LIKE query."""

    def __init__(self, like_rows):
        self._like_rows = like_rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        rows = []
        if "sqlite_master" in sql:
            rows = [("file_index",)]
        elif "WHERE isrc" in sql:
            rows = []  # no exact ISRC match in file_index
        elif "title LIKE" in sql:
            rows = self._like_rows

        class _Cur:
            def fetchone(_s):
                return rows[0] if rows else None

            def fetchall(_s):
                return rows

        return _Cur()


class TestFileIndexFuzzyValidation(unittest.TestCase):
    """The file_index fuzzy path uses a coarse SQL LIKE prefilter ('Drift%' also
    matches 'Drifting'); each candidate must be confirmed with _titles_match /
    _artists_match. This is the path that actually re-mis-matched the Bonobo single
    'Drift' to the file 'Bonobo - Drifting' (match_source=file_index_title_artist)."""

    def test_drift_does_not_match_drifting_file(self):
        conn = _FileIndexConn(like_rows=[(
            "/music/Database/Bonobo/Lazarus/Bonobo - Drifting 5M59.flac",
            "Drifting", "Bonobo")])
        with mock.patch.object(pp, "get_db", return_value=conn):
            result = pp._check_existing_by_isrc("/tmp/x.db", {
                "title": "Drift", "artist": "Bonobo", "isrc": "GBCFB2600207"})
        self.assertIsNone(result, "'Drift' must not match the 'Drifting' file")

    def test_real_drift_file_still_matches(self):
        conn = _FileIndexConn(like_rows=[(
            "/music/Database/Bonobo/Bonobo - Drift.flac", "Drift", "Bonobo")])
        with mock.patch.object(pp, "get_db", return_value=conn):
            result = pp._check_existing_by_isrc("/tmp/x.db", {
                "title": "Drift", "artist": "Bonobo", "isrc": "GBCFB2600207"})
        self.assertIsNotNone(result)
        self.assertEqual(result["match_type"], "title_artist")


class _RecordingConn:
    """Fake DB connection recording executed SQL; UPDATE returns a rowcount."""

    def __init__(self, rowcount=0):
        self._rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.executed.append(sql)

        class _R:
            rowcount = self._rowcount
        return _R()


class TestWaitingRevival(unittest.TestCase):
    """Issue: April 2026 + June 2026 monthly playlists were never created because
    every one of those months' tracks was stranded at pipeline_stage='waiting'.
    In full/download mode `_process_matching` must revive 'waiting' tracks back to
    'matching'; in scan mode it must NOT (scan mode legitimately parks them)."""

    def test_full_mode_revives_waiting_tracks(self):
        conn = _RecordingConn(rowcount=26)
        with mock.patch.object(pp, "get_config", return_value="full"), \
             mock.patch.object(pp, "get_db", return_value=conn), \
             mock.patch.object(pp, "get_tracks_by_stage", return_value=[]), \
             mock.patch.object(pp, "log_activity"):
            pp._process_matching("/tmp/x.db")
        revive_stmts = [s for s in conn.executed
                        if "UPDATE tracks" in s and "pipeline_stage = 'matching'" in s
                        and "pipeline_stage = 'waiting'" in s]
        self.assertEqual(len(revive_stmts), 1, "full mode must revive waiting->matching exactly once")

    def test_scan_mode_does_not_revive(self):
        conn = _RecordingConn(rowcount=0)
        with mock.patch.object(pp, "get_config", return_value="scan"), \
             mock.patch.object(pp, "get_db", return_value=conn), \
             mock.patch.object(pp, "get_tracks_by_stage", return_value=[]), \
             mock.patch.object(pp, "log_activity"):
            pp._process_matching("/tmp/x.db")
        revive_stmts = [s for s in conn.executed if "pipeline_stage = 'waiting'" in s]
        self.assertEqual(revive_stmts, [], "scan mode must NOT revive waiting tracks")


class EmptyImportSyncLagMarkerTests(unittest.TestCase):
    """The direct-import fix POSTs the Mac-local path, which imports 0 tracks while
    the freshly-downloaded file is still replicating NAS -> Mac via Synology Drive.
    That transient empty import must be tolerated (retried) for a grace window
    before it is escalated to the loud mount-down error. The first-seen timestamp
    is persisted in pipeline_error via a parseable '[empty_since:<ts>]' marker so
    the window survives worker restarts."""

    def test_parse_empty_since_roundtrip(self):
        ts = 1783805169
        err = f"[empty_since:{ts}] Lexicon import empty — awaiting Synology Drive sync"
        self.assertEqual(pp._parse_empty_since(err), float(ts))

    def test_parse_empty_since_absent(self):
        self.assertIsNone(pp._parse_empty_since(None))
        self.assertIsNone(pp._parse_empty_since(""))
        self.assertIsNone(pp._parse_empty_since("some unrelated error"))

    def test_default_grace_window_is_generous(self):
        # Must comfortably exceed real Synology Drive NAS->Mac sync lag so a normal
        # sync delay never false-alarms as a mount outage.
        self.assertGreaterEqual(pp._DEFAULT_EMPTY_IMPORT_GRACE_SECONDS, 600)


if __name__ == "__main__":
    unittest.main()
