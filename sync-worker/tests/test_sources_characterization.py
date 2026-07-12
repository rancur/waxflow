"""CHARACTERIZATION tests — prove the Phase A source-plugin refactor is
behavior-identical to the pre-refactor inline pipeline code.

The whole point of Phase A is ZERO regression. These tests pin the observable
behavior of the source adapters against the exact inline implementations they wrap:

  * Tidal search  — _tidal_source.search_raw(q) is byte-identical to
                    process_pipeline._tidal_search(q): same return value AND the
                    same HTTP requests (url / params / headers).
  * Tidal acquire — _tidal_source.acquire_raw(db, track) is byte-identical to
                    process_pipeline._download_track_via_tiddl(db, track): same
                    subprocess argv AND the same destination path.
  * Soulseek      — the sources.soulseek facade re-exports the SAME function
                    objects as tasks.soulseek_fallback (identity), and
                    run_fallback forwards to process_soulseek_fallback verbatim.
  * Seam checks   — the adapters forward their arguments unchanged and return the
                    callee's value unchanged (spy tests).

No network / no real subprocess / no real filesystem writes.
"""

import os
import sys
import unittest
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import process_pipeline as pp  # noqa: E402
from tasks import soulseek_fallback as sf  # noqa: E402
from tasks.sources import soulseek as soulseek_src  # noqa: E402
from tasks.sources import tidal as tidal_src  # noqa: E402


# --------------------------------------------------------------------------- Tidal search
class _RecordingResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _RecordingClient:
    """Stand-in for httpx.Client that records every .get(...) call."""

    def __init__(self, calls, items):
        self._calls = calls
        self._items = items

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, params=None, headers=None):
        self._calls.append({"url": url, "params": params, "headers": headers})
        return _RecordingResponse(200, {"items": self._items})


def _run_search(fn, query, items):
    """Run a search fn under a fresh recording httpx client; return (result, calls)."""
    calls: list[dict] = []
    with mock.patch.object(pp.httpx, "Client", return_value=_RecordingClient(calls, items)):
        result = fn(query)
    return result, calls


class TestTidalSearchCharacterization(unittest.TestCase):
    ITEMS = [
        {"id": 111, "title": "Labyrinth", "isrc": "GB0001", "duration": 300},
        {"id": 222, "title": "Shai Hulud", "isrc": "GB0002", "duration": 250},
    ]

    def test_search_raw_identical_return_and_requests(self):
        for query in ("GB0001", "Mob Tactics Labyrinth", "  ", "weird & chars +"):
            inline_res, inline_calls = _run_search(pp._tidal_search, query, self.ITEMS)
            src_res, src_calls = _run_search(tidal_src.search_raw, query, self.ITEMS)
            self.assertEqual(src_res, inline_res, f"return differs for {query!r}")
            self.assertEqual(src_calls, inline_calls, f"HTTP requests differ for {query!r}")

    def test_search_raw_returns_items(self):
        res, calls = _run_search(tidal_src.search_raw, "GB0001", self.ITEMS)
        self.assertEqual(res, self.ITEMS)
        # first request carries the public tidal token header (unchanged behavior)
        self.assertEqual(calls[0]["headers"], {"x-tidal-token": "CzET4vdadNUFQ5JU"})
        self.assertEqual(calls[0]["params"]["query"], "GB0001")

    def test_search_raw_forwards_to_inline(self):
        sentinel = [{"id": 1}]
        with mock.patch.object(pp, "_tidal_search", return_value=sentinel) as spy:
            out = tidal_src.search_raw("q")
        spy.assert_called_once_with("q")
        self.assertIs(out, sentinel)


# ------------------------------------------------------------------------- Tidal acquire
class _FakeProc:
    returncode = 0
    stderr = ""
    stdout = ""


def _run_download(fn, db_path, track):
    """Run a download fn with all external boundaries mocked; return (dest, argv)."""
    captured = {}

    def fake_run(argv, *a, **k):
        captured["argv"] = argv
        return _FakeProc()

    def fake_walk(path):
        return [(path, [], ["track.flac"])]

    def fake_get_config(db, key):
        return {"tidal_download_quality": "max", "plex_uid": "1000", "plex_gid": "1000"}.get(key)

    with mock.patch.object(pp, "_tiddl_config_dir", "/tmp/tiddl-home-test", create=True), \
         mock.patch.object(pp.subprocess, "run", side_effect=fake_run), \
         mock.patch.object(pp.os, "makedirs"), \
         mock.patch.object(pp.os, "walk", side_effect=fake_walk), \
         mock.patch.object(pp.os, "remove"), \
         mock.patch.object(pp.os, "chown"), \
         mock.patch.object(pp.shutil, "copyfile"), \
         mock.patch.object(pp.shutil, "rmtree"), \
         mock.patch.object(pp, "get_config", side_effect=fake_get_config):
        dest = fn(db_path, track)
    return dest, captured.get("argv")


class TestTidalAcquireCharacterization(unittest.TestCase):
    TRACK = {"tidal_id": "987654", "artist": "Mob Tactics", "title": "Labyrinth"}

    def test_acquire_raw_identical_dest_and_argv(self):
        inline_dest, inline_argv = _run_download(pp._download_track_via_tiddl, "/db", dict(self.TRACK))
        src_dest, src_argv = _run_download(tidal_src.acquire_raw, "/db", dict(self.TRACK))
        self.assertEqual(src_dest, inline_dest)
        self.assertEqual(src_argv, inline_argv)
        # sanity: the argv is the tiddl download command for this track
        self.assertIn("tiddl", src_argv)
        self.assertIn("track/987654", src_argv)
        self.assertTrue(src_dest.endswith("Mob Tactics - Labyrinth.flac"))

    def test_acquire_raw_forwards_to_inline(self):
        sentinel = "/music/x.flac"
        with mock.patch.object(pp, "_download_track_via_tiddl", return_value=sentinel) as spy:
            out = tidal_src.acquire_raw("/db", self.TRACK)
        spy.assert_called_once_with("/db", self.TRACK)
        self.assertIs(out, sentinel)

    def test_typed_acquire_builds_track_and_forwards(self):
        from tasks.sources.base import SourceResult, TrackQuery
        q = TrackQuery(artist="Mob Tactics", title="Labyrinth")
        r = SourceResult(source="tidal", confidence=1.0, kind="acquire", external_id="987654")
        with mock.patch.object(pp, "_download_track_via_tiddl", return_value="/music/x.flac") as spy:
            out = tidal_src.TidalSource().acquire("/db", q, r)
        self.assertEqual(out, "/music/x.flac")
        spy.assert_called_once()
        args = spy.call_args[0]
        self.assertEqual(args[0], "/db")
        self.assertEqual(args[1]["tidal_id"], "987654")
        self.assertEqual(args[1]["artist"], "Mob Tactics")
        self.assertEqual(args[1]["title"], "Labyrinth")


# ----------------------------------------------------------------------------- Soulseek
class TestSoulseekCharacterization(unittest.TestCase):
    def test_facade_reexports_same_function_objects(self):
        # Identity == byte-identical: the facade did not copy or wrap the logic.
        self.assertIs(soulseek_src.process_soulseek_fallback, sf.process_soulseek_fallback)
        self.assertIs(soulseek_src.is_enabled, sf.is_enabled)
        self.assertIs(soulseek_src.already_attempted, sf.already_attempted)
        self.assertIs(soulseek_src.queue_for_fallback, sf.queue_for_fallback)
        self.assertIs(soulseek_src.reject_nonlossless_for_import, sf.reject_nonlossless_for_import)

    def test_run_fallback_forwards_verbatim(self):
        with mock.patch.object(soulseek_src, "process_soulseek_fallback", return_value=None) as spy:
            soulseek_src.run_fallback("/db")
        spy.assert_called_once_with("/db")

    def test_pipeline_imports_route_through_facade(self):
        # The pipeline's Soulseek surface must be the SAME objects as the facade
        # (proving the call-site swap did not change which code runs).
        self.assertIs(pp._soulseek_enabled, sf.is_enabled)
        self.assertIs(pp._soulseek_already_attempted, sf.already_attempted)
        self.assertIs(pp._soulseek_queue, sf.queue_for_fallback)
        self.assertIs(pp._reject_nonlossless_for_import, sf.reject_nonlossless_for_import)

    def test_source_run_fallback_forwards(self):
        with mock.patch.object(soulseek_src, "process_soulseek_fallback", return_value=None) as spy:
            soulseek_src.SoulseekSource().run_fallback("/db")
        spy.assert_called_once_with("/db")


if __name__ == "__main__":
    unittest.main()
