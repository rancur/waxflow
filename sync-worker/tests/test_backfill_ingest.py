"""Tests for the resumable, lock-resilient Spotify liked-songs backfill ingest.

Covers the fixes that let a full ~5.5k-track backfill actually complete while the
pipeline is concurrently writing to the shared sync.db:

  * _with_db_retry retries a write on transient "database is locked" (and does
    NOT retry unrelated OperationalErrors).
  * A backfill resumes from the persisted offset instead of restarting at 0.
  * A backfill that exits early on a Spotify API error leaves the one-shot flag
    SET (so it resumes) rather than dropping to incremental with a partial library.
  * A backfill that walks to the end clears the flag and the resume cursor.
"""

import os
import sqlite3
import sys
import unittest
from contextlib import contextmanager
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import poll_spotify as ps  # noqa: E402


class TestWithDbRetry(unittest.TestCase):
    def test_retries_on_database_locked_then_succeeds(self):
        calls = {"n": 0}

        @contextmanager
        def fake_get_db(_db):
            calls["n"] += 1
            if calls["n"] < 3:
                raise sqlite3.OperationalError("database is locked")
            yield object()  # a "conn" the func can use

        with mock.patch.object(ps, "get_db", fake_get_db), \
             mock.patch.object(ps.time, "sleep", lambda *_: None):
            result = ps._with_db_retry("db", lambda conn: 42)
        self.assertEqual(result, 42)
        self.assertEqual(calls["n"], 3, "should retry twice then succeed on the 3rd try")

    def test_does_not_retry_unrelated_operational_error(self):
        calls = {"n": 0}

        @contextmanager
        def fake_get_db(_db):
            calls["n"] += 1
            raise sqlite3.OperationalError("no such table: tracks")
            yield  # pragma: no cover

        with mock.patch.object(ps, "get_db", fake_get_db), \
             mock.patch.object(ps.time, "sleep", lambda *_: None):
            with self.assertRaises(sqlite3.OperationalError):
                ps._with_db_retry("db", lambda conn: 1)
        self.assertEqual(calls["n"], 1, "a non-lock error must not be retried")


class _FakeSpotify:
    """Records the offsets requested; returns empty pages (so the walk ends fast)
    unless configured to raise."""

    def __init__(self, total=5550, raise_on_call=False):
        self.total = total
        self.raise_on_call = raise_on_call
        self.offsets = []

    def current_user_saved_tracks(self, limit=50, offset=0):
        self.offsets.append(offset)
        if self.raise_on_call:
            raise RuntimeError("Spotify API 500")
        # Empty page -> _poll treats it as end-of-library (walk_complete).
        return {"items": [], "total": self.total}


def _run_poll_with_config(config: dict, sp: _FakeSpotify):
    """Run ps._poll with get_config/set_config backed by an in-memory dict and the
    Spotify client / activity logging stubbed out. Returns the config dict."""
    store = dict(config)

    def fake_get_config(_db, key):
        return store.get(key)

    def fake_set_config(_db, key, value):
        store[key] = value

    with mock.patch.object(ps, "get_spotify_client", lambda _db: sp), \
         mock.patch.object(ps, "get_config", fake_get_config), \
         mock.patch.object(ps, "set_config", fake_set_config), \
         mock.patch.object(ps, "log_activity", lambda *a, **k: None):
        ps._poll("db")
    return store


class TestBackfillResumeAndCompletion(unittest.TestCase):
    def test_resumes_from_persisted_offset(self):
        sp = _FakeSpotify()
        _run_poll_with_config(
            {"backfill_all_liked": "1", "backfill_offset": "1500"}, sp)
        self.assertEqual(sp.offsets[0], 1500,
                         "backfill must resume from the persisted offset, not 0")

    def test_completed_walk_clears_flag_and_cursor(self):
        sp = _FakeSpotify()
        store = _run_poll_with_config(
            {"backfill_all_liked": "1", "backfill_offset": "0"}, sp)
        self.assertEqual(store.get("backfill_all_liked"), "0",
                         "a completed backfill clears the one-shot flag")
        self.assertEqual(store.get("backfill_offset"), "0",
                         "a completed backfill resets the resume cursor")

    def test_api_error_preserves_flag_for_resume(self):
        sp = _FakeSpotify(raise_on_call=True)
        store = _run_poll_with_config(
            {"backfill_all_liked": "1", "backfill_offset": "800"}, sp)
        self.assertEqual(store.get("backfill_all_liked"), "1",
                         "an API error mid-backfill must leave the flag set to resume")

    def test_incremental_poll_starts_at_zero(self):
        sp = _FakeSpotify()
        _run_poll_with_config({"last_spotify_poll": "2026-01-01T00:00:00Z"}, sp)
        self.assertEqual(sp.offsets[0], 0, "incremental polls always start at offset 0")


if __name__ == "__main__":
    unittest.main()
