"""Tests for Phase 3 real-time flow-on-like: tighter poll + cheap change detection.

Spotify has NO push/webhook for saved/liked tracks, so "real-time" == a short-
interval poll with cheap change-detection. These tests prove:
  * an INCREMENTAL poll uses the small configurable page size (not the 50-wide
    backfill page), so a tight poll is cheap,
  * when nothing is newer than last_poll the poll costs a SINGLE tiny API call
    (newest-first + break-at-cutoff), i.e. change-detection short-circuits,
  * a FULL backfill still uses the big page,
  * the 429 rate-limit path honours Retry-After and retries (poll safety).

Pure/offline: the Spotify client is a fake recording call args.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from spotipy.exceptions import SpotifyException  # noqa: E402

from tasks import poll_spotify  # noqa: E402
from tasks.helpers import get_db, set_config  # noqa: E402


def _db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE, spotify_uri TEXT, spotify_added_at TEXT,
            title TEXT, artist TEXT, album TEXT, duration_ms INTEGER, isrc TEXT,
            spotify_popularity INTEGER, pipeline_stage TEXT, match_status TEXT,
            download_status TEXT, verify_status TEXT, lexicon_status TEXT,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT, track_id INTEGER, message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()
    conn.close()
    return path


class FakeSpotify:
    """Records (limit, offset) of each call; returns items all added BEFORE the
    cutoff so the newest-first walk breaks after the first item."""

    def __init__(self, added_at="2026-07-01T00:00:00Z"):
        self.calls = []
        self.added_at = added_at

    def current_user_saved_tracks(self, limit, offset):
        self.calls.append((limit, offset))
        return {
            "items": [{"added_at": self.added_at,
                       "track": {"id": "old", "name": "Old", "artists": [{"name": "A"}],
                                 "album": {"name": "Alb"}, "external_ids": {}}}],
            "total": 1,
        }


class FastPollTest(unittest.TestCase):
    def setUp(self):
        self.db = _db()
        self._orig_client = poll_spotify.get_spotify_client
        self._orig_sleep = poll_spotify.time.sleep

    def tearDown(self):
        poll_spotify.get_spotify_client = self._orig_client
        poll_spotify.time.sleep = self._orig_sleep
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_incremental_uses_small_page_and_single_call(self):
        set_config(self.db, "last_spotify_poll", "2026-07-05T00:00:00Z")  # newer than items
        set_config(self.db, "spotify_incremental_page_size", "20")
        fake = FakeSpotify(added_at="2026-07-01T00:00:00Z")  # older than cutoff
        poll_spotify.get_spotify_client = lambda db: fake

        poll_spotify._poll(self.db)

        # Change-detection: exactly one tiny call, at the small page size.
        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0][0], 20)   # limit
        self.assertEqual(fake.calls[0][1], 0)    # offset

    def test_page_size_is_clamped(self):
        set_config(self.db, "last_spotify_poll", "2026-07-05T00:00:00Z")
        set_config(self.db, "spotify_incremental_page_size", "999")  # absurd -> clamp to 50
        fake = FakeSpotify()
        poll_spotify.get_spotify_client = lambda db: fake
        poll_spotify._poll(self.db)
        self.assertEqual(fake.calls[0][0], 50)

    def test_backfill_uses_big_page(self):
        set_config(self.db, "backfill_all_liked", "1")
        set_config(self.db, "spotify_incremental_page_size", "10")  # ignored in backfill
        fake = FakeSpotify(added_at="2026-07-01T00:00:00Z")
        poll_spotify.get_spotify_client = lambda db: fake
        poll_spotify._poll(self.db)
        self.assertEqual(fake.calls[0][0], 50)

    def test_429_honours_retry_after_then_succeeds(self):
        slept = []
        poll_spotify.time.sleep = lambda s: slept.append(s)

        class Flaky:
            def __init__(self):
                self.n = 0

            def current_user_saved_tracks(self, limit, offset):
                self.n += 1
                if self.n == 1:
                    raise SpotifyException(429, -1, "rate limited", headers={"Retry-After": "2"})
                return {"items": [], "total": 0}

        flaky = Flaky()
        res = poll_spotify._saved_tracks_with_ratelimit(flaky, limit=20, offset=0)
        self.assertEqual(res["total"], 0)
        self.assertEqual(flaky.n, 2)      # retried once after the 429
        self.assertEqual(slept, [2])      # honoured Retry-After=2


if __name__ == "__main__":
    unittest.main()
