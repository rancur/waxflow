"""Tests for the non-music ingest filter (keep audiobooks/podcasts/spoken-word
out of the DJ library)."""

import os
import sys
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks.nonmusic_filter import DEFAULT_MAX_DURATION_MS, is_nonmusic  # noqa: E402


class TestNonMusicFilter(unittest.TestCase):
    def test_normal_music_track_passes(self):
        skip, reason = is_nonmusic(
            {"type": "track", "duration_ms": 4 * 60 * 1000,
             "title": "Better Today Better Tomorrow", "artists": "Kasablanca"})
        self.assertFalse(skip, reason)

    def test_podcast_episode_type_is_skipped(self):
        skip, reason = is_nonmusic({"type": "episode", "duration_ms": 5 * 60 * 1000, "title": "Ep 12"})
        self.assertTrue(skip)
        self.assertIn("non_track_type", reason)

    def test_episode_flag_is_skipped(self):
        skip, reason = is_nonmusic({"type": "track", "episode": True, "title": "Some Show"})
        self.assertTrue(skip)
        self.assertEqual(reason, "episode")

    def test_audiobook_chapter_keyword_is_skipped(self):
        # LibriVox-style: normal 'track' type, short chapter duration -> keyword catches it.
        skip, reason = is_nonmusic(
            {"type": "track", "duration_ms": 3 * 60 * 1000,
             "title": "The Hound of the Baskervilles - Chapter 7",
             "artists": "Arthur Conan Doyle"})
        self.assertTrue(skip)
        self.assertIn("keyword", reason)

    def test_librivox_album_keyword_is_skipped(self):
        skip, reason = is_nonmusic(
            {"type": "track", "duration_ms": 4 * 60 * 1000, "title": "001 of 111",
             "album": "The Hound of the Baskervilles (LibriVox)", "artists": "Arthur Conan Doyle"})
        self.assertTrue(skip)
        self.assertIn("keyword", reason)

    def test_long_duration_is_skipped(self):
        skip, reason = is_nonmusic(
            {"type": "track", "duration_ms": 45 * 60 * 1000, "title": "Very Long Thing"})
        self.assertTrue(skip)
        self.assertIn("long_duration", reason)

    def test_duration_cap_is_configurable(self):
        # A 40-min continuous DJ mix should pass if the cap is raised to 60 min.
        meta = {"type": "track", "duration_ms": 40 * 60 * 1000, "title": "Continuous Mix"}
        self.assertTrue(is_nonmusic(meta)[0])  # default 30-min cap -> skipped
        self.assertFalse(is_nonmusic(meta, max_duration_ms=60 * 60 * 1000)[0])

    def test_missing_duration_does_not_crash_or_skip(self):
        skip, _ = is_nonmusic({"type": "track", "title": "No Duration", "duration_ms": None})
        self.assertFalse(skip)

    def test_default_cap_is_30_minutes(self):
        self.assertEqual(DEFAULT_MAX_DURATION_MS, 30 * 60 * 1000)

    def test_song_titled_chapters_is_not_falsely_skipped(self):
        # "Chapters" without a following number must NOT match the chapter-<n> rule.
        skip, _ = is_nonmusic(
            {"type": "track", "duration_ms": 3 * 60 * 1000, "title": "Chapters", "artists": "Yellow Claw"})
        self.assertFalse(skip)


if __name__ == "__main__":
    unittest.main()
