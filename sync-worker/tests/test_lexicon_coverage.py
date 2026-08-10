"""Tests for post-processing coverage rollups.

The number this produces is the whole point -- it is what turns "I think cue points
stopped generating" into a fact. So the counting rules have to be right:
an empty cue list is not coverage, a bpm of 0 means unanalyzed, and archived tracks
are not part of the library and must not drag the percentage down.
"""

import os
import sys
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import lexicon_coverage as lc  # noqa: E402


def _track(**kw):
    base = {"cuepoints": [], "tempomarkers": [], "tags": [],
            "bpm": 0, "key": "", "genre": "", "archived": False}
    base.update(kw)
    return base


class SummarizeTest(unittest.TestCase):
    def test_empty_list_is_not_coverage(self):
        # The bug this guards: `"cuepoints" in track` is True even when the list is
        # empty, which would report 100% cue coverage for a library with none.
        summary = lc.summarize([_track(), _track()])
        self.assertEqual(summary["coverage"]["cuepoints"]["count"], 0)
        self.assertEqual(summary["coverage"]["cuepoints"]["pct"], 0.0)

    def test_populated_fields_are_counted(self):
        tracks = [
            _track(cuepoints=[{"id": 1}], bpm=128, key="Am", genre="House"),
            _track(cuepoints=[{"id": 2}], bpm=140),
        ]
        summary = lc.summarize(tracks)
        self.assertEqual(summary["coverage"]["cuepoints"]["count"], 2)
        self.assertEqual(summary["coverage"]["cuepoints"]["pct"], 100.0)
        self.assertEqual(summary["coverage"]["genre"]["count"], 1)
        self.assertEqual(summary["coverage"]["genre"]["pct"], 50.0)

    def test_zero_bpm_means_unanalyzed(self):
        summary = lc.summarize([_track(bpm=0), _track(bpm=128)])
        self.assertEqual(summary["coverage"]["bpm"]["count"], 1)

    def test_whitespace_only_string_is_not_coverage(self):
        summary = lc.summarize([_track(genre="   "), _track(genre="Techno")])
        self.assertEqual(summary["coverage"]["genre"]["count"], 1)

    def test_archived_tracks_do_not_drag_the_percentage_down(self):
        # An archived track has left the library; counting it as "missing cues"
        # would make coverage look broken when it is not.
        tracks = [
            _track(cuepoints=[{"id": 1}]),
            _track(archived=True),
        ]
        summary = lc.summarize(tracks)
        self.assertEqual(summary["total_tracks"], 2)
        self.assertEqual(summary["active_tracks"], 1)
        self.assertEqual(summary["coverage"]["cuepoints"]["pct"], 100.0)

    def test_empty_library_does_not_divide_by_zero(self):
        summary = lc.summarize([])
        self.assertEqual(summary["active_tracks"], 0)
        self.assertEqual(summary["coverage"]["cuepoints"]["pct"], 0.0)

    def test_reports_what_it_cannot_measure(self):
        # Lexicon's API exposes no artwork or cloud field. Saying so is better than
        # silently omitting them and implying they were checked.
        summary = lc.summarize([_track()])
        self.assertIn("artwork", summary["unavailable"])
        self.assertIn("cloud", summary["unavailable"])

    def test_missing_field_is_treated_as_absent_not_an_error(self):
        summary = lc.summarize([{"title": "no post-processing fields at all"}])
        self.assertEqual(summary["coverage"]["cuepoints"]["count"], 0)


if __name__ == "__main__":
    unittest.main()
