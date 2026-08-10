"""Tests for post-processing coverage rollups.

The number this produces is the whole point -- it is what turns "I think cue points
stopped generating" into a fact. So the counting rules have to be right:
an empty cue list is not coverage, a bpm of 0 means unanalyzed, and archived tracks
are not part of the library and must not drag the percentage down.
"""

import os
import sys
import unittest
from unittest import mock

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


class FetchPaginationTest(unittest.TestCase):
    """Lexicon caps /v1/tracks at 1000 rows and rejects larger limits.

    Reading the unpaginated response LOOKED like it worked -- a full 1000 tracks
    came back and the percentages were plausible -- while actually measuring only
    the oldest fifth of a 5,611-track library. The log line ("1000 active tracks")
    was the only tell. A silently-truncated denominator is the worst failure mode
    for a module whose entire output is a percentage.
    """

    def _client_returning(self, total, page_size=1000):
        pages = []

        def get(path, params=None, **kw):
            offset = (params or {}).get("offset", 0)
            limit = (params or {}).get("limit", page_size)
            batch = [{"id": i} for i in range(offset, min(offset + limit, total))]
            pages.append(offset)
            resp = mock.MagicMock()
            resp.json.return_value = {
                "data": {"total": total, "limit": limit,
                         "offset": offset, "tracks": batch}
            }
            resp.raise_for_status.return_value = None
            return resp

        client = mock.MagicMock()
        client.get.side_effect = get
        client.__enter__ = lambda s: client
        client.__exit__ = lambda s, *a: False
        return client, pages

    def test_fetches_every_page_not_just_the_first(self):
        client, pages = self._client_returning(5611)
        with mock.patch.object(lc.httpx, "Client", return_value=client):
            tracks = lc.fetch_tracks("http://lexicon.test")
        self.assertEqual(len(tracks), 5611)
        self.assertEqual(pages, [0, 1000, 2000, 3000, 4000, 5000])

    def test_exact_multiple_of_page_size_terminates(self):
        # The off-by-one trap: a final page that is exactly full, with nothing after.
        client, _ = self._client_returning(2000)
        with mock.patch.object(lc.httpx, "Client", return_value=client):
            tracks = lc.fetch_tracks("http://lexicon.test")
        self.assertEqual(len(tracks), 2000)

    def test_single_short_page_makes_one_request(self):
        client, pages = self._client_returning(42)
        with mock.patch.object(lc.httpx, "Client", return_value=client):
            tracks = lc.fetch_tracks("http://lexicon.test")
        self.assertEqual(len(tracks), 42)
        self.assertEqual(pages, [0])

    def test_empty_library(self):
        client, _ = self._client_returning(0)
        with mock.patch.object(lc.httpx, "Client", return_value=client):
            self.assertEqual(lc.fetch_tracks("http://lexicon.test"), [])

    def test_never_requests_a_limit_lexicon_would_reject(self):
        client, _ = self._client_returning(3000)
        with mock.patch.object(lc.httpx, "Client", return_value=client):
            lc.fetch_tracks("http://lexicon.test")
        for call in client.get.call_args_list:
            self.assertLessEqual(call.kwargs["params"]["limit"], 1000)


if __name__ == "__main__":
    unittest.main()
