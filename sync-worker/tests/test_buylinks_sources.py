"""Tests for the Phase 4 buy-link source plugins (Beatport / Qobuz / Bandcamp).

Verifies each plugin produces a WELL-FORMED, correctly-encoded store search URL
from a TrackQuery, uses the right host/path/param per platform, dedups sanely, and
NEVER advertises an acquire/purchase capability. No network — pure URL building.
"""

import os
import sys
import unittest
from urllib.parse import parse_qs, urlparse

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks.sources import bandcamp, beatport, qobuz  # noqa: E402
from tasks.sources.base import SourceCapability, TrackQuery  # noqa: E402
from tasks.sources.linkbuild import dedup_key, search_terms  # noqa: E402

Q = TrackQuery(artist="Daft Punk", title="One More Time", isrc="GBDUW0000059")
Q_SPECIAL = TrackQuery(artist="Röyksopp & Susanne Sundfør", title="Never Ever")


class TestSearchTerms(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(search_terms(Q), "Daft Punk One More Time")

    def test_whitespace_collapsed(self):
        q = TrackQuery(artist="  A   B ", title=" C  ")
        self.assertEqual(search_terms(q), "A B C")

    def test_title_only(self):
        self.assertEqual(search_terms(TrackQuery(title="Solo")), "Solo")

    def test_empty(self):
        self.assertEqual(search_terms(TrackQuery()), "")


class TestBeatport(unittest.TestCase):
    def test_url_shape(self):
        url = beatport.build_url(Q)
        parsed = urlparse(url)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "www.beatport.com")
        self.assertEqual(parsed.path, "/search/tracks")
        self.assertEqual(parse_qs(parsed.query)["q"][0], "Daft Punk One More Time")

    def test_special_chars_encoded(self):
        url = beatport.build_url(Q_SPECIAL)
        # Ampersand in the artist must be encoded so it does not split the query.
        self.assertNotIn("& Susanne", url)
        parsed = urlparse(url)
        self.assertEqual(
            parse_qs(parsed.query)["q"][0], "Röyksopp & Susanne Sundfør Never Ever"
        )

    def test_capabilities_link_only(self):
        s = beatport.BeatportSource()
        self.assertIn(SourceCapability.SEARCH_LINK, s.capabilities)
        self.assertNotIn(SourceCapability.ACQUIRE, s.capabilities)
        self.assertNotIn(SourceCapability.LOSSLESS, s.capabilities)

    def test_purchase_link_result(self):
        res = beatport.BeatportSource().purchase_link(Q)
        self.assertIsNotNone(res)
        self.assertEqual(res.kind, "link")
        self.assertEqual(res.source, "beatport")
        self.assertTrue(res.url.startswith("https://www.beatport.com/search/tracks?q="))

    def test_no_terms_no_link(self):
        self.assertIsNone(beatport.BeatportSource().purchase_link(TrackQuery()))


class TestQobuz(unittest.TestCase):
    def test_url_shape_path_based(self):
        url = qobuz.build_url(Q)
        parsed = urlparse(url)
        self.assertEqual(parsed.netloc, "www.qobuz.com")
        # Qobuz uses a path-based track search, not ?q=.
        self.assertTrue(parsed.path.startswith("/us-en/search/tracks/"))
        self.assertEqual(parsed.query, "")
        # Spaces encoded as %20 in the path segment.
        self.assertIn("Daft%20Punk%20One%20More%20Time", url)

    def test_link_only_without_creds(self):
        s = qobuz.QobuzSource()
        self.assertIn(SourceCapability.SEARCH_LINK, s.capabilities)
        self.assertNotIn(SourceCapability.ACQUIRE, s.capabilities)

    def test_purchase_link_result(self):
        res = qobuz.QobuzSource().purchase_link(Q)
        self.assertEqual(res.source, "qobuz")
        self.assertEqual(res.kind, "link")


class TestBandcamp(unittest.TestCase):
    def test_url_shape(self):
        url = bandcamp.build_url(Q)
        parsed = urlparse(url)
        self.assertEqual(parsed.netloc, "bandcamp.com")
        self.assertEqual(parsed.path, "/search")
        qs = parse_qs(parsed.query)
        self.assertEqual(qs["q"][0], "Daft Punk One More Time")
        self.assertEqual(qs["item_type"][0], "t")  # track-scoped

    def test_capabilities_link_only(self):
        s = bandcamp.BandcampSource()
        self.assertIn(SourceCapability.SEARCH_LINK, s.capabilities)
        self.assertNotIn(SourceCapability.ACQUIRE, s.capabilities)


class TestDedupKey(unittest.TestCase):
    def test_prefers_isrc(self):
        key = dedup_key("beatport", Q)
        self.assertEqual(key, "beatport:gbduw0000059")

    def test_falls_back_to_terms(self):
        q = TrackQuery(artist="A", title="B")
        self.assertEqual(dedup_key("qobuz", q), "qobuz:a b")

    def test_source_scoped(self):
        self.assertNotEqual(dedup_key("beatport", Q), dedup_key("qobuz", Q))


if __name__ == "__main__":
    unittest.main()
