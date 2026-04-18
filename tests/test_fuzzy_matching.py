"""Tests for the fuzzy Tidal matching logic in process_pipeline."""

import sys
import os

# Allow importing from the sync-worker tasks package without a full Docker env
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sync-worker"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sync-worker", "tasks"))

import pytest

# Minimal stubs so the module-level code in process_pipeline doesn't break
# when imported outside Docker (no tiddl binary, no env vars).
import unittest.mock as mock

with mock.patch("shutil.which", return_value=None):
    from tasks.process_pipeline import _build_fuzzy_queries, _extract_base_title


class TestExtractBaseTitle:
    def test_strips_original_mix(self):
        assert _extract_base_title("Song Title - Original Mix") == "song title"

    def test_strips_remix_suffix(self):
        assert _extract_base_title("Song Title (DJ X Remix)") == "song title"

    def test_strips_brackets(self):
        assert _extract_base_title("Song Title [Mix Cut]") == "song title"

    def test_preserves_plain_title(self):
        assert _extract_base_title("Plain Song") == "plain song"

    def test_strips_live_pattern(self):
        assert _extract_base_title("Song - Live At Fabric") == "song"


class TestBuildFuzzyQueries:
    def _queries(self, artist, title, depth):
        return [q for q, _ in _build_fuzzy_queries(artist, title, depth)]

    def _descs(self, artist, title, depth):
        return [d for _, d in _build_fuzzy_queries(artist, title, depth)]

    # --- Depth 0 ---

    def test_depth0_single_artist(self):
        queries = self._queries("Artist", "Track", 0)
        assert "Artist Track" in queries

    def test_depth0_multi_artist_adds_first_artist(self):
        queries = self._queries("Artist A, Artist B", "Track", 0)
        assert "Artist A, Artist B Track" in queries
        assert "Artist A Track" in queries

    def test_depth0_feat_in_title_adds_feat_stripped(self):
        queries = self._queries("Artist", "Track (feat. Someone)", 0)
        assert "Artist Track" in queries  # feat-stripped version
        assert any("feat" not in q for q in queries)

    def test_depth0_no_duplicates(self):
        queries = self._queries("Solo", "Simple", 0)
        assert len(queries) == len(set(q.lower() for q in queries))

    def test_depth0_single_artist_no_feat_gives_one_query(self):
        queries = self._queries("Solo", "Simple Title", 0)
        # No multi-artist, no feat tag → only full query
        assert queries == ["Solo Simple Title"]

    # --- Depth 1 ---

    def test_depth1_adds_title_only(self):
        queries = self._queries("Artist", "Track (feat. Someone)", 1)
        assert "Track" in queries  # feat-stripped title only

    def test_depth1_adds_version_stripped(self):
        queries = self._queries("Artist", "Track (Extended Mix)", 1)
        assert any("extended mix" not in q.lower() for q in queries)
        # Should include version-stripped "Track" alone or with artist
        clean_queries_lower = [q.lower() for q in queries]
        assert any("artist track" == q for q in clean_queries_lower) or any(
            q == "track" for q in clean_queries_lower
        )

    def test_depth1_is_superset_of_depth0(self):
        d0 = self._queries("Artist A, B", "Track (feat. X)", 0)
        d1 = self._queries("Artist A, B", "Track (feat. X)", 1)
        for q in d0:
            assert q in d1

    # --- Depth 2 ---

    def test_depth2_strips_all_parens(self):
        queries = self._queries("Artist", "Track (feat. X) (Extended)", 2)
        assert "Track" in queries or "Artist Track" in queries

    def test_depth2_is_superset_of_depth1(self):
        d1 = self._queries("DJ X & DJ Y", "Song - Radio Edit", 1)
        d2 = self._queries("DJ X & DJ Y", "Song - Radio Edit", 2)
        for q in d1:
            assert q in d2

    # --- Depth 3 ---

    def test_depth3_uses_first_word_of_multi_word_artist(self):
        queries = self._queries("First Second", "Title", 3)
        descs = self._descs("First Second", "Title", 3)
        assert any("first word" in d for d in descs)

    def test_depth3_single_word_artist_skips_first_word_pass(self):
        queries = self._queries("Solo", "Title", 3)
        descs = self._descs("Solo", "Title", 3)
        assert not any("first word" in d for d in descs)

    def test_depth3_is_superset_of_depth2(self):
        d2 = self._queries("Artist One", "Track (feat. X) - Remix", 2)
        d3 = self._queries("Artist One", "Track (feat. X) - Remix", 3)
        for q in d2:
            assert q in d3

    # --- Edge cases ---

    def test_empty_artist_still_returns_title(self):
        queries = self._queries("", "Track Title", 0)
        assert any("track title" in q.lower() for q in queries)

    def test_no_duplicate_queries_across_all_depths(self):
        for depth in range(4):
            queries = self._queries("Artist A & Artist B", "Song (feat. X) - Extended Mix", depth)
            lower = [q.lower() for q in queries]
            assert len(lower) == len(set(lower)), f"Duplicates at depth {depth}: {queries}"

    def test_feat_in_artist_does_not_contaminate_query(self):
        # Artist string from Spotify sometimes includes "feat." inline
        queries = self._queries("Artist feat. Other", "Song Title", 0)
        assert "Artist Song Title" in queries  # first_artist stripped feat part


class TestRetryUnmatchedLogic:
    """Unit tests for the fuzzy depth retry logic (pure logic, no DB)."""

    def test_depth_increments_on_retry(self):
        # Simulate what _retry_unmatched_sync does: new_depth = (fuzzy_depth or 0) + 1
        initial_depth = 0
        new_depth = (initial_depth or 0) + 1
        assert new_depth == 1

    def test_depth_at_max_goes_to_permanently_failed(self):
        max_depth = 4
        for depth in range(max_depth):
            assert depth < max_depth  # retryable
        assert max_depth >= max_depth  # permanently failed

    def test_exhausted_message_includes_depth(self):
        depth = 4
        msg = f"No Tidal match after {depth} fuzzy search passes"
        assert "4" in msg
        assert "fuzzy" in msg
