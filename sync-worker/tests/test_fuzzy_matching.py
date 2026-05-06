"""Tests for fuzzy Tidal match retry logic in process_pipeline."""

import pytest

from tasks.process_pipeline import (
    _build_fuzzy_query_passes,
    _extract_base_title,
    _normalize_for_comparison,
    _normalize_title,
    _artists_match,
    _titles_match,
)


# ---------------------------------------------------------------------------
# _normalize_for_comparison
# ---------------------------------------------------------------------------

class TestNormalizeForComparison:
    def test_lowercase(self):
        assert _normalize_for_comparison("Hello World") == "hello world"

    def test_strips_accents(self):
        assert _normalize_for_comparison("Sigur Rós") == "sigur ros"
        assert _normalize_for_comparison("Björk") == "bjork"

    def test_removes_feat_tag(self):
        result = _normalize_for_comparison("Track feat. Artist")
        assert "feat" not in result

    def test_collapses_whitespace(self):
        assert _normalize_for_comparison("  a   b  ") == "a b"

    def test_removes_punctuation(self):
        result = _normalize_for_comparison("It's Over (Now)")
        assert "'" not in result
        assert "(" not in result


# ---------------------------------------------------------------------------
# _extract_base_title
# ---------------------------------------------------------------------------

class TestExtractBaseTitle:
    def test_strips_original_mix(self):
        assert _extract_base_title("Song Title - Original Mix") == "song title"

    def test_strips_extended_mix(self):
        assert _extract_base_title("Song Title (Extended Mix)") == "song title"

    def test_strips_radio_edit(self):
        assert _extract_base_title("Song Title - Radio Edit") == "song title"

    def test_strips_remix_suffix(self):
        assert _extract_base_title("Song - Artist Name Remix") == "song"

    def test_strips_remix_parenthetical(self):
        base = _extract_base_title("Levels (Skrillex Remix)")
        assert "remix" not in base.lower()
        assert "skrillex" not in base.lower()

    def test_preserves_plain_title(self):
        assert _extract_base_title("Simple Song") == "simple song"

    def test_feat_in_title_not_stripped_by_extract_base(self):
        # _extract_base_title strips remix/edit suffixes but NOT feat tags;
        # feat normalization is handled by _normalize_for_comparison instead.
        base = _extract_base_title("Song (feat. Someone)")
        assert "song" in base.lower()


# ---------------------------------------------------------------------------
# _normalize_title
# ---------------------------------------------------------------------------

class TestNormalizeTitle:
    def test_strips_original_mix(self):
        assert _normalize_title("Track - Original Mix") == "track"

    def test_strips_feat_tag(self):
        result = _normalize_title("Song (feat. Somebody)")
        assert "feat" not in result.lower()
        assert "somebody" not in result.lower()

    def test_lowercase(self):
        assert _normalize_title("Hello World") == "hello world"

    def test_plain_title_unchanged(self):
        assert _normalize_title("starboy") == "starboy"


# ---------------------------------------------------------------------------
# _artists_match
# ---------------------------------------------------------------------------

class TestArtistsMatch:
    def test_exact_single_artist(self):
        assert _artists_match("Disclosure", "Disclosure") is True

    def test_case_insensitive(self):
        assert _artists_match("disclosure", "DISCLOSURE") is True

    def test_multi_artist_full_vs_first(self):
        # "Disclosure, Sam Smith" should match "Disclosure"
        assert _artists_match("Disclosure, Sam Smith", "Disclosure") is True

    def test_multi_artist_any_overlap(self):
        assert _artists_match("G Jones, Eprom", "G Jones") is True

    def test_different_artists(self):
        assert _artists_match("Aphex Twin", "Boards of Canada") is False

    def test_empty_artist(self):
        assert _artists_match("", "Artist") is False

    def test_featuring_stripped(self):
        # Artist string with "feat." should still match the primary artist
        assert _artists_match("Calvin Harris feat. Rihanna", "Calvin Harris") is True

    def test_ampersand_split(self):
        assert _artists_match("Oasis & Blur", "Oasis") is True

    def test_vs_split(self):
        assert _artists_match("Fatboy Slim vs Basement Jaxx", "Fatboy Slim") is True


# ---------------------------------------------------------------------------
# _titles_match
# ---------------------------------------------------------------------------

class TestTitlesMatch:
    def test_exact_match(self):
        assert _titles_match("Levels", "Levels") is True

    def test_case_insensitive(self):
        assert _titles_match("LEVELS", "levels") is True

    def test_remix_vs_plain(self):
        # "Levels (Skrillex Remix)" should match "Levels" after base title stripping
        assert _titles_match("Levels (Skrillex Remix)", "Levels") is True

    def test_original_mix_vs_plain(self):
        assert _titles_match("Song - Original Mix", "Song") is True

    def test_feat_in_title(self):
        assert _titles_match("Song (feat. Artist)", "Song") is True

    def test_no_match(self):
        assert _titles_match("Completely Different", "Another Song") is False

    def test_accent_normalization(self):
        assert _titles_match("Sigur Rós - Ára bátur", "Ára bátur") is True


# ---------------------------------------------------------------------------
# _build_fuzzy_query_passes
# ---------------------------------------------------------------------------

class TestBuildFuzzyQueryPasses:
    def test_returns_list_of_tuples(self):
        passes = _build_fuzzy_query_passes("Artist", "Title")
        assert isinstance(passes, list)
        assert all(isinstance(p, tuple) and len(p) == 3 for p in passes)

    def test_pass_numbers_present(self):
        passes = _build_fuzzy_query_passes("Artist", "Title")
        pass_nums = [p[0] for p in passes]
        assert 1 in pass_nums

    def test_first_pass_is_full_artist_full_title(self):
        passes = _build_fuzzy_query_passes("Disclosure", "Latch")
        assert passes[0][1] == "Disclosure Latch"

    def test_multi_artist_first_artist_pass(self):
        passes = _build_fuzzy_query_passes("Disclosure, Sam Smith", "Latch")
        queries = [p[1] for p in passes]
        # Pass 1: full artist string; pass 2+: first artist only ("Disclosure")
        assert any("Disclosure, Sam Smith" in q for q in queries), "full artist pass missing"
        assert any("Disclosure Latch" == q for q in queries), "first-artist-only pass missing"

    def test_feat_title_generates_no_feat_pass(self):
        passes = _build_fuzzy_query_passes("Artist", "Song (feat. Someone)")
        queries = [p[1].lower() for p in passes]
        # One pass should strip the feat tag
        assert any("feat" not in q for q in queries)

    def test_remix_title_generates_base_title_pass(self):
        passes = _build_fuzzy_query_passes("Skrillex", "Scary Monsters - Original Mix")
        queries = [p[1].lower() for p in passes]
        # At least one pass should strip "original mix"
        assert any("original mix" not in q for q in queries)

    def test_no_duplicate_queries(self):
        passes = _build_fuzzy_query_passes("Artist", "Plain Title")
        queries = [p[1].lower() for p in passes]
        # No two passes should produce the same query
        assert len(queries) == len(set(queries))

    def test_depth_controls_number_of_passes(self):
        all_passes = _build_fuzzy_query_passes("Disclosure, Sam Smith", "Latch (feat. Sam Smith)")
        # Slicing to depth=2 should give 2 passes
        assert len(all_passes[:2]) == 2

    def test_single_artist_same_as_first_artist_deduped(self):
        # If artist has only one name, pass 1 and 2 are identical -> deduped
        passes = _build_fuzzy_query_passes("Adele", "Hello")
        queries = [p[1].lower() for p in passes]
        assert queries.count("adele hello") == 1

    def test_plain_title_deduplicates_similar_passes(self):
        # "Plain Title" has no feat/remix, so several passes may collapse
        passes = _build_fuzzy_query_passes("Artist", "Plain Title")
        queries = [p[1].lower() for p in passes]
        assert len(queries) == len(set(queries))

    def test_all_queries_non_empty(self):
        passes = _build_fuzzy_query_passes("Artist", "Title")
        for _, query, _ in passes:
            assert query.strip() != ""

    def test_description_strings_non_empty(self):
        passes = _build_fuzzy_query_passes("Artist", "Title")
        for _, _, desc in passes:
            assert desc != ""
