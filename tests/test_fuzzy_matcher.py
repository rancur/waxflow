"""Integration tests for Tidal fuzzy matching, importing real module code.

These tests complement tests/test_fuzzy_matching.py (which uses inline copies
of the helper functions) by importing and exercising the actual production
implementations in sync-worker/tasks/process_pipeline.py.  The full
_match_track() flow is tested end-to-end with a mocked Tidal API.
"""

import sqlite3
import os
from unittest.mock import patch, MagicMock

import pytest

from tasks.process_pipeline import (
    _normalize_for_comparison,
    _extract_base_title,
    _normalize_artists,
    _artists_match,
    _titles_match,
    _strip_all_parens,
    _strip_features_from_artist,
    _extract_remix_artist,
    _build_search_queries,
    _score_tidal_result,
    _match_track,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(path: str) -> None:
    """Minimal schema required by _match_track() to write into."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY,
            spotify_id TEXT,
            isrc TEXT,
            title TEXT,
            artist TEXT,
            album TEXT,
            duration_ms INTEGER,
            pipeline_stage TEXT DEFAULT 'matching',
            pipeline_error TEXT,
            match_status TEXT DEFAULT 'pending',
            match_source TEXT,
            match_confidence REAL,
            tidal_id TEXT,
            download_status TEXT,
            download_attempts INTEGER DEFAULT 0,
            match_retry_depth INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            track_id INTEGER,
            message TEXT NOT NULL,
            details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            search_query TEXT,
            result_count INTEGER,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.commit()
    conn.close()


def _tidal_item(
    track_id: int,
    title: str,
    artist: str,
    duration_s: int = 200,
    isrc: str = "",
) -> dict:
    return {
        "id": track_id,
        "title": title,
        "artist": {"name": artist},
        "duration": duration_s,
        "isrc": isrc,
    }


@pytest.fixture
def tmp_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    _make_db(db_path)
    return db_path


def _insert_track(db_path: str, **kwargs) -> int:
    defaults = {
        "id": 1,
        "spotify_id": "spot1",
        "isrc": "ISRC123456",
        "title": "Test Track",
        "artist": "Test Artist",
        "album": "Test Album",
        "duration_ms": 200_000,
        "pipeline_stage": "matching",
        "match_status": "pending",
        "match_retry_depth": 0,
    }
    defaults.update(kwargs)
    conn = sqlite3.connect(db_path)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    conn.execute(f"INSERT INTO tracks ({cols}) VALUES ({placeholders})", list(defaults.values()))
    conn.commit()
    conn.close()
    return defaults["id"]


def _fetch_track(db_path: str, track_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM tracks WHERE id = ?", (track_id,)).fetchone()
    conn.close()
    return dict(row)


def _fetch_fallback_attempts(db_path: str, track_id: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM fallback_attempts WHERE track_id = ?", (track_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# _normalize_for_comparison (real implementation)
# ---------------------------------------------------------------------------

class TestNormalizeForComparison:
    def test_lowercase(self):
        assert _normalize_for_comparison("Hello World") == "hello world"

    def test_unicode_normalization(self):
        assert _normalize_for_comparison("café") == "cafe"
        assert _normalize_for_comparison("über") == "uber"

    def test_strips_apostrophe(self):
        assert _normalize_for_comparison("it's") == "its"

    def test_collapses_whitespace(self):
        result = _normalize_for_comparison("  too   many   spaces  ")
        assert "  " not in result

    def test_empty(self):
        assert _normalize_for_comparison("") == ""
        assert _normalize_for_comparison(None) == ""  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _extract_base_title (real implementation)
# ---------------------------------------------------------------------------

class TestExtractBaseTitle:
    def test_strips_original_mix(self):
        assert _extract_base_title("Song - Original Mix") == "song"
        assert _extract_base_title("Song (Original Mix)") == "song"

    def test_strips_radio_edit(self):
        assert _extract_base_title("Song - Radio Edit") == "song"

    def test_strips_remix_suffix(self):
        assert _extract_base_title("Song - DJ Name Remix") == "song"
        assert _extract_base_title("Song (DJ Name Remix)") == "song"

    def test_strips_remaster(self):
        assert _extract_base_title("Song - Remastered") == "song"

    def test_leaves_plain_title_alone(self):
        assert _extract_base_title("Plain Song") == "plain song"

    def test_strips_bracket_annotation(self):
        assert _extract_base_title("Song [Mix Cut]") == "song"


# ---------------------------------------------------------------------------
# _normalize_artists / _artists_match (real implementations)
# ---------------------------------------------------------------------------

class TestNormalizeArtists:
    def test_single_artist(self):
        assert _normalize_artists("Bicep") == {"bicep"}

    def test_comma_separated(self):
        result = _normalize_artists("G Jones, Eprom")
        assert "g jones" in result
        assert "eprom" in result

    def test_ampersand_separated(self):
        result = _normalize_artists("Bonobo & Four Tet")
        assert "bonobo" in result
        assert "four tet" in result

    def test_empty(self):
        assert _normalize_artists("") == set()


class TestArtistsMatch:
    def test_exact(self):
        assert _artists_match("Bicep", "Bicep")

    def test_case_insensitive(self):
        assert _artists_match("bicep", "BICEP")

    def test_multi_artist_order_independent(self):
        assert _artists_match("G Jones, Eprom", "Eprom, G Jones")

    def test_first_artist_subset(self):
        assert _artists_match("Artist A, Artist B", "Artist A")

    def test_no_match(self):
        assert not _artists_match("Bicep", "Four Tet")

    def test_empty_artists(self):
        assert not _artists_match("", "Bicep")


# ---------------------------------------------------------------------------
# _titles_match (real implementation)
# ---------------------------------------------------------------------------

class TestTitlesMatch:
    def test_exact(self):
        assert _titles_match("Glue", "Glue")

    def test_case_insensitive(self):
        assert _titles_match("GLUE", "glue")

    def test_base_title_match(self):
        assert _titles_match("Song - Original Mix", "Song")

    def test_no_match(self):
        assert not _titles_match("Totally Different", "Other Track")

    def test_word_overlap_80pct(self):
        assert _titles_match("Dark Room", "Dark Room Edit")


# ---------------------------------------------------------------------------
# _strip_all_parens / _strip_features_from_artist / _extract_remix_artist
# ---------------------------------------------------------------------------

class TestStripAllParens:
    def test_removes_round_brackets(self):
        assert _strip_all_parens("Song (Club Mix)") == "Song"

    def test_removes_square_brackets(self):
        assert _strip_all_parens("Song [Deluxe]") == "Song"

    def test_multiple_bracket_types(self):
        assert _strip_all_parens("Song (feat. X) [Remix]") == "Song"

    def test_plain_title_unchanged(self):
        assert _strip_all_parens("Plain Song") == "Plain Song"


class TestStripFeaturesFromArtist:
    def test_strips_feat(self):
        assert _strip_features_from_artist("Artist feat. Other") == "Artist"

    def test_strips_ft(self):
        assert _strip_features_from_artist("Artist ft. Other") == "Artist"

    def test_no_feature_unchanged(self):
        assert _strip_features_from_artist("Bicep") == "Bicep"


class TestExtractRemixArtist:
    def test_extracts_remix(self):
        assert _extract_remix_artist("Song (Bicep Remix)") == "Bicep"

    def test_extracts_edit(self):
        assert _extract_remix_artist("Song (Four Tet Edit)") == "Four Tet"

    def test_no_remix_returns_none(self):
        assert _extract_remix_artist("Plain Song") is None

    def test_original_mix_returns_none(self):
        assert _extract_remix_artist("Song (Original Mix)") is None


# ---------------------------------------------------------------------------
# _build_search_queries — verify first_artist determinism
# ---------------------------------------------------------------------------

class TestBuildSearchQueriesFirstArtistDeterminism:
    def test_first_artist_is_first_comma_token(self):
        """first_artist must always be the first-listed artist, not a random set element."""
        queries_and_descs = _build_search_queries("Bicep, Four Tet", "Glue", depth=0)
        queries = [q for q, _ in queries_and_descs]
        # The first-artist query should contain "Bicep" not "Four Tet" as the primary artist
        first_artist_queries = [q for q in queries if "Bicep" in q and "Four Tet" not in q]
        assert first_artist_queries, (
            "Expected a query with 'Bicep' (first-listed) but not 'Four Tet'. "
            f"Got: {queries}"
        )

    def test_stable_across_calls(self):
        """Calling _build_search_queries multiple times must return the same query list."""
        results = [_build_search_queries("Bicep, Four Tet", "Glue", depth=0) for _ in range(10)]
        first = results[0]
        for r in results[1:]:
            assert r == first, "Query list is not deterministic"


# ---------------------------------------------------------------------------
# _score_tidal_result (real implementation)
# ---------------------------------------------------------------------------

class TestScoreTidalResult:
    def _item(self, title: str, artist: str, duration_s: int = 200) -> dict:
        return {"title": title, "artist": {"name": artist}, "duration": duration_s}

    def test_perfect_match(self):
        item = self._item("Glue", "Bicep", 200)
        score = _score_tidal_result(item, "Bicep", "Glue", 200_000, depth=0)
        assert score >= 0.90

    def test_artist_mismatch_returns_zero(self):
        item = self._item("Glue", "Four Tet", 200)
        score = _score_tidal_result(item, "Bicep", "Glue", 200_000, depth=0)
        assert score == 0.0

    def test_empty_artist_skips_artist_check(self):
        item = self._item("Glue", "Any Artist", 200)
        score = _score_tidal_result(item, "", "Glue", 200_000, depth=0)
        assert score > 0.0

    def test_title_mismatch_returns_zero(self):
        item = self._item("Completely Different", "Bicep", 200)
        score = _score_tidal_result(item, "Bicep", "Glue", 200_000, depth=0)
        assert score == 0.0

    def test_duration_affects_confidence(self):
        item_close = self._item("Glue", "Bicep", 200)
        item_far = self._item("Glue", "Bicep", 250)
        score_close = _score_tidal_result(item_close, "Bicep", "Glue", 200_000, depth=0)
        score_far = _score_tidal_result(item_far, "Bicep", "Glue", 200_000, depth=0)
        assert score_close >= score_far

    def test_depth3_accepts_wider_duration(self):
        # 25-second duration gap: rejected at depth 0 (only 15s window), accepted at depth 3
        item = self._item("Glue", "Bicep", 225)
        score_d0 = _score_tidal_result(item, "Bicep", "Glue", 200_000, depth=0)
        score_d3 = _score_tidal_result(item, "Bicep", "Glue", 200_000, depth=3)
        assert score_d0 == 0.0
        assert score_d3 > 0.0


# ---------------------------------------------------------------------------
# _match_track — end-to-end with mocked Tidal API
# ---------------------------------------------------------------------------

class TestMatchTrackISRC:
    def test_isrc_match_succeeds(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="USRC12345678", title="Glue", artist="Bicep")
        track = {
            "id": track_id, "isrc": "USRC12345678",
            "title": "Glue", "artist": "Bicep", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }
        result = [_tidal_item(9001, "Glue", "Bicep", 200, isrc="USRC12345678")]

        with patch("tasks.process_pipeline._tidal_search", return_value=result):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "matched"
        assert row["match_source"] == "isrc"
        assert row["match_confidence"] == 1.0
        assert row["tidal_id"] == "9001"
        assert row["pipeline_stage"] == "downloading"

    def test_isrc_miss_falls_through_to_text_search(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="USRC99999999", title="Glue", artist="Bicep")
        track = {
            "id": track_id, "isrc": "USRC99999999",
            "title": "Glue", "artist": "Bicep", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }

        def fake_search(query: str):
            # ISRC returns no exact match; text query returns one
            if query == "USRC99999999":
                return [_tidal_item(9001, "Something Else", "Bicep", 200, isrc="OTHER")]
            return [_tidal_item(9001, "Glue", "Bicep", 200)]

        with patch("tasks.process_pipeline._tidal_search", side_effect=fake_search):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "matched"
        assert row["match_source"] == "search"

    def test_isrc_miss_logged_in_fallback_attempts(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title="Ghost", artist="Nobody")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Ghost", "artist": "Nobody", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }
        with patch("tasks.process_pipeline._tidal_search", return_value=[]):
            _match_track(tmp_db, track)

        attempts = _fetch_fallback_attempts(tmp_db, track_id)
        assert any(a["search_query"] == "NOMATCH" for a in attempts)


class TestMatchTrackFuzzyDepths:
    def test_depth0_tries_full_and_first_artist_queries(self, tmp_db):
        track_id = _insert_track(
            tmp_db, isrc="NOMATCH", title="Glue", artist="Bicep, Four Tet",
        )
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Glue", "artist": "Bicep, Four Tet", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }
        queries_seen = []

        def fake_search(query: str):
            queries_seen.append(query)
            # Full artist query returns nothing; first-artist query matches
            if "," not in query and "bicep" in query.lower():
                return [_tidal_item(9002, "Glue", "Bicep", 200)]
            return []

        with patch("tasks.process_pipeline._tidal_search", side_effect=fake_search):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "matched"
        # The full-artist query (with comma) must have been tried first
        assert any("," in q for q in queries_seen)

    def test_depth1_strips_parentheticals(self, tmp_db):
        title = "Glue (feat. Someone)"
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title=title, artist="Bicep")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": title, "artist": "Bicep", "duration_ms": 200_000,
            "match_retry_depth": 1,
        }

        def fake_search(query: str):
            # Only match when parenthetical content is stripped
            if "feat" not in query.lower() and "bicep" in query.lower():
                return [_tidal_item(9003, "Glue", "Bicep", 200)]
            return []

        with patch("tasks.process_pipeline._tidal_search", side_effect=fake_search):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "matched"

    def test_depth2_tries_title_only_query(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title="Glue", artist="Bicep")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Glue", "artist": "Bicep", "duration_ms": 200_000,
            "match_retry_depth": 2,
        }

        def fake_search(query: str):
            # Only the title-only query "Glue" returns a result
            if query.lower().strip() == "glue":
                return [_tidal_item(9004, "Glue", "Bicep Electronic", 200)]
            return []

        with patch("tasks.process_pipeline._tidal_search", side_effect=fake_search):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "matched"

    def test_all_passes_fail_sets_error(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title="Ghost Track", artist="Nobody")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Ghost Track", "artist": "Nobody", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }
        with patch("tasks.process_pipeline._tidal_search", return_value=[]):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert row["match_status"] == "failed"
        assert row["pipeline_stage"] == "error"
        assert "No Tidal match found" in row["pipeline_error"]

    def test_failed_queries_logged_in_fallback_attempts(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title="Ghost Track", artist="Nobody")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Ghost Track", "artist": "Nobody", "duration_ms": 200_000,
            "match_retry_depth": 0,
        }
        with patch("tasks.process_pipeline._tidal_search", return_value=[]):
            _match_track(tmp_db, track)

        attempts = _fetch_fallback_attempts(tmp_db, track_id)
        # At minimum: ISRC query + at least one text query
        assert len(attempts) >= 2

    def test_depth_stored_in_error_message(self, tmp_db):
        track_id = _insert_track(tmp_db, isrc="NOMATCH", title="Ghost", artist="Nobody")
        track = {
            "id": track_id, "isrc": "NOMATCH",
            "title": "Ghost", "artist": "Nobody", "duration_ms": 200_000,
            "match_retry_depth": 2,
        }
        with patch("tasks.process_pipeline._tidal_search", return_value=[]):
            _match_track(tmp_db, track)

        row = _fetch_track(tmp_db, track_id)
        assert "2" in row["pipeline_error"]
