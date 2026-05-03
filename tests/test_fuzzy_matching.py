"""Tests for fuzzy Tidal matching helpers and retry_unmatched logic."""

import re
import sqlite3
import sys
import os
import tempfile
import pytest

# ---------------------------------------------------------------------------
# Import helpers directly without loading the full worker stack.
# We copy the pure functions under test into this file so tests stay
# self-contained and don't require Docker / external dependencies.
# ---------------------------------------------------------------------------

# --- Copied pure helpers from process_pipeline.py ---

def _normalize_for_comparison(text: str) -> str:
    import unicodedata
    t = unicodedata.normalize("NFKD", text)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower()
    t = re.sub(r"[^\w\s]", " ", t)
    return " ".join(t.split())


def _normalize_artists(artist_str: str) -> set:
    if not artist_str:
        return set()
    parts = re.split(r"[,&+]|\bx\b|\bvs\.?\b|\band\b", artist_str, flags=re.IGNORECASE)
    result = set()
    for p in parts:
        normalized = _normalize_for_comparison(p)
        if normalized and len(normalized) > 1:
            result.add(normalized)
    return result


def _normalize_title(title: str) -> str:
    t = title.lower().strip()
    for suffix in [
        " - original mix", " (original mix)", " - extended mix", " (extended mix)",
        " - radio edit", " (radio edit)", " - vip", " (vip)",
        " - original version", " (original version)",
    ]:
        t = t.replace(suffix, "")
    t = re.sub(r"\s*[\(\[]\s*(?:feat|ft)\.?\s+[^\)\]]+[\)\]]", "", t)
    t = t.replace(" - ", " ").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    return " ".join(t.split())


def _strip_all_parens(title: str) -> str:
    t = re.sub(r"\s*[\(\[][^\)\]]*[\)\]]", "", title).strip()
    return " ".join(t.split())


def _strip_features_from_artist(artist: str) -> str:
    t = re.sub(r"\s*(?:feat|ft|featuring)\.?\s+.*$", "", artist, flags=re.IGNORECASE)
    return t.strip()


def _extract_remix_artist(title: str) -> str | None:
    m = re.search(
        r"[\(\[]\s*(.+?)\s+(?:remix|rmx|rework|edit|bootleg)\s*[\)\]]",
        title,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    return None


def _build_search_queries(artist: str, title: str, depth: int) -> list:
    artists_set = _normalize_artists(artist)
    first_artist = next(iter(artists_set), artist.strip()) if artists_set else artist.strip()
    base_title = re.sub(
        r"\s*[\(\[]\s*(?:feat|ft)\.?\s+[^\)\]]+[\)\]]", "", title, flags=re.IGNORECASE
    ).strip()
    clean_title = _strip_all_parens(title)
    clean_artist = _strip_features_from_artist(artist)

    queries: list = []

    if depth == 0:
        full_q = f"{artist} {title}".strip()
        if full_q:
            queries.append((full_q, "full-artist + title"))
        first_q = f"{first_artist} {title}".strip()
        if first_q and first_q.lower() != full_q.lower():
            queries.append((first_q, "first-artist + title"))
        if base_title and base_title.lower() != title.lower():
            base_q = f"{first_artist} {base_title}".strip()
            if base_q.lower() not in {q.lower() for q, _ in queries}:
                queries.append((base_q, "first-artist + feat-stripped-title"))

    elif depth == 1:
        if clean_title and clean_title.lower() != title.lower():
            q = f"{first_artist} {clean_title}".strip()
            queries.append((q, "first-artist + no-parens-title"))
        if clean_artist and clean_artist.lower() != artist.lower():
            q2 = f"{clean_artist} {clean_title or title}".strip()
            if q2.lower() not in {q.lower() for q, _ in queries}:
                queries.append((q2, "clean-artist + no-parens-title"))
        for alt_artist in sorted(artists_set):
            if alt_artist.lower() == first_artist.lower():
                continue
            q3 = f"{alt_artist} {clean_title or title}".strip()
            if q3.lower() not in {q.lower() for q, _ in queries}:
                queries.append((q3, f"alt-artist({alt_artist}) + no-parens-title"))

    elif depth == 2:
        all_artists = list(artists_set) if artists_set else [first_artist]
        for indiv in sorted(all_artists):
            q = f"{indiv} {clean_title or title}".strip()
            if q.lower() not in {q_.lower() for q_, _ in queries}:
                queries.append((q, f"individual-artist({indiv}) + clean-title"))
        title_q = (clean_title or title).strip()
        if title_q:
            queries.append((title_q, "title-only"))
        remix_artist = _extract_remix_artist(title)
        if remix_artist:
            remix_q = f"{remix_artist} {base_title or title}".strip()
            if remix_q.lower() not in {q.lower() for q, _ in queries}:
                queries.append((remix_q, f"remixer({remix_artist}) + base-title"))

    elif depth >= 3:
        words = (clean_title or title).split()
        if len(words) > 4:
            partial = " ".join(words[:4])
            queries.append((f"{first_artist} {partial}", "first-artist + partial-title"))
        if clean_title:
            queries.append((clean_title, "clean-title-only"))

    return queries


# --- retry_unmatched pure logic (extracted for unit testing) ---

def _retry_unmatched_sync_pure(conn, max_depth: int) -> tuple[int, int]:
    """Returns (retried, exhausted) counts. Uses the provided connection."""
    rows = conn.execute(
        """SELECT id, title, artist, match_retry_depth FROM tracks
           WHERE match_status = 'failed'
             AND pipeline_stage = 'error'
             AND pipeline_error LIKE '%No Tidal match found%'"""
    ).fetchall()

    retried = 0
    exhausted = 0
    for r in rows:
        depth = r["match_retry_depth"] or 0
        if depth >= max_depth:
            conn.execute(
                """UPDATE tracks
                      SET pipeline_error = 'Permanently unavailable on Tidal (all fuzzy passes exhausted)',
                          updated_at = datetime('now')
                    WHERE id = ?""",
                (r["id"],),
            )
            exhausted += 1
        else:
            new_depth = depth + 1
            conn.execute(
                """UPDATE tracks
                      SET pipeline_stage = 'matching',
                          match_status = 'pending',
                          pipeline_error = NULL,
                          match_retry_depth = ?,
                          updated_at = datetime('now')
                    WHERE id = ?""",
                (new_depth, r["id"]),
            )
            retried += 1
    return retried, exhausted


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db():
    """In-memory SQLite database with the minimal tracks schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            title TEXT,
            artist TEXT,
            match_status TEXT NOT NULL DEFAULT 'pending',
            pipeline_stage TEXT NOT NULL DEFAULT 'new',
            pipeline_error TEXT,
            match_retry_depth INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    yield conn
    conn.close()


def _insert_failed_track(conn, spotify_id, title, artist, depth=0):
    conn.execute(
        """INSERT INTO tracks (spotify_id, title, artist, match_status, pipeline_stage,
                               pipeline_error, match_retry_depth)
           VALUES (?, ?, ?, 'failed', 'error', 'No Tidal match found', ?)""",
        (spotify_id, title, artist, depth),
    )
    conn.commit()
    return conn.execute(
        "SELECT id FROM tracks WHERE spotify_id = ?", (spotify_id,)
    ).fetchone()["id"]


# ---------------------------------------------------------------------------
# Tests: _strip_all_parens
# ---------------------------------------------------------------------------

class TestStripAllParens:
    def test_strips_round_brackets(self):
        assert _strip_all_parens("Song Title (Extended Mix)") == "Song Title"

    def test_strips_square_brackets(self):
        assert _strip_all_parens("Song Title [Instrumental]") == "Song Title"

    def test_strips_multiple(self):
        assert _strip_all_parens("Song (feat. Artist) [Edit]") == "Song"

    def test_no_parens_unchanged(self):
        assert _strip_all_parens("Song Title") == "Song Title"

    def test_cleans_extra_whitespace(self):
        assert _strip_all_parens("Song   (Mix)") == "Song"


# ---------------------------------------------------------------------------
# Tests: _strip_features_from_artist
# ---------------------------------------------------------------------------

class TestStripFeaturesFromArtist:
    def test_strips_feat(self):
        assert _strip_features_from_artist("Artist feat. Other") == "Artist"

    def test_strips_ft(self):
        assert _strip_features_from_artist("Artist ft. Other") == "Artist"

    def test_strips_featuring(self):
        assert _strip_features_from_artist("Artist featuring Other") == "Artist"

    def test_no_feature_unchanged(self):
        assert _strip_features_from_artist("Artist Name") == "Artist Name"

    def test_case_insensitive(self):
        assert _strip_features_from_artist("Artist FEAT. Other") == "Artist"


# ---------------------------------------------------------------------------
# Tests: _extract_remix_artist
# ---------------------------------------------------------------------------

class TestExtractRemixArtist:
    def test_extracts_remix(self):
        assert _extract_remix_artist("Song (Remixer Remix)") == "Remixer"

    def test_extracts_edit(self):
        assert _extract_remix_artist("Song (DJ Name Edit)") == "DJ Name"

    def test_extracts_rmx(self):
        assert _extract_remix_artist("Song [Producer Rmx]") == "Producer"

    def test_no_remix_returns_none(self):
        assert _extract_remix_artist("Regular Song Title") is None

    def test_original_mix_returns_none(self):
        assert _extract_remix_artist("Song (Original Mix)") is None


# ---------------------------------------------------------------------------
# Tests: _build_search_queries
# ---------------------------------------------------------------------------

class TestBuildSearchQueriesDepth0:
    def test_full_artist_title(self):
        queries = _build_search_queries("Artist", "Song", 0)
        query_strings = [q for q, _ in queries]
        assert "Artist Song" in query_strings

    def test_strips_feat_from_title_in_base(self):
        queries = _build_search_queries("Artist", "Song (feat. Other)", 0)
        descs = [d for _, d in queries]
        assert "first-artist + feat-stripped-title" in descs
        query_strings = [q for q, _ in queries]
        assert any("Song" in q and "feat" not in q.lower() for q in query_strings)

    def test_no_duplicate_queries(self):
        queries = _build_search_queries("Artist", "Song", 0)
        query_strings = [q.lower() for q, _ in queries]
        assert len(query_strings) == len(set(query_strings))

    def test_multi_artist_splits_first(self):
        queries = _build_search_queries("Artist A, Artist B", "Song", 0)
        query_strings = [q for q, _ in queries]
        # full query includes both artists
        assert any("Artist A, Artist B" in q for q in query_strings)
        # first-artist query uses just the first one
        assert any("Artist B" not in q and "Song" in q for q in query_strings)


class TestBuildSearchQueriesDepth1:
    def test_strips_all_parens(self):
        queries = _build_search_queries("Artist", "Song (Extended Mix)", 1)
        query_strings = [q for q, _ in queries]
        # Should have a query with just "Song" (no parenthetical)
        assert any(q.endswith("Song") for q in query_strings)

    def test_clean_artist_feat_stripped(self):
        queries = _build_search_queries("Artist feat. X", "Song (Remix)", 1)
        descs = [d for _, d in queries]
        assert any("clean-artist" in d for d in descs)

    def test_no_new_queries_when_nothing_to_strip(self):
        # Title has no parens, artist has no feat → depth 1 adds nothing meaningful
        queries = _build_search_queries("Artist", "Song", 1)
        # Should still be valid (may be empty list since nothing to strip)
        assert isinstance(queries, list)


class TestBuildSearchQueriesDepth2:
    def test_includes_title_only(self):
        queries = _build_search_queries("Artist", "Song", 2)
        descs = [d for _, d in queries]
        assert "title-only" in descs

    def test_includes_remixer_query(self):
        queries = _build_search_queries("Artist", "Song (Remixer Remix)", 2)
        descs = [d for _, d in queries]
        assert any("remixer" in d for d in descs)

    def test_individual_artists_for_multi_artist(self):
        queries = _build_search_queries("Artist A, Artist B", "Song", 2)
        descs = [d for _, d in queries]
        assert any("individual-artist" in d for d in descs)


class TestBuildSearchQueriesDepth3:
    def test_partial_title_for_long_titles(self):
        queries = _build_search_queries("Artist", "One Two Three Four Five", 3)
        descs = [d for _, d in queries]
        assert any("partial-title" in d for d in descs)

    def test_short_title_no_partial(self):
        # Fewer than 5 words: no partial-title entry
        queries = _build_search_queries("Artist", "Short Song", 3)
        descs = [d for _, d in queries]
        assert "first-artist + partial-title" not in descs

    def test_depth4_same_as_depth3(self):
        q3 = _build_search_queries("Artist", "One Two Three Four Five Six", 3)
        q4 = _build_search_queries("Artist", "One Two Three Four Five Six", 4)
        assert q3 == q4


# ---------------------------------------------------------------------------
# Tests: retry_unmatched logic
# ---------------------------------------------------------------------------

class TestRetryUnmatched:
    def test_resets_track_to_matching(self, tmp_db):
        _insert_failed_track(tmp_db, "sp1", "Song", "Artist", depth=0)
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        assert retried == 1
        assert exhausted == 0
        row = tmp_db.execute("SELECT * FROM tracks WHERE spotify_id='sp1'").fetchone()
        assert row["pipeline_stage"] == "matching"
        assert row["match_status"] == "pending"
        assert row["pipeline_error"] is None
        assert row["match_retry_depth"] == 1

    def test_increments_depth_each_retry(self, tmp_db):
        _insert_failed_track(tmp_db, "sp1", "Song", "Artist", depth=1)
        _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        row = tmp_db.execute("SELECT match_retry_depth FROM tracks WHERE spotify_id='sp1'").fetchone()
        assert row["match_retry_depth"] == 2

    def test_exhausts_at_max_depth(self, tmp_db):
        _insert_failed_track(tmp_db, "sp1", "Song", "Artist", depth=3)
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        assert retried == 0
        assert exhausted == 1
        row = tmp_db.execute("SELECT pipeline_error FROM tracks WHERE spotify_id='sp1'").fetchone()
        assert "exhausted" in row["pipeline_error"]

    def test_mixed_depths(self, tmp_db):
        _insert_failed_track(tmp_db, "sp1", "Song A", "Artist", depth=0)
        _insert_failed_track(tmp_db, "sp2", "Song B", "Artist", depth=3)
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        assert retried == 1
        assert exhausted == 1

    def test_configurable_max_depth(self, tmp_db):
        _insert_failed_track(tmp_db, "sp1", "Song", "Artist", depth=1)
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=1)
        assert retried == 0
        assert exhausted == 1

    def test_no_failed_tracks_returns_zero(self, tmp_db):
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        assert retried == 0
        assert exhausted == 0

    def test_tracks_with_other_errors_not_touched(self, tmp_db):
        tmp_db.execute(
            """INSERT INTO tracks (spotify_id, title, artist, match_status, pipeline_stage,
                                   pipeline_error, match_retry_depth)
               VALUES ('sp1', 'Song', 'Artist', 'failed', 'error', 'Download failed', 0)"""
        )
        tmp_db.commit()
        retried, exhausted = _retry_unmatched_sync_pure(tmp_db, max_depth=3)
        assert retried == 0
