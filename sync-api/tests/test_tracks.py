"""Tests for /api/tracks routes."""

from tests.conftest import make_track, open_db


# ---------------------------------------------------------------------------
# List tracks
# ---------------------------------------------------------------------------

def test_list_tracks_empty(client):
    r = client.get("/api/tracks")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["tracks"] == []
    assert body["page"] == 1
    assert body["pages"] == 1


def test_list_tracks_returns_seeded_data(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", title="Alpha")
    make_track(conn, spotify_id="sp2", title="Beta")
    conn.close()

    r = client.get("/api/tracks")
    body = r.json()
    assert body["total"] == 2
    assert len(body["tracks"]) == 2


def test_list_tracks_filter_by_pipeline_stage(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", pipeline_stage="new")
    make_track(conn, spotify_id="sp2", pipeline_stage="complete")
    make_track(conn, spotify_id="sp3", pipeline_stage="complete")
    conn.close()

    r = client.get("/api/tracks?pipeline_stage=complete")
    assert r.json()["total"] == 2

    r2 = client.get("/api/tracks?pipeline_stage=new")
    assert r2.json()["total"] == 1


def test_list_tracks_search_by_title(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", title="Bohemian Rhapsody")
    make_track(conn, spotify_id="sp2", title="Hotel California")
    conn.close()

    r = client.get("/api/tracks?search=bohemian")
    assert r.json()["total"] == 1
    assert r.json()["tracks"][0]["title"] == "Bohemian Rhapsody"


def test_list_tracks_search_by_artist(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", artist="Queen", title="Don't Stop Me Now")
    make_track(conn, spotify_id="sp2", artist="Eagles", title="Hotel California")
    conn.close()

    r = client.get("/api/tracks?search=Queen")
    assert r.json()["total"] == 1


def test_list_tracks_pagination(client, db_path):
    conn = open_db(db_path)
    for i in range(5):
        make_track(conn, spotify_id=f"sp{i}", title=f"Track {i}")
    conn.close()

    r = client.get("/api/tracks?per_page=2&page=1")
    body = r.json()
    assert body["total"] == 5
    assert body["pages"] == 3
    assert len(body["tracks"]) == 2

    r2 = client.get("/api/tracks?per_page=2&page=3")
    assert len(r2.json()["tracks"]) == 1


def test_list_tracks_filter_by_match_status(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", match_status="matched")
    make_track(conn, spotify_id="sp2", match_status="failed")
    conn.close()

    r = client.get("/api/tracks?status=matched")
    assert r.json()["total"] == 1
    assert r.json()["tracks"][0]["match_status"] == "matched"


# ---------------------------------------------------------------------------
# Parity
# ---------------------------------------------------------------------------

def test_parity_empty_db(client):
    r = client.get("/api/tracks/parity")
    assert r.status_code == 200
    body = r.json()
    assert body["spotify_total"] == 0
    assert body["parity_pct"] == 0.0


def test_parity_calculates_percentage(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", lexicon_status="synced")
    make_track(conn, spotify_id="sp2", lexicon_status="synced")
    make_track(conn, spotify_id="sp3", lexicon_status="pending")
    make_track(conn, spotify_id="sp4", lexicon_status="pending")
    conn.close()

    r = client.get("/api/tracks/parity")
    body = r.json()
    assert body["spotify_total"] == 4
    assert body["lexicon_synced"] == 2
    assert body["parity_pct"] == 50.0


def test_parity_full_sync(client, db_path):
    conn = open_db(db_path)
    for i in range(3):
        make_track(conn, spotify_id=f"sp{i}", lexicon_status="synced")
    conn.close()

    r = client.get("/api/tracks/parity")
    assert r.json()["parity_pct"] == 100.0


# ---------------------------------------------------------------------------
# Get single track
# ---------------------------------------------------------------------------

def test_get_track_by_id(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", title="My Track", artist="My Artist")
    conn.close()

    r = client.get(f"/api/tracks/{track_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == track_id
    assert body["title"] == "My Track"
    assert body["artist"] == "My Artist"


def test_get_track_not_found(client):
    r = client.get("/api/tracks/99999")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Update track
# ---------------------------------------------------------------------------

def test_update_track_notes(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1")
    conn.close()

    r = client.patch(f"/api/tracks/{track_id}", json={"notes": "needs review"})
    assert r.status_code == 200
    assert r.json()["notes"] == "needs review"


def test_update_track_is_protected(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1")
    conn.close()

    r = client.patch(f"/api/tracks/{track_id}", json={"is_protected": True})
    assert r.status_code == 200
    assert r.json()["is_protected"] is True


def test_update_track_not_found(client):
    r = client.patch("/api/tracks/99999", json={"notes": "nope"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

def test_retry_track_resets_pipeline(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(
        conn,
        spotify_id="sp1",
        pipeline_stage="error",
        pipeline_error="No Tidal match found",
        match_status="failed",
        download_attempts=2,
    )
    conn.close()

    r = client.post(f"/api/tracks/{track_id}/retry")
    assert r.status_code == 200
    body = r.json()
    assert body["pipeline_stage"] == "new"
    assert body["pipeline_error"] is None
    assert body["match_status"] == "pending"
    assert body["download_attempts"] == 0


def test_retry_track_not_found(client):
    r = client.post("/api/tracks/99999/retry")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Ignore / unignore
# ---------------------------------------------------------------------------

def test_ignore_track(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", pipeline_stage="new")
    conn.close()

    r = client.post(f"/api/tracks/{track_id}/ignore")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    r2 = client.get(f"/api/tracks/{track_id}")
    assert r2.json()["pipeline_stage"] == "ignored"
    assert r2.json()["is_protected"] is True


def test_unignore_track(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", pipeline_stage="ignored", is_protected=1)
    conn.close()

    r = client.post(f"/api/tracks/{track_id}/unignore")
    assert r.status_code == 200

    r2 = client.get(f"/api/tracks/{track_id}")
    body = r2.json()
    assert body["pipeline_stage"] == "new"
    assert body["is_protected"] is False


def test_ignore_track_not_found(client):
    r = client.post("/api/tracks/99999/ignore")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Bulk ignore
# ---------------------------------------------------------------------------

def test_bulk_ignore_tracks(client, db_path):
    conn = open_db(db_path)
    id1 = make_track(conn, spotify_id="sp1")
    id2 = make_track(conn, spotify_id="sp2")
    id3 = make_track(conn, spotify_id="sp3")
    conn.close()

    r = client.post("/api/tracks/bulk-ignore", json=[id1, id2])
    assert r.status_code == 200
    assert r.json()["count"] == 2

    r2 = client.get(f"/api/tracks/{id1}")
    assert r2.json()["pipeline_stage"] == "ignored"

    r3 = client.get(f"/api/tracks/{id3}")
    assert r3.json()["pipeline_stage"] == "new"


# ---------------------------------------------------------------------------
# Error tracks
# ---------------------------------------------------------------------------

def test_get_errors_empty(client):
    r = client.get("/api/tracks/errors")
    assert r.status_code == 200
    body = r.json()
    assert body["total_errors"] == 0
    assert body["total_ignored"] == 0


def test_get_errors_categorizes_no_tidal_match(client, db_path):
    conn = open_db(db_path)
    make_track(
        conn,
        spotify_id="sp1",
        pipeline_stage="error",
        pipeline_error="No Tidal match found",
        match_status="failed",
    )
    conn.close()

    r = client.get("/api/tracks/errors")
    body = r.json()
    assert body["total_errors"] == 1
    assert len(body["categories"]["no_tidal_match"]) == 1


def test_get_errors_categorizes_not_lossless(client, db_path):
    conn = open_db(db_path)
    make_track(
        conn,
        spotify_id="sp1",
        pipeline_stage="error",
        verify_codec="aac",
        match_status="failed",
        pipeline_error="Not lossless",
    )
    conn.close()

    r = client.get("/api/tracks/errors")
    body = r.json()
    assert len(body["categories"]["not_lossless"]) == 1


def test_get_errors_includes_ignored(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", pipeline_stage="ignored")
    conn.close()

    r = client.get("/api/tracks/errors")
    body = r.json()
    assert body["total_ignored"] == 1
    assert len(body["ignored"]) == 1
