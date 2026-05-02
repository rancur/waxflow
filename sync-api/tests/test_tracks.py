"""Tests for track-management endpoints."""
import pytest
from tests.conftest import insert_track


# ---------------------------------------------------------------------------
# GET /api/tracks
# ---------------------------------------------------------------------------

def test_list_tracks_empty(client):
    resp = client.get("/api/tracks")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tracks"] == []
    assert data["total"] == 0
    assert data["page"] == 1
    assert data["pages"] == 1


def test_list_tracks_returns_inserted_rows(client):
    insert_track("sp1", title="Song A", artist="Artist A")
    insert_track("sp2", title="Song B", artist="Artist B")

    resp = client.get("/api/tracks")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert len(data["tracks"]) == 2


def test_list_tracks_pagination(client):
    for i in range(5):
        insert_track(f"sp{i}", title=f"Track {i}")

    resp = client.get("/api/tracks?page=1&per_page=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["tracks"]) == 2
    assert data["total"] == 5
    assert data["pages"] == 3


def test_list_tracks_filter_by_match_status(client):
    insert_track("sp1", match_status="matched")
    insert_track("sp2", match_status="failed")

    resp = client.get("/api/tracks?status=matched")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["tracks"][0]["match_status"] == "matched"


def test_list_tracks_filter_by_pipeline_stage(client):
    insert_track("sp1", pipeline_stage="error")
    insert_track("sp2", pipeline_stage="new")

    resp = client.get("/api/tracks?pipeline_stage=error")
    data = resp.json()
    assert data["total"] == 1
    assert data["tracks"][0]["pipeline_stage"] == "error"


def test_list_tracks_search(client):
    insert_track("sp1", title="Bohemian Rhapsody", artist="Queen")
    insert_track("sp2", title="Stairway to Heaven", artist="Led Zeppelin")

    resp = client.get("/api/tracks?search=Bohemian")
    data = resp.json()
    assert data["total"] == 1
    assert data["tracks"][0]["title"] == "Bohemian Rhapsody"


def test_list_tracks_sort_asc(client):
    insert_track("sp1", title="Zebra")
    insert_track("sp2", title="Apple")

    resp = client.get("/api/tracks?sort_by=title&sort_dir=asc")
    data = resp.json()
    titles = [t["title"] for t in data["tracks"]]
    assert titles == sorted(titles)


# ---------------------------------------------------------------------------
# GET /api/tracks/parity
# ---------------------------------------------------------------------------

def test_parity_empty_db(client):
    resp = client.get("/api/tracks/parity")
    assert resp.status_code == 200
    data = resp.json()
    assert data["spotify_total"] == 0
    assert data["parity_pct"] == 0.0


def test_parity_with_synced_tracks(client):
    insert_track("sp1", lexicon_status="synced")
    insert_track("sp2", lexicon_status="synced")
    insert_track("sp3", lexicon_status="pending")

    resp = client.get("/api/tracks/parity")
    data = resp.json()
    assert data["spotify_total"] == 3
    assert data["lexicon_synced"] == 2
    assert round(data["parity_pct"], 2) == round(2 / 3 * 100, 2)


# ---------------------------------------------------------------------------
# GET /api/tracks/errors
# ---------------------------------------------------------------------------

def test_get_error_tracks_empty(client):
    resp = client.get("/api/tracks/errors")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_errors"] == 0
    assert data["total_ignored"] == 0


def test_get_error_tracks_categorises_no_tidal_match(client):
    insert_track(
        "sp1",
        pipeline_stage="error",
        title="No Match Track",
    )
    import db
    with db.get_db() as conn:
        conn.execute(
            "UPDATE tracks SET pipeline_error = 'No Tidal match found' WHERE spotify_id = 'sp1'"
        )

    resp = client.get("/api/tracks/errors")
    data = resp.json()
    assert data["total_errors"] == 1
    assert len(data["categories"]["no_tidal_match"]) == 1


# ---------------------------------------------------------------------------
# GET /api/tracks/{track_id}
# ---------------------------------------------------------------------------

def test_get_track(client):
    tid = insert_track("sp1", title="My Track")
    resp = client.get(f"/api/tracks/{tid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "My Track"


def test_get_track_not_found(client):
    resp = client.get("/api/tracks/99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/tracks/{track_id}/activity
# ---------------------------------------------------------------------------

def test_get_track_activity_empty(client):
    tid = insert_track("sp1")
    resp = client.get(f"/api/tracks/{tid}/activity")
    assert resp.status_code == 200
    data = resp.json()
    assert data["track_id"] == tid
    assert data["activity"] == []


def test_get_track_activity_not_found(client):
    resp = client.get("/api/tracks/99999/activity")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PATCH /api/tracks/{track_id}
# ---------------------------------------------------------------------------

def test_update_track_notes(client):
    tid = insert_track("sp1")
    resp = client.patch(f"/api/tracks/{tid}", json={"notes": "manual check needed"})
    assert resp.status_code == 200
    assert resp.json()["notes"] == "manual check needed"


def test_update_track_not_found(client):
    resp = client.patch("/api/tracks/99999", json={"notes": "x"})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/tracks/{track_id}/ignore and /unignore
# ---------------------------------------------------------------------------

def test_ignore_track(client):
    tid = insert_track("sp1")
    resp = client.post(f"/api/tracks/{tid}/ignore")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    track_resp = client.get(f"/api/tracks/{tid}")
    assert track_resp.json()["pipeline_stage"] == "ignored"
    assert track_resp.json()["is_protected"] is True


def test_ignore_track_not_found(client):
    resp = client.post("/api/tracks/99999/ignore")
    assert resp.status_code == 404


def test_unignore_track(client):
    tid = insert_track("sp1", pipeline_stage="ignored")
    resp = client.post(f"/api/tracks/{tid}/unignore")
    assert resp.status_code == 200

    track_resp = client.get(f"/api/tracks/{tid}")
    assert track_resp.json()["pipeline_stage"] == "new"


def test_unignore_track_not_found(client):
    resp = client.post("/api/tracks/99999/unignore")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/tracks/bulk-ignore
# ---------------------------------------------------------------------------

def test_bulk_ignore(client):
    id1 = insert_track("sp1")
    id2 = insert_track("sp2")

    resp = client.post("/api/tracks/bulk-ignore", json=[id1, id2])
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2

    for tid in (id1, id2):
        t = client.get(f"/api/tracks/{tid}").json()
        assert t["pipeline_stage"] == "ignored"


# ---------------------------------------------------------------------------
# POST /api/tracks/{track_id}/retry
# ---------------------------------------------------------------------------

def test_retry_track_resets_pipeline(client):
    tid = insert_track(
        "sp1",
        match_status="failed",
        pipeline_stage="error",
        download_status="failed",
        verify_status="fail",
        lexicon_status="error",
    )

    resp = client.post(f"/api/tracks/{tid}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pipeline_stage"] == "new"
    assert data["match_status"] == "pending"
    assert data["download_status"] == "pending"
    assert data["verify_status"] == "pending"
    assert data["lexicon_status"] == "pending"
    assert data["download_attempts"] == 0
    assert data["pipeline_error"] is None


def test_retry_track_not_found(client):
    resp = client.post("/api/tracks/99999/retry")
    assert resp.status_code == 404
