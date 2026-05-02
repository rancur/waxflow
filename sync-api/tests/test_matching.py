"""Tests for match-review endpoints."""
from tests.conftest import insert_track


# ---------------------------------------------------------------------------
# GET /api/matching/review
# ---------------------------------------------------------------------------

def test_review_no_mismatched_tracks(client):
    resp = client.get("/api/matching/review")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tracks"] == []
    assert data["total"] == 0
    assert data["stats"]["total_mismatched"] == 0


def test_review_returns_mismatched_tracks(client):
    insert_track("sp1", match_status="mismatched", tidal_id="tid123")
    insert_track("sp2", match_status="matched")  # should not appear

    resp = client.get("/api/matching/review")
    data = resp.json()
    assert data["total"] == 1
    assert data["tracks"][0]["track"]["tidal_id"] == "tid123"
    assert data["stats"]["total_mismatched"] == 1


def test_review_includes_comparison_fields(client):
    insert_track("sp1", match_status="mismatched", duration_ms=180000)

    resp = client.get("/api/matching/review")
    comparison = resp.json()["tracks"][0]
    assert "spotify_title" in comparison
    assert "spotify_artist" in comparison
    assert "match_confidence" in comparison
    assert "title_similarity" in comparison
    assert "artist_similarity" in comparison


# ---------------------------------------------------------------------------
# POST /api/matching/{track_id}/approve
# ---------------------------------------------------------------------------

def test_approve_match_advances_to_downloading(client):
    tid = insert_track("sp1", match_status="mismatched", tidal_id="tid1")

    resp = client.post(f"/api/matching/{tid}/approve")
    assert resp.status_code == 200
    data = resp.json()
    assert data["match_status"] == "matched"
    assert data["pipeline_stage"] == "downloading"


def test_approve_match_with_complete_download_advances_to_verifying(client):
    tid = insert_track(
        "sp1",
        match_status="mismatched",
        download_status="complete",
        verify_status="pending",
    )

    resp = client.post(f"/api/matching/{tid}/approve")
    assert resp.status_code == 200
    assert resp.json()["pipeline_stage"] == "verifying"


def test_approve_match_with_complete_download_and_verify_advances_to_organizing(client):
    tid = insert_track(
        "sp1",
        match_status="mismatched",
        download_status="complete",
        verify_status="pass",
    )

    resp = client.post(f"/api/matching/{tid}/approve")
    assert resp.status_code == 200
    assert resp.json()["pipeline_stage"] == "organizing"


def test_approve_match_not_found(client):
    resp = client.post("/api/matching/99999/approve")
    assert resp.status_code == 404


def test_approve_match_invalid_status(client):
    tid = insert_track("sp1", match_status="matched")  # already matched
    resp = client.post(f"/api/matching/{tid}/approve")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/matching/{track_id}/reject
# ---------------------------------------------------------------------------

def test_reject_match_resets_to_new(client):
    tid = insert_track(
        "sp1",
        match_status="mismatched",
        tidal_id="tid1",
        pipeline_stage="downloading",
    )

    resp = client.post(f"/api/matching/{tid}/reject")
    assert resp.status_code == 200
    data = resp.json()
    assert data["match_status"] == "failed"
    assert data["pipeline_stage"] == "new"
    assert data["tidal_id"] is None
    assert data["match_confidence"] is None


def test_reject_match_not_found(client):
    resp = client.post("/api/matching/99999/reject")
    assert resp.status_code == 404


def test_reject_match_logs_fallback_attempt(client):
    import db
    tid = insert_track("sp1", match_status="mismatched")
    client.post(f"/api/matching/{tid}/reject")

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT * FROM fallback_attempts WHERE track_id = ?", (tid,)
        ).fetchone()
    assert row is not None
    assert row["status"] == "rejected"


# ---------------------------------------------------------------------------
# POST /api/matching/{track_id}/skip
# ---------------------------------------------------------------------------

def test_skip_match_returns_skipped(client):
    tid = insert_track("sp1", match_status="mismatched")
    resp = client.post(f"/api/matching/{tid}/skip")
    assert resp.status_code == 200
    assert resp.json()["status"] == "skipped"
    assert resp.json()["track_id"] == tid


# ---------------------------------------------------------------------------
# POST /api/matching/{track_id}/manual
# ---------------------------------------------------------------------------

def test_manual_match_with_tidal_id(client):
    tid = insert_track("sp1")

    resp = client.post(f"/api/matching/{tid}/manual", json={"tidal_id": "tidal999"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["tidal_id"] == "tidal999"
    assert data["match_status"] == "manual"
    assert data["match_source"] == "manual"
    assert data["match_confidence"] == 1.0
    assert data["pipeline_stage"] == "downloading"


def test_manual_match_with_file_path(client):
    tid = insert_track("sp1")

    resp = client.post(
        f"/api/matching/{tid}/manual",
        json={"file_path": "/music/track.flac"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["file_path"] == "/music/track.flac"
    assert data["download_status"] == "complete"
    assert data["pipeline_stage"] == "verifying"


def test_manual_match_no_params_returns_400(client):
    tid = insert_track("sp1")
    resp = client.post(f"/api/matching/{tid}/manual", json={})
    assert resp.status_code == 400


def test_manual_match_not_found(client):
    resp = client.post("/api/matching/99999/manual", json={"tidal_id": "x"})
    assert resp.status_code == 404
