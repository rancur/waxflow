"""Tests for /api/matching routes."""

from tests.conftest import make_track, open_db


# ---------------------------------------------------------------------------
# Review
# ---------------------------------------------------------------------------

def test_review_empty(client):
    r = client.get("/api/matching/review")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["tracks"] == []
    assert body["stats"]["total_mismatched"] == 0


def test_review_returns_mismatched_tracks(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", match_status="mismatched", match_confidence=0.82)
    make_track(conn, spotify_id="sp2", match_status="matched")
    conn.close()

    r = client.get("/api/matching/review")
    body = r.json()
    assert body["total"] == 1
    assert body["tracks"][0]["match_confidence"] == 0.82
    assert body["stats"]["total_mismatched"] == 1


# ---------------------------------------------------------------------------
# Approve
# ---------------------------------------------------------------------------

def test_approve_advances_to_downloading(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(
        conn,
        spotify_id="sp1",
        match_status="mismatched",
        download_status="pending",
    )
    conn.close()

    r = client.post(f"/api/matching/{track_id}/approve")
    assert r.status_code == 200
    body = r.json()
    assert body["match_status"] == "matched"
    assert body["pipeline_stage"] == "downloading"


def test_approve_with_complete_download_advances_to_verifying(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(
        conn,
        spotify_id="sp1",
        match_status="mismatched",
        download_status="complete",
        verify_status="pending",
    )
    conn.close()

    r = client.post(f"/api/matching/{track_id}/approve")
    assert r.status_code == 200
    assert r.json()["pipeline_stage"] == "verifying"


def test_approve_with_complete_download_and_verify_advances_to_organizing(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(
        conn,
        spotify_id="sp1",
        match_status="mismatched",
        download_status="complete",
        verify_status="pass",
    )
    conn.close()

    r = client.post(f"/api/matching/{track_id}/approve")
    assert r.status_code == 200
    assert r.json()["pipeline_stage"] == "organizing"


def test_approve_not_found(client):
    r = client.post("/api/matching/99999/approve")
    assert r.status_code == 404


def test_approve_invalid_status_rejected(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="failed")
    conn.close()

    r = client.post(f"/api/matching/{track_id}/approve")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Reject
# ---------------------------------------------------------------------------

def test_reject_resets_track_to_new(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(
        conn,
        spotify_id="sp1",
        match_status="mismatched",
        tidal_id="tidal_123",
        match_confidence=0.85,
        pipeline_stage="matching",
    )
    conn.close()

    r = client.post(f"/api/matching/{track_id}/reject")
    assert r.status_code == 200
    body = r.json()
    assert body["match_status"] == "failed"
    assert body["pipeline_stage"] == "new"
    assert body["tidal_id"] is None
    assert body["match_confidence"] is None


def test_reject_records_fallback_attempt(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="mismatched")
    conn.close()

    client.post(f"/api/matching/{track_id}/reject")

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT * FROM fallback_attempts WHERE track_id = ?", (track_id,)
    ).fetchone()
    conn.close()
    assert row is not None
    assert row["source"] == "manual_reject"
    assert row["status"] == "rejected"


def test_reject_not_found(client):
    r = client.post("/api/matching/99999/reject")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Skip
# ---------------------------------------------------------------------------

def test_skip_returns_skipped_status(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="mismatched")
    conn.close()

    r = client.post(f"/api/matching/{track_id}/skip")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "skipped"
    assert body["track_id"] == track_id


def test_skip_does_not_change_db_state(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="mismatched")
    conn.close()

    client.post(f"/api/matching/{track_id}/skip")

    conn = open_db(db_path)
    row = conn.execute("SELECT match_status FROM tracks WHERE id = ?", (track_id,)).fetchone()
    conn.close()
    assert row["match_status"] == "mismatched"


# ---------------------------------------------------------------------------
# Manual match
# ---------------------------------------------------------------------------

def test_manual_match_with_tidal_id(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="pending")
    conn.close()

    r = client.post(
        f"/api/matching/{track_id}/manual",
        json={"tidal_id": "tidal_xyz"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["match_status"] == "manual"
    assert body["tidal_id"] == "tidal_xyz"
    assert body["pipeline_stage"] == "downloading"
    assert body["match_confidence"] == 1.0


def test_manual_match_with_file_path(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1", match_status="pending")
    conn.close()

    r = client.post(
        f"/api/matching/{track_id}/manual",
        json={"file_path": "/music/track.flac"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["match_status"] == "manual"
    assert body["file_path"] == "/music/track.flac"
    assert body["pipeline_stage"] == "verifying"
    assert body["download_status"] == "complete"


def test_manual_match_no_params_returns_400(client, db_path):
    conn = open_db(db_path)
    track_id = make_track(conn, spotify_id="sp1")
    conn.close()

    r = client.post(f"/api/matching/{track_id}/manual", json={})
    assert r.status_code == 400


def test_manual_match_not_found(client):
    r = client.post("/api/matching/99999/manual", json={"tidal_id": "x"})
    assert r.status_code == 404
