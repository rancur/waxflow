"""Tests for /api/admin and /api/settings routes."""

from tests.conftest import make_track, open_db


def test_health_returns_ok(client):
    r = client.get("/api/admin/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["uptime_seconds"] >= 0


def test_get_sync_mode_default(client):
    r = client.get("/api/admin/sync-mode")
    assert r.status_code == 200
    assert r.json()["sync_mode"] == "scan"


def test_set_sync_mode_full(client):
    r = client.post("/api/admin/sync-mode", json={"mode": "full"})
    assert r.status_code == 200
    body = r.json()
    assert body["sync_mode"] == "full"
    assert body["tracks_queued"] == 0

    r2 = client.get("/api/admin/sync-mode")
    assert r2.json()["sync_mode"] == "full"


def test_set_sync_mode_queues_waiting_tracks(client, db_path):
    conn = open_db(db_path)
    make_track(conn, spotify_id="sp1", pipeline_stage="waiting")
    make_track(conn, spotify_id="sp2", pipeline_stage="waiting")
    make_track(conn, spotify_id="sp3", pipeline_stage="new")
    conn.close()

    r = client.post("/api/admin/sync-mode", json={"mode": "full"})
    assert r.json()["tracks_queued"] == 2


def test_set_sync_mode_invalid_value(client):
    r = client.post("/api/admin/sync-mode", json={"mode": "turbo"})
    assert r.status_code == 400


def test_get_settings_returns_dict(client):
    r = client.get("/api/settings")
    assert r.status_code == 200
    body = r.json()
    assert "settings" in body
    assert isinstance(body["settings"], dict)
    assert "sync_mode" in body["settings"]


def test_sensitive_tokens_excluded_from_settings(client, db_path):
    conn = open_db(db_path)
    for key in ("spotify_access_token", "spotify_refresh_token", "spotify_token_expiry"):
        conn.execute(
            "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
            (key, "supersecret"),
        )
    conn.commit()
    conn.close()

    r = client.get("/api/settings")
    settings = r.json()["settings"]
    assert "spotify_access_token" not in settings
    assert "spotify_refresh_token" not in settings
    assert "spotify_token_expiry" not in settings


def test_update_settings(client):
    r = client.patch("/api/settings", json={"settings": {"spotify_poll_interval_seconds": "600"}})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "spotify_poll_interval_seconds" in body["updated"]

    r2 = client.get("/api/settings")
    assert r2.json()["settings"]["spotify_poll_interval_seconds"] == "600"


def test_version_endpoint(client):
    r = client.get("/api/admin/version")
    assert r.status_code == 200
    body = r.json()
    assert "version" in body
    assert "git_sha" in body
