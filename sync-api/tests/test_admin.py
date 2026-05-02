"""Tests for admin and settings endpoints."""


def test_health_check(client):
    resp = client.get("/api/admin/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["database"] == "ok"
    assert data["uptime_seconds"] >= 0


def test_get_settings_returns_defaults(client):
    resp = client.get("/api/settings")
    assert resp.status_code == 200
    data = resp.json()
    assert "settings" in data
    settings = data["settings"]
    # Default config values seeded by init_db
    assert "sync_mode" in settings
    assert "max_concurrent_downloads" in settings
    # Sensitive keys must be excluded
    assert "spotify_access_token" not in settings
    assert "spotify_refresh_token" not in settings


def test_update_settings(client):
    resp = client.patch("/api/settings", json={"settings": {"sync_mode": "full"}})
    assert resp.status_code == 200
    data = resp.json()
    assert "sync_mode" in data["updated"]

    # Verify persisted
    resp2 = client.get("/api/settings")
    assert resp2.json()["settings"]["sync_mode"] == "full"


def test_update_settings_sensitive_key_ignored(client):
    """Sensitive keys in the PATCH body must be silently dropped."""
    resp = client.patch(
        "/api/settings",
        json={"settings": {"spotify_access_token": "evil", "sync_mode": "scan"}},
    )
    assert resp.status_code == 200
    resp2 = client.get("/api/settings")
    assert "spotify_access_token" not in resp2.json()["settings"]


def test_get_sync_mode_default(client):
    resp = client.get("/api/admin/sync-mode")
    assert resp.status_code == 200
    assert resp.json()["sync_mode"] in ("scan", "full")


def test_set_sync_mode_valid(client):
    resp = client.post("/api/admin/sync-mode", json={"mode": "full"})
    assert resp.status_code == 200
    assert resp.json()["sync_mode"] == "full"


def test_set_sync_mode_invalid(client):
    resp = client.post("/api/admin/sync-mode", json={"mode": "turbo"})
    assert resp.status_code == 400


def test_analyze_stats(client):
    resp = client.get("/api/admin/analyze-stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "enabled" in data
    assert "total_processed" in data
    assert "interval_seconds" in data
    assert "batch_size" in data
    assert "events_last_24h" in data


def test_export_json_empty(client):
    resp = client.get("/api/admin/export")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tracks"] == []
    assert data["total"] == 0


def test_export_csv_empty(client):
    resp = client.get("/api/admin/export?format=csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().splitlines()
    # Only the header row when there are no tracks
    assert len(lines) == 1
    assert "spotify_id" in lines[0]


def test_version_endpoint(client):
    resp = client.get("/api/admin/version")
    assert resp.status_code == 200
    # May be None if VERSION file absent in CI, but shape must match schema
    data = resp.json()
    assert "version" in data
    assert "git_sha" in data


def test_rebuild_playlists(client):
    resp = client.post("/api/admin/rebuild-playlists")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_list_backups_empty(client):
    resp = client.get("/api/admin/backups")
    assert resp.status_code == 200
    assert resp.json()["backups"] == []
