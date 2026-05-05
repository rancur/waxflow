"""Tests for parity drop alert logic in tasks/parity_check.py."""

import time
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers: fake config store so tests don't need a real SQLite DB
# ---------------------------------------------------------------------------

def make_config_store(**initial):
    """Return get_config / set_config pair backed by a dict."""
    store = dict(initial)

    def get_config(_db_path, key):
        return store.get(key)

    def set_config(_db_path, key, value):
        store[key] = value

    return store, get_config, set_config


# ---------------------------------------------------------------------------
# _check_parity_drop_alert
# ---------------------------------------------------------------------------

class TestCheckParityDropAlert:
    """Unit tests for the alert-gating logic (no webhook, no DB)."""

    def _call(self, get_config_fn, set_config_fn, **kwargs):
        from tasks.parity_check import _check_parity_drop_alert
        defaults = dict(
            db_path="fake.db",
            parity_pct=90.0,
            prev_pct=96.0,
            synced=900,
            total=1000,
            errors=10,
            in_progress=5,
            prev_synced=960,
            prev_total=1000,
            prev_errors=5,
            spotify_total=1050,
            prev_spotify=1000,
        )
        defaults.update(kwargs)
        with (
            patch("tasks.parity_check.get_config", get_config_fn),
            patch("tasks.parity_check.set_config", set_config_fn),
            patch("tasks.parity_check.log_activity"),
            patch("tasks.parity_check._notify_parity_drop") as mock_notify,
        ):
            _check_parity_drop_alert(**defaults)
        return mock_notify

    def test_alert_fires_when_below_threshold_and_dropped(self):
        store, gc, sc = make_config_store(
            parity_alert_threshold="95.0",
            parity_alert_cooldown_seconds="1800",
        )
        mock_notify = self._call(gc, sc)
        mock_notify.assert_called_once()

    def test_no_alert_when_parity_above_threshold(self):
        store, gc, sc = make_config_store(parity_alert_threshold="85.0")
        mock_notify = self._call(gc, sc, parity_pct=90.0, prev_pct=92.0)
        mock_notify.assert_not_called()

    def test_no_alert_when_parity_did_not_drop(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        # parity_pct >= prev_pct: not a drop
        mock_notify = self._call(gc, sc, parity_pct=90.0, prev_pct=88.0)
        mock_notify.assert_not_called()

    def test_no_alert_when_parity_unchanged(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, parity_pct=90.0, prev_pct=90.0)
        mock_notify.assert_not_called()

    def test_cooldown_suppresses_second_alert(self):
        recent_ts = str(time.time() - 60)  # 60 s ago, within 1800 s cooldown
        store, gc, sc = make_config_store(
            parity_alert_threshold="95.0",
            parity_alert_cooldown_seconds="1800",
            parity_alert_last_sent_ts=recent_ts,
        )
        mock_notify = self._call(gc, sc)
        mock_notify.assert_not_called()

    def test_alert_fires_after_cooldown_expires(self):
        old_ts = str(time.time() - 3600)  # 1 h ago, cooldown=1800 s
        store, gc, sc = make_config_store(
            parity_alert_threshold="95.0",
            parity_alert_cooldown_seconds="1800",
            parity_alert_last_sent_ts=old_ts,
        )
        mock_notify = self._call(gc, sc)
        mock_notify.assert_called_once()

    def test_last_sent_ts_is_updated_after_alert(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        before = time.time()
        self._call(gc, sc)
        assert "parity_alert_last_sent_ts" in store
        assert float(store["parity_alert_last_sent_ts"]) >= before

    def test_delta_contains_new_errors(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, errors=20, prev_errors=5)
        _, kwargs = mock_notify.call_args
        assert kwargs["delta"]["new_errors"] == 15

    def test_delta_contains_new_liked_songs(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, spotify_total=1100, prev_spotify=1000)
        _, kwargs = mock_notify.call_args
        assert kwargs["delta"]["new_liked_songs"] == 100

    def test_delta_no_liked_songs_when_spotify_unavailable(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, spotify_total=None, prev_spotify=None)
        _, kwargs = mock_notify.call_args
        assert "new_liked_songs" not in kwargs["delta"]

    def test_causes_list_reflects_new_errors(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, errors=15, prev_errors=5, spotify_total=None, prev_spotify=None)
        _, kwargs = mock_notify.call_args
        causes = kwargs["causes"]
        assert any("error" in c for c in causes)

    def test_causes_list_reflects_new_liked_songs(self):
        store, gc, sc = make_config_store(parity_alert_threshold="95.0")
        mock_notify = self._call(gc, sc, spotify_total=1100, prev_spotify=1000, errors=5, prev_errors=5)
        _, kwargs = mock_notify.call_args
        causes = kwargs["causes"]
        assert any("liked" in c for c in causes)

    def test_default_threshold_is_95(self):
        """If no threshold config, default 95.0 should be used."""
        store, gc, sc = make_config_store()  # no threshold set
        mock_notify = self._call(gc, sc, parity_pct=94.0, prev_pct=96.0)
        mock_notify.assert_called_once()

    def test_default_threshold_no_alert_above_95(self):
        store, gc, sc = make_config_store()  # no threshold set
        mock_notify = self._call(gc, sc, parity_pct=96.0, prev_pct=97.0)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# _notify_parity_drop
# ---------------------------------------------------------------------------

class TestNotifyParityDrop:
    """Unit tests for the webhook POST in _notify_parity_drop."""

    def _call(self, webhook_url, **kwargs):
        from tasks.parity_check import _notify_parity_drop
        store = {"webhook_url": webhook_url}

        def gc(_db, key):
            return store.get(key)

        defaults = dict(
            db_path="fake.db",
            parity_pct=90.0,
            prev_pct=96.0,
            threshold=95.0,
            synced=900,
            total=1000,
            errors=10,
            delta={"new_errors": 5, "new_tracks_added": 0, "newly_synced": 0},
            causes=["5 track(s) moved to error state"],
        )
        defaults.update(kwargs)

        with patch("tasks.parity_check.get_config", gc):
            with patch("tasks.parity_check.httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
                mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
                _notify_parity_drop(**defaults)
        return mock_client

    def test_posts_to_webhook_url(self):
        client = self._call("https://hooks.example.com/test")
        client.post.assert_called_once()
        url = client.post.call_args[0][0]
        assert url == "https://hooks.example.com/test"

    def test_payload_contains_event_key(self):
        client = self._call("https://hooks.example.com/test")
        payload = client.post.call_args[1]["json"]
        assert payload["event"] == "parity_drop_alert"

    def test_payload_contains_parity_pct(self):
        client = self._call("https://hooks.example.com/test")
        payload = client.post.call_args[1]["json"]
        assert payload["parity_pct"] == 90.0
        assert payload["prev_pct"] == 96.0
        assert payload["threshold"] == 95.0

    def test_payload_contains_delta_and_causes(self):
        client = self._call("https://hooks.example.com/test")
        payload = client.post.call_args[1]["json"]
        assert "delta" in payload
        assert "causes" in payload

    def test_no_post_when_webhook_url_empty(self):
        client = self._call("")
        client.post.assert_not_called()

    def test_no_post_when_webhook_url_none(self):
        from tasks.parity_check import _notify_parity_drop
        store: dict = {}

        def gc(_db, key):
            return store.get(key)

        with patch("tasks.parity_check.get_config", gc):
            with patch("tasks.parity_check.httpx.Client") as mock_client_cls:
                _notify_parity_drop(
                    db_path="fake.db",
                    parity_pct=90.0,
                    prev_pct=96.0,
                    threshold=95.0,
                    synced=900,
                    total=1000,
                    errors=10,
                    delta={},
                    causes=[],
                )
                mock_client_cls.assert_not_called()

    def test_webhook_failure_does_not_raise(self):
        """A network error must be swallowed so the parity check keeps running."""
        from tasks.parity_check import _notify_parity_drop
        store = {"webhook_url": "https://hooks.example.com/test"}

        def gc(_db, key):
            return store.get(key)

        with patch("tasks.parity_check.get_config", gc):
            with patch("tasks.parity_check.httpx.Client") as mock_client_cls:
                mock_client_cls.return_value.__enter__ = MagicMock(
                    side_effect=Exception("network error")
                )
                mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
                # Should not raise
                _notify_parity_drop(
                    db_path="fake.db",
                    parity_pct=90.0,
                    prev_pct=96.0,
                    threshold=95.0,
                    synced=900,
                    total=1000,
                    errors=10,
                    delta={},
                    causes=[],
                )
