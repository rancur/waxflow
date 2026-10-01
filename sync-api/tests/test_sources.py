"""Tests for the acquire-source order/toggle API and its migration (2.20.0).

Covers:
  * migration: a fresh install AND a pre-2.20 database both end up Soulseek-first;
    an order the user chose is never overwritten by a restart.
  * GET /api/sources shape + per-source availability (Soulseek from the worker's
    persisted probe, Tidal from its auth file, "disabled" when switched off).
  * PUT /api/sources validation: unknown names, link-only stores in the order,
    duplicates, bad toggles and "everything off" are all 400s and write nothing.
  * PATCH /api/settings cannot be used to smuggle in an invalid source_priority.
  * the dashboard shows the per-source rows in the configured order.

Self-contained: temp SQLite built by the real init_db, no network.
"""

import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest import mock

SYNC_API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_API_DIR not in sys.path:
    sys.path.insert(0, SYNC_API_DIR)

_TMP = tempfile.mkdtemp(prefix="waxflow-sources-")
os.environ.setdefault("SLS_DB_PATH", os.path.join(_TMP, "default.db"))

import db as db_mod  # noqa: E402
import init_db  # noqa: E402
import sources_config as sc  # noqa: E402
from routes import admin as admin_mod  # noqa: E402
from routes import dashboard as dash_mod  # noqa: E402
from routes import sources as sources_mod  # noqa: E402
from routes import tidal as tidal_mod  # noqa: E402


def _cfg(path: str, key: str):
    conn = sqlite3.connect(path)
    row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else None


def _set(path: str, key: str, value):
    conn = sqlite3.connect(path)
    if value is None:
        conn.execute("DELETE FROM app_config WHERE key = ?", (key,))
    else:
        conn.execute(
            "INSERT INTO app_config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
    conn.commit()
    conn.close()


class _DbCase(unittest.TestCase):
    def setUp(self):
        self.db = os.path.join(tempfile.mkdtemp(dir=_TMP), "sync.db")
        saved = db_mod.DB_PATH
        db_mod.DB_PATH = self.db
        self.addCleanup(lambda: setattr(db_mod, "DB_PATH", saved))
        with mock.patch("builtins.print"):
            init_db.init()


class TestMigration(_DbCase):
    def test_new_install_is_soulseek_first(self):
        self.assertEqual(_cfg(self.db, "source_priority"), "soulseek,tidal")

    def test_pre_220_database_is_migrated_to_soulseek_first(self):
        # A 2.19 database has no source_priority key at all.
        _set(self.db, "source_priority", None)
        with mock.patch("builtins.print"):
            init_db.init()
        self.assertEqual(_cfg(self.db, "source_priority"), "soulseek,tidal")
        conn = sqlite3.connect(self.db)
        n = conn.execute(
            "SELECT COUNT(*) FROM activity_log WHERE event_type='source_priority_migrated'"
        ).fetchone()[0]
        conn.close()
        self.assertGreaterEqual(n, 1)

    def test_user_choice_survives_restart(self):
        _set(self.db, "source_priority", "tidal,soulseek")
        with mock.patch("builtins.print"):
            init_db.init()
        self.assertEqual(_cfg(self.db, "source_priority"), "tidal,soulseek")

    def test_existing_toggles_untouched(self):
        _set(self.db, "soulseek_fallback_enabled", "1")
        _set(self.db, "source_tidal_enabled", "0")
        with mock.patch("builtins.print"):
            init_db.init()
        self.assertEqual(_cfg(self.db, "soulseek_fallback_enabled"), "1")
        self.assertEqual(_cfg(self.db, "source_tidal_enabled"), "0")


class TestValidation(unittest.TestCase):
    def test_parse_is_tolerant_and_complete(self):
        self.assertEqual(sc.parse_priority(None), ["soulseek", "tidal"])
        self.assertEqual(sc.parse_priority("tidal"), ["tidal", "soulseek"])
        self.assertEqual(sc.parse_priority("bogus, TIDAL ,tidal"), ["tidal", "soulseek"])

    def test_validate_accepts_list_and_string(self):
        self.assertEqual(sc.validate_priority(["tidal", "soulseek"]), ["tidal", "soulseek"])
        self.assertEqual(sc.validate_priority("soulseek"), ["soulseek", "tidal"])

    def test_validate_rejects(self):
        for bad in (["napster"], ["soulseek", "soulseek"], ["qobuz"], [], "", 5, [1]):
            with self.subTest(bad=bad), self.assertRaises(sc.SourceConfigError):
                sc.validate_priority(bad)

    def test_validate_enabled(self):
        self.assertEqual(sc.validate_enabled({"tidal": False, "qobuz": "1"}),
                         {"tidal": False, "qobuz": True})
        for bad in ({"napster": True}, {"tidal": "maybe"}, ["tidal"]):
            with self.subTest(bad=bad), self.assertRaises(sc.SourceConfigError):
                sc.validate_enabled(bad)


class _NoNetwork:
    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, *a, **kw):
        raise OSError("no network in tests")


class TestEndpoints(_DbCase):
    @classmethod
    def setUpClass(cls):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app = FastAPI()
        app.include_router(sources_mod.router)
        app.include_router(admin_mod.router)
        app.include_router(dash_mod.router)
        cls.client = TestClient(app)

    def setUp(self):
        super().setUp()
        # A valid Tidal auth file + a fresh Soulseek probe verdict.
        self.auth = os.path.join(os.path.dirname(self.db), "auth.json")
        with open(self.auth, "w") as f:
            json.dump({"token": "t", "expires_at": time.time() + 3600}, f)
        p = mock.patch.object(tidal_mod, "_AUTH_PATHS", [self.auth])
        p.start()
        self.addCleanup(p.stop)
        p2 = mock.patch.object(dash_mod.httpx, "AsyncClient", _NoNetwork)
        p2.start()
        self.addCleanup(p2.stop)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _set(self.db, "soulseek_health", "ok")
        _set(self.db, "soulseek_checked_at", now)

    def _get(self):
        r = self.client.get("/api/sources")
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def test_get_default(self):
        body = self._get()
        self.assertEqual(body["priority"], ["soulseek", "tidal"])
        self.assertEqual(body["active_order"], ["soulseek", "tidal"])
        by = {s["name"]: s for s in body["acquire"]}
        self.assertEqual(by["soulseek"]["status"], "ok")
        self.assertEqual(by["soulseek"]["position"], 1)
        self.assertEqual(by["tidal"]["status"], "ok")
        self.assertEqual({s["name"] for s in body["links"]}, {"qobuz", "beatport", "bandcamp"})

    def test_tidal_expired_token_reports_error(self):
        with open(self.auth, "w") as f:
            json.dump({"token": "t", "expires_at": time.time() - 7200}, f)
        by = {s["name"]: s for s in self._get()["acquire"]}
        self.assertEqual(by["tidal"]["status"], "error")
        self.assertIn("expired", by["tidal"]["detail"])

    def test_put_reorder_and_disable_tidal(self):
        r = self.client.put("/api/sources", json={"priority": ["tidal", "soulseek"]})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(_cfg(self.db, "source_priority"), "tidal,soulseek")

        r = self.client.put("/api/sources", json={"priority": "soulseek,tidal",
                                                  "enabled": {"tidal": False}})
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(_cfg(self.db, "source_priority"), "soulseek,tidal")
        self.assertEqual(_cfg(self.db, "source_tidal_enabled"), "0")
        self.assertEqual(body["active_order"], ["soulseek"])
        by = {s["name"]: s for s in body["acquire"]}
        self.assertEqual(by["tidal"]["status"], "disabled")

        # The dashboard shows Tidal as disabled, not as broken, in source order.
        dash = self.client.get("/api/dashboard").json()
        names = [s["name"] for s in dash["services"]]
        self.assertLess(names.index("soulseek"), names.index("tidal"))
        self.assertEqual(next(s for s in dash["services"] if s["name"] == "tidal")["status"],
                         "disabled")

    def test_put_rejects_bad_input_and_writes_nothing(self):
        before = _cfg(self.db, "source_priority")
        for body in (
            {"priority": ["soulseek", "napster"]},
            {"priority": ["qobuz", "soulseek"]},
            {"priority": ["tidal", "tidal"]},
            {"enabled": {"napster": True}},
            {"enabled": {"soulseek": False, "tidal": False}},
            {},
        ):
            with self.subTest(body=body):
                r = self.client.put("/api/sources", json=body)
                self.assertEqual(r.status_code, 400, r.text)
        self.assertEqual(_cfg(self.db, "source_priority"), before)
        self.assertNotEqual(_cfg(self.db, "source_tidal_enabled"), "0")

    def test_link_toggle(self):
        r = self.client.put("/api/sources", json={"enabled": {"beatport": False}})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(_cfg(self.db, "source_beatport_enabled"), "0")
        by = {s["name"]: s for s in r.json()["links"]}
        self.assertFalse(by["beatport"]["enabled"])

    def test_patch_settings_validates_source_priority(self):
        r = self.client.patch("/api/settings", json={"settings": {"source_priority": "napster"}})
        self.assertEqual(r.status_code, 400, r.text)
        r = self.client.patch("/api/settings", json={"settings": {"source_priority": "tidal"}})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(_cfg(self.db, "source_priority"), "tidal,soulseek")


if __name__ == "__main__":
    unittest.main()
