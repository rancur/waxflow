"""Tests for the Soulseek health probe.

The probe's whole job is to tell the truth about a service the API cannot reach
itself. The failure that matters is reporting "ok" when it is not -- so each
distinct real-world state must map to its own status, and a probe that throws must
never take the worker loop down.
"""

import os
import sys
import unittest
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import soulseek_health as sh  # noqa: E402


class _FakeClient:
    def __init__(self, state=None, raises=None, configured=True, base="http://slskd:5030"):
        self._state = state or {}
        self._raises = raises
        self.configured = configured
        self.base = base

    def server_state(self):
        if self._raises:
            raise self._raises
        return self._state


class SoulseekHealthTest(unittest.TestCase):
    def setUp(self):
        self.recorded = {}
        patcher = mock.patch.object(
            sh, "record",
            side_effect=lambda db, status, detail, ok, latency=None: self.recorded.update(
                status=status, detail=detail, ok=ok, latency=latency))
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, *, enabled=True, client=None, build_raises=None):
        fake_sf = mock.MagicMock()
        fake_sf.is_enabled.return_value = enabled
        if build_raises:
            fake_sf.build_client.side_effect = build_raises
        else:
            fake_sf.build_client.return_value = client or _FakeClient()
        # Patch BOTH the package attribute and sys.modules. `from tasks import
        # soulseek_fallback` reads the attribute off the already-imported `tasks`
        # package, so patching sys.modules alone silently does nothing as soon as
        # any other test in the run has imported the real module.
        import tasks
        with mock.patch.object(tasks, "soulseek_fallback", fake_sf, create=True), \
             mock.patch.dict(sys.modules, {"tasks.soulseek_fallback": fake_sf}):
            return sh.check("/tmp/x.db")

    def test_logged_in_is_ok(self):
        status, _ = self._run(client=_FakeClient({"isLoggedIn": True, "username": "dj"}))
        self.assertEqual(status, "ok")
        self.assertIs(self.recorded["ok"], True)
        self.assertIn("dj", self.recorded["detail"])

    def test_running_but_logged_out_is_not_ok(self):
        # The failure this whole module exists to catch: slskd answers, so a naive
        # reachability probe would say "ok", but searches return nothing.
        status, _ = self._run(client=_FakeClient({"isLoggedIn": False}))
        self.assertEqual(status, "logged_out")
        self.assertIs(self.recorded["ok"], False)

    def test_unreachable_api(self):
        status, _ = self._run(client=_FakeClient(raises=OSError("connection refused")))
        self.assertEqual(status, "unreachable")
        self.assertIs(self.recorded["ok"], False)
        self.assertIn("connection refused", self.recorded["detail"])

    def test_disabled_is_not_reported_as_an_error(self):
        status, _ = self._run(enabled=False)
        self.assertEqual(status, "disabled")
        self.assertIsNone(self.recorded["ok"])

    def test_unconfigured_is_distinct_from_broken(self):
        status, _ = self._run(client=_FakeClient(configured=False))
        self.assertEqual(status, "not_configured")
        self.assertIsNone(self.recorded["ok"])

    def test_client_construction_failure_is_reported(self):
        status, _ = self._run(build_raises=RuntimeError("bad config"))
        self.assertEqual(status, "unreachable")

    def test_latency_is_recorded_on_success(self):
        self._run(client=_FakeClient({"isLoggedIn": True}))
        self.assertIsNotNone(self.recorded["latency"])

    def test_entry_point_never_raises(self):
        import asyncio
        with mock.patch.object(sh, "check", side_effect=RuntimeError("boom")):
            asyncio.run(sh.soulseek_health_check("/tmp/x.db"))  # must not propagate
        self.assertEqual(self.recorded["status"], "unreachable")


if __name__ == "__main__":
    unittest.main()
