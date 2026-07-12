"""Tests for Phase 3 Mac/Lexicon availability detection.

Proves the detector distinguishes the three states that drive sleep-tolerance:
  * available    — Lexicon API answers.
  * lexicon_down — Mac reachable (TCP open) but Lexicon API not answering.
  * asleep       — Mac unreachable (TCP closed) and API not answering.
And that each probe records a rolling sample into the mac_availability table.

No real network: the TCP + API probes are monkeypatched.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import mac_availability, v3_schema  # noqa: E402


def _db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (id INTEGER PRIMARY KEY AUTOINCREMENT, spotify_id TEXT);
        CREATE TABLE playlists (id INTEGER PRIMARY KEY AUTOINCREMENT);
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)
    return path


class AvailabilityDetectionTest(unittest.TestCase):
    def setUp(self):
        self.db = _db()
        self._orig_api = mac_availability._lexicon_api_ok
        self._orig_tcp = mac_availability._tcp_reachable

    def tearDown(self):
        mac_availability._lexicon_api_ok = self._orig_api
        mac_availability._tcp_reachable = self._orig_tcp
        if os.path.exists(self.db):
            os.remove(self.db)

    def _patch(self, api_ok, tcp_ok):
        mac_availability._lexicon_api_ok = lambda url, t: (api_ok, "stub")
        mac_availability._tcp_reachable = lambda h, p, t: tcp_ok

    def test_available_when_api_ok(self):
        self._patch(api_ok=True, tcp_ok=True)
        a = mac_availability.probe(self.db)
        self.assertEqual(a.state, mac_availability.STATE_AVAILABLE)
        self.assertTrue(a.lexicon_available)
        self.assertTrue(a.api_ok)

    def test_lexicon_down_when_reachable_but_api_dead(self):
        self._patch(api_ok=False, tcp_ok=True)
        a = mac_availability.probe(self.db)
        self.assertEqual(a.state, mac_availability.STATE_LEXICON_DOWN)
        self.assertFalse(a.lexicon_available)
        self.assertTrue(a.reachable)
        self.assertFalse(a.api_ok)

    def test_asleep_when_unreachable(self):
        self._patch(api_ok=False, tcp_ok=False)
        a = mac_availability.probe(self.db)
        self.assertEqual(a.state, mac_availability.STATE_ASLEEP)
        self.assertFalse(a.lexicon_available)
        self.assertFalse(a.reachable)

    def test_probe_records_sample_and_latest_reads_it(self):
        self._patch(api_ok=False, tcp_ok=False)
        mac_availability.probe(self.db)
        with sqlite3.connect(self.db) as conn:
            n = conn.execute("SELECT COUNT(*) FROM mac_availability").fetchone()[0]
        self.assertEqual(n, 1)
        last = mac_availability.latest(self.db)
        self.assertEqual(last.state, mac_availability.STATE_ASLEEP)


if __name__ == "__main__":
    unittest.main()
