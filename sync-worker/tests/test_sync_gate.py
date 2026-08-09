"""Tests for the replication gate (tasks/sync_gate.py).

The gate exists because WaxFlow now hands Lexicon a LOCAL Mac path, so an import
fired before the one-way rsync has landed the file produces Lexicon's silent
HTTP-200-imported-0-tracks failure.

The contract these tests pin down:
  * a file older than the last completed sync pass imports immediately,
  * a file newer than it is HELD (still in flight),
  * every degenerate heartbeat case FAILS OPEN — missing, malformed, stale, or
    status!=ok must never stall the pipeline, because a gate that can deadlock is
    worse than the lag it prevents,
  * the hold is bounded: past sync_gate_max_hold_seconds we import anyway rather
    than spin forever on a file that will never replicate,
  * the whole gate can be switched off live via sync_gate_enabled.
"""

import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import sync_gate  # noqa: E402


def _db(**config) -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);"
    )
    for k, v in config.items():
        conn.execute("INSERT INTO app_config (key, value) VALUES (?, ?)", (k, str(v)))
    conn.commit()
    conn.close()
    return path


def _heartbeat(tmpdir: str, completed_at: float, status: str = "ok") -> str:
    p = os.path.join(tmpdir, ".waxflow-sync-heartbeat")
    with open(p, "w", encoding="utf-8") as fh:
        json.dump({"status": status, "completed_at": int(completed_at)}, fh)
    return p


def _audio(tmpdir: str, mtime: float) -> str:
    p = os.path.join(tmpdir, "track.flac")
    with open(p, "wb") as fh:
        fh.write(b"\0")
    os.utime(p, (mtime, mtime))
    return p


class SyncGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.now = time.time()

    def test_file_older_than_last_sync_is_replicated(self):
        hb = _heartbeat(self.tmp, self.now - 60)
        f = _audio(self.tmp, self.now - 600)          # written well before the sync
        db = _db(sync_gate_heartbeat_path=hb)
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertTrue(ok)
        self.assertEqual(reason, "replicated")

    def test_file_newer_than_last_sync_is_held(self):
        hb = _heartbeat(self.tmp, self.now - 300)
        f = _audio(self.tmp, self.now - 10)           # written after the last pass
        db = _db(sync_gate_heartbeat_path=hb)
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertFalse(ok)
        self.assertIn("awaiting replication", reason)

    def test_missing_heartbeat_fails_open(self):
        f = _audio(self.tmp, self.now)
        db = _db(sync_gate_heartbeat_path=os.path.join(self.tmp, "nope.json"))
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertTrue(ok)
        self.assertIn("no usable sync heartbeat", reason)

    def test_malformed_heartbeat_fails_open(self):
        p = os.path.join(self.tmp, "bad.json")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        f = _audio(self.tmp, self.now)
        ok, _ = sync_gate.is_replicated(_db(sync_gate_heartbeat_path=p), f)
        self.assertTrue(ok)

    def test_stale_heartbeat_fails_open(self):
        # agent died an hour ago; max age is 30 min -> treat as no heartbeat
        hb = _heartbeat(self.tmp, self.now - 3600)
        f = _audio(self.tmp, self.now)
        db = _db(sync_gate_heartbeat_path=hb, sync_gate_heartbeat_max_age_seconds=1800)
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertTrue(ok)
        self.assertIn("no usable sync heartbeat", reason)

    def test_error_status_heartbeat_fails_open(self):
        hb = _heartbeat(self.tmp, self.now - 60, status="error")
        f = _audio(self.tmp, self.now)
        ok, reason = sync_gate.is_replicated(_db(sync_gate_heartbeat_path=hb), f)
        self.assertTrue(ok)
        self.assertIn("status=error", reason)

    def test_hold_is_bounded_by_max_hold_seconds(self):
        # file is newer than the last sync AND older than the max hold -> import
        hb = _heartbeat(self.tmp, self.now - 7200)
        # keep the heartbeat itself fresh enough to be usable
        db = _db(sync_gate_heartbeat_path=hb,
                 sync_gate_heartbeat_max_age_seconds=99999,
                 sync_gate_max_hold_seconds=600)
        f = _audio(self.tmp, self.now - 3600)
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertTrue(ok)
        self.assertIn("max hold", reason)

    def test_disabled_gate_always_allows(self):
        hb = _heartbeat(self.tmp, self.now - 300)
        f = _audio(self.tmp, self.now)
        db = _db(sync_gate_heartbeat_path=hb, sync_gate_enabled="0")
        ok, reason = sync_gate.is_replicated(db, f)
        self.assertTrue(ok)
        self.assertEqual(reason, "gate disabled")

    def test_no_file_path_allows(self):
        ok, reason = sync_gate.is_replicated(_db(), None)
        self.assertTrue(ok)
        self.assertEqual(reason, "no file_path")

    def test_unstatable_file_fails_open(self):
        hb = _heartbeat(self.tmp, self.now - 60)
        db = _db(sync_gate_heartbeat_path=hb)
        ok, reason = sync_gate.is_replicated(db, os.path.join(self.tmp, "ghost.flac"))
        self.assertTrue(ok)
        self.assertIn("not stat-able", reason)


if __name__ == "__main__":
    unittest.main()
