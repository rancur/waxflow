"""Tests for WaxFlow v3 Feature 8 — backup-aware throttling (tasks/throttle.py).

Proves BEHAVIOR, not shape:

  * should_yield across the full backup_active × iowait × enabled × freshness
    permutation matrix, including the fail-open cases (disabled / no signal /
    stale signal all proceed);
  * YieldGate reports only the transition edges (log-once, not per-track);
  * an end-to-end cycle test: a simulated worker loop where the HEAVY stage is
    skipped while a backup flag is set and the LIGHT stage keeps running every
    cycle, with exactly ONE "pausing" log across the whole backup window and one
    "resuming" log when it clears.

No network / no external deps — a temp SQLite DB with just app_config.
"""

import logging
import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import throttle  # noqa: E402
from tasks.helpers import set_config  # noqa: E402


def _db_with_config(**cfg) -> str:
    """A minimal DB: just the app_config key/value table, optionally seeded."""
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);"
    )
    conn.commit()
    conn.close()
    for k, v in cfg.items():
        set_config(path, k, str(v))
    return path


# A fixed "now" so freshness math is deterministic regardless of wall clock.
NOW = 1_000_000.0
FRESH = NOW - 10   # 10s old — well inside the default 180s window
STALE = NOW - 999  # ~16 min old — outside the window


class TestShouldYieldMatrix(unittest.TestCase):
    def test_disabled_never_yields_even_with_active_backup(self):
        # Master switch off ⇒ inert, regardless of a raging backup.
        db = _db_with_config(
            backup_throttle_enabled="0",
            nas_backup_active="1",
            nas_iowait_pct="95",
            nas_signal_updated_at=FRESH,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)
        self.assertIn("disabled", reason)

    def test_enabled_backup_active_yields(self):
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="1",
            nas_iowait_pct="5",
            nas_signal_updated_at=FRESH,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertTrue(y)
        self.assertIn("HyperBackup", reason)

    def test_enabled_high_iowait_yields_without_backup_flag(self):
        # No detected process, but iowait over threshold ⇒ still yield.
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="60",
            nas_signal_updated_at=FRESH,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertTrue(y)
        self.assertIn("iowait", reason)

    def test_enabled_low_iowait_no_backup_proceeds(self):
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="10",
            nas_signal_updated_at=FRESH,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)
        self.assertIn("idle", reason)

    def test_iowait_exactly_at_threshold_yields(self):
        # Boundary: >= threshold yields. Default threshold is 35.
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="35",
            nas_signal_updated_at=FRESH,
        )
        y, _ = throttle.should_yield(db, now=NOW)
        self.assertTrue(y)

    def test_iowait_just_below_threshold_proceeds(self):
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="34",
            nas_signal_updated_at=FRESH,
        )
        y, _ = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)

    def test_custom_threshold_respected(self):
        # Lower the bar to 20 ⇒ 25% now yields.
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="25",
            iowait_throttle_pct="20",
            nas_signal_updated_at=FRESH,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertTrue(y)
        self.assertIn("20% threshold", reason)

    def test_stale_signal_fails_open(self):
        # Backup flag set, but the probe's last write is ancient ⇒ ignore it
        # and PROCEED (never stall the pipeline on a frozen probe).
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="1",
            nas_iowait_pct="95",
            nas_signal_updated_at=STALE,
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)
        self.assertIn("stale", reason)

    def test_no_signal_ever_written_fails_open(self):
        # Enabled but the probe has never reported ⇒ proceed.
        db = _db_with_config(backup_throttle_enabled="1")
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)
        self.assertIn("no NAS backup signal", reason)

    def test_custom_max_age_allows_older_signal(self):
        # Widen the freshness window so the otherwise-stale signal counts.
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="1",
            nas_signal_updated_at=STALE,
            nas_signal_max_age_seconds="5000",
        )
        y, reason = throttle.should_yield(db, now=NOW)
        self.assertTrue(y)
        self.assertIn("HyperBackup", reason)

    def test_garbage_iowait_treated_as_zero(self):
        # Operator-editable config must never crash the predicate.
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="not-a-number",
            nas_signal_updated_at=FRESH,
        )
        y, _ = throttle.should_yield(db, now=NOW)
        self.assertFalse(y)

    def test_boolean_word_forms_enable(self):
        for word in ("true", "yes", "on", "Y", "1"):
            db = _db_with_config(
                backup_throttle_enabled=word,
                nas_backup_active="true",
                nas_signal_updated_at=FRESH,
            )
            y, _ = throttle.should_yield(db, now=NOW)
            self.assertTrue(y, f"{word!r} should enable throttling")


class TestYieldGate(unittest.TestCase):
    def test_edges_only(self):
        gate = throttle.YieldGate()
        self.assertIsNone(gate.update(False))          # start idle: no edge
        self.assertEqual(gate.update(True), "enter")   # idle -> yield
        self.assertIsNone(gate.update(True))           # still yielding: no edge
        self.assertIsNone(gate.update(True))           # ...
        self.assertEqual(gate.update(False), "exit")   # yield -> idle
        self.assertIsNone(gate.update(False))          # still idle: no edge

    def test_log_transition_emits_once(self):
        gate = throttle.YieldGate()
        lg = logging.getLogger("test.throttle.gate")
        with self.assertLogs(lg, level="INFO") as cap:
            gate.log_transition(True, "NAS backup active", logger=lg)   # -> 1 log
            for _ in range(50):
                gate.log_transition(True, "NAS backup active", logger=lg)  # 0 logs
            gate.log_transition(False, "NAS idle", logger=lg)           # -> 1 log
        self.assertEqual(len(cap.records), 2)
        self.assertIn("pausing heavy ops", cap.output[0])
        self.assertIn("resuming heavy ops", cap.output[1])


class TestSimulatedWorkerCycle(unittest.TestCase):
    """End-to-end: heavy stages skip while light proceeds, log once.

    Models a Phase-C wiring where each worker cycle consults should_yield at the
    top of a HEAVY stage. We drive a backup that starts, runs a while, then ends,
    and assert the heavy stage never fired during the backup, the light stage
    fired every cycle, and only the two transition edges were logged.
    """

    def test_backup_window_pauses_heavy_keeps_light(self):
        db = _db_with_config(
            backup_throttle_enabled="1",
            nas_backup_active="0",
            nas_iowait_pct="5",
            nas_signal_updated_at=NOW,
        )
        gate = throttle.YieldGate()
        lg = logging.getLogger("test.throttle.cycle")

        heavy_runs = []
        light_runs = []

        # Timeline of the (backup_active, iowait) the probe would publish each
        # cycle: idle, then a 6-cycle backup, then idle again.
        timeline = [
            (0, 5), (0, 5),                      # idle
            (1, 80), (1, 82), (1, 79),           # backup running (proc + iowait)
            (0, 55), (0, 40),                    # backup proc gone but iowait tail-off (still high) -> still yields
            (0, 10), (0, 5),                     # fully idle again
        ]

        with self.assertLogs(lg, level="INFO") as cap:
            for cycle, (active, iowait) in enumerate(timeline):
                # The host probe publishes fresh signals for this cycle.
                sim_now = NOW + cycle
                set_config(db, "nas_backup_active", str(active))
                set_config(db, "nas_iowait_pct", str(iowait))
                set_config(db, "nas_signal_updated_at", str(sim_now))

                # Worker cycle: consult throttle at the top of the HEAVY stage.
                y, reason = throttle.should_yield(db, now=sim_now)
                gate.log_transition(y, reason, logger=lg)

                if y:
                    # HEAVY stage yields — but LIGHT ops keep running.
                    light_runs.append(cycle)
                    continue
                # Not yielding: both heavy and light run.
                heavy_runs.append(cycle)
                light_runs.append(cycle)

        # Light stage ran EVERY cycle (never paused).
        self.assertEqual(len(light_runs), len(timeline))
        # Heavy stage ran ONLY in the idle cycles: first two + last two = 4.
        self.assertEqual(heavy_runs, [0, 1, 7, 8])
        # Heavy NEVER ran during the backup window (cycles 2..6).
        for c in range(2, 7):
            self.assertNotIn(c, heavy_runs)
        # Exactly one pause + one resume logged across the whole window.
        self.assertEqual(len(cap.records), 2)
        self.assertIn("pausing heavy ops", cap.output[0])
        self.assertIn("resuming heavy ops", cap.output[1])


if __name__ == "__main__":
    unittest.main()
