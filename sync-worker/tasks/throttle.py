"""WaxFlow v3 — backup-aware throttling (Feature 8).

Generalizes last night's *manual* iowait-gating (the ``downloads_paused``
app_config kill-switch in ``process_pipeline._process_downloading``) into a
permanent, automatic auto-yield so heavy WaxFlow ops never fight a NAS
HyperBackup run.

Problem shape
-------------
The worker runs in a Docker container on the NAS. It CAN read the host's
aggregate iowait from ``/proc/stat`` (the container shares the host kernel), but
it CANNOT see the host-side HyperBackup processes (``synoimgbkptool`` /
``aws_s3_ccpd``) — those live in the host PID namespace. So detection is split
in two:

  * a HOST-SIDE probe (``scripts/nas-backup-probe.sh``) runs on the NAS, detects
    the HyperBackup process + samples iowait, and writes three signals into
    ``app_config`` (the cross-process signal bus): ``nas_backup_active`` (0/1),
    ``nas_iowait_pct`` (percent), and ``nas_signal_updated_at`` (epoch seconds,
    for freshness).
  * this module (``should_yield``) reads those signals and decides whether a
    HEAVY stage should yield.

``should_yield(db_path)`` is DESIGNED to be consulted at the top of every HEAVY
stage (downloads, acquisition, index_library, Plex scans, direct-write bulk):
when it returns ``(True, reason)`` the caller pauses heavy work, keeps light
ops (polling, config, health) running, logs the transition ONCE (via
``YieldGate``), and resumes automatically when the signals go idle.

Config (app_config keys; all default-OFF / conservative)
--------------------------------------------------------
  * ``backup_throttle_enabled``   — master switch. Absent/false ⇒ NEVER yields.
  * ``iowait_throttle_pct``       — iowait percent at/above which heavy ops
                                    yield even without a detected backup
                                    process. Default 35.
  * ``nas_signal_max_age_seconds``— signals older than this are treated as STALE
                                    and IGNORED (fail-open, see below).
                                    Default 180.

Signals (app_config keys; written by the host probe)
----------------------------------------------------
  * ``nas_backup_active``    — "1" while a HyperBackup process is running.
  * ``nas_iowait_pct``       — host iowait percent (integer/float string).
  * ``nas_signal_updated_at``— epoch seconds of the probe's last write.

Fail-open by design
--------------------
Throttling protects *throughput of the backup*, but a stuck "yield" would stall
parity forever. So every ambiguous case fails toward NOT yielding (heavy ops
proceed): master switch off, no signals written, or STALE signals ⇒ do not
yield. The worst case of a false negative is fighting the backup a little (the
pre-Feature-8 status quo); the worst case of a false positive is a permanent
pipeline stall — so we bias away from the latter.

INERT
-----
This module is not yet consulted by the live worker loop. Phase C wires
``should_yield`` into the heavy stages behind ``backup_throttle_enabled`` in a
no-backup window. Importing this module has NO side effects and writes nothing.
"""

from __future__ import annotations

import logging
import time
from typing import Optional, Tuple

from tasks.helpers import get_config

log = logging.getLogger("worker.throttle")

# --- app_config keys -------------------------------------------------------
CFG_ENABLED = "backup_throttle_enabled"
CFG_IOWAIT_PCT = "iowait_throttle_pct"
CFG_SIGNAL_MAX_AGE = "nas_signal_max_age_seconds"

SIG_BACKUP_ACTIVE = "nas_backup_active"
SIG_IOWAIT_PCT = "nas_iowait_pct"
SIG_UPDATED_AT = "nas_signal_updated_at"

# --- defaults (used when the key is absent) --------------------------------
DEFAULT_IOWAIT_PCT = 35.0
DEFAULT_SIGNAL_MAX_AGE = 180.0

_TRUE = {"1", "true", "yes", "on", "y", "t"}


def _truthy(val: Optional[str]) -> bool:
    """Mirror the worker's app_config boolean convention (see
    ``process_pipeline`` ``downloads_paused`` parsing)."""
    return str(val or "0").strip().lower() in _TRUE


def _as_float(val: Optional[str], default: float) -> float:
    """Parse a numeric app_config value, falling back to ``default`` on any
    missing/garbage value (config is operator-editable, so be forgiving)."""
    if val is None:
        return default
    try:
        return float(str(val).strip())
    except (TypeError, ValueError):
        return default


def should_yield(db_path: str, now: Optional[float] = None) -> Tuple[bool, str]:
    """Decide whether a HEAVY stage should yield to a NAS backup.

    Returns ``(yield, reason)``. ``reason`` is a short human string for the
    one-shot transition log (never per-track).

    Fail-open: master-switch-off, no-signal, and stale-signal all return
    ``(False, ...)`` so heavy work is never stalled by a missing/frozen probe.

    ``now`` is injectable for deterministic tests; defaults to wall clock.
    """
    if now is None:
        now = time.time()

    # 1) Master switch. Absent/false ⇒ feature is inert, never yields.
    if not _truthy(get_config(db_path, CFG_ENABLED)):
        return False, "throttle disabled (backup_throttle_enabled off)"

    # 2) Freshness. If the probe has never written, or its last write is older
    #    than the max age, IGNORE the signals and do not yield (fail-open).
    max_age = _as_float(get_config(db_path, CFG_SIGNAL_MAX_AGE), DEFAULT_SIGNAL_MAX_AGE)
    updated_raw = get_config(db_path, SIG_UPDATED_AT)
    if updated_raw is None:
        return False, "no NAS backup signal yet (probe not reporting)"
    updated_at = _as_float(updated_raw, 0.0)
    age = now - updated_at
    if age > max_age:
        return (
            False,
            f"NAS signal stale ({age:.0f}s > {max_age:.0f}s) — ignoring, not yielding",
        )

    # 3) A detected HyperBackup process is the strongest signal.
    if _truthy(get_config(db_path, SIG_BACKUP_ACTIVE)):
        return True, "NAS backup active (HyperBackup running)"

    # 4) No detected backup, but sustained host iowait ⇒ still yield. This also
    #    catches backups whose process name the probe doesn't recognize.
    threshold = _as_float(get_config(db_path, CFG_IOWAIT_PCT), DEFAULT_IOWAIT_PCT)
    iowait = _as_float(get_config(db_path, SIG_IOWAIT_PCT), 0.0)
    if iowait >= threshold:
        return True, f"host iowait {iowait:.0f}% >= {threshold:.0f}% threshold"

    # 5) Idle: backup inactive and iowait below threshold.
    return False, f"NAS idle (backup inactive, iowait {iowait:.0f}% < {threshold:.0f}%)"


class YieldGate:
    """Log-once gate for the yield transition across worker cycles.

    ``should_yield`` is a pure predicate consulted every cycle / stage. Logging
    its result directly would spam one line per track while a backup runs. This
    gate remembers the last state and only reports the *edges* (enter/exit), so
    a multi-hour backup produces exactly one "yielding" line and one "resumed"
    line.

    Usage (Phase C wiring — shown here, not yet live)::

        gate = YieldGate()               # module/loop-scoped, persists across cycles
        ...
        y, reason = should_yield(db_path)
        edge = gate.update(y, reason)
        if edge == "enter":
            log.info("throttle: pausing heavy ops — %s", reason)
        elif edge == "exit":
            log.info("throttle: resuming heavy ops — %s", reason)
        if y:
            return  # skip THIS heavy stage; light ops elsewhere keep running
    """

    def __init__(self) -> None:
        self._yielding = False

    @property
    def yielding(self) -> bool:
        return self._yielding

    def update(self, yielding: bool, reason: str = "") -> Optional[str]:
        """Feed the latest ``should_yield`` boolean. Returns ``"enter"`` on the
        first cycle we start yielding, ``"exit"`` on the first cycle we stop,
        and ``None`` when the state is unchanged (the common case)."""
        if yielding and not self._yielding:
            self._yielding = True
            return "enter"
        if not yielding and self._yielding:
            self._yielding = False
            return "exit"
        return None

    def log_transition(self, yielding: bool, reason: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
        """Convenience: update + emit the one-shot log line. Returns the edge
        (or None). Kept tiny so the Phase-C call site is a single line."""
        edge = self.update(yielding, reason)
        lg = logger or log
        if edge == "enter":
            lg.info("throttle: pausing heavy ops — %s", reason)
        elif edge == "exit":
            lg.info("throttle: resuming heavy ops — %s", reason)
        return edge
