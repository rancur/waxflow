"""Soulseek (slskd) reachability probe.

WHY A WORKER TASK AND NOT AN API PROBE
    sync-api cannot import sync-worker code -- separate containers, separate build
    contexts -- and the slskd credentials are injected into the WORKER. So the
    worker probes and persists the verdict into app_config, and the API reads those
    keys when it assembles /api/dashboard. This is exactly the pattern
    lexicon_health.record_import_health already uses for Lexicon.

WHY IT MATTERS
    Soulseek is the fallback that sources anything Tidal cannot supply. When it is
    logged out, tracks quietly accumulate in the fallback queue and nothing on the
    dashboard says why -- SlskdClient.is_logged_in() existed but was never surfaced
    anywhere in the UI.

Statuses:
    ok              slskd reachable and logged in to the Soulseek network
    logged_out      slskd is up but not logged in -- searches will return nothing
    unreachable     slskd's REST API did not answer
    not_configured  no slskd URL/API key set -- never set up, as opposed to broken
    disabled        the fallback is switched off; not an error

Config (read live from app_config):
    soulseek_fallback_enabled          default 1  (shared with soulseek_fallback)
    soulseek_health_interval_seconds   default 120
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from tasks.helpers import get_config, set_config

log = logging.getLogger("worker.soulseek_health")

STATUS_KEY = "soulseek_health"
OK_KEY = "soulseek_ok"
DETAIL_KEY = "soulseek_detail"
CHECKED_AT_KEY = "soulseek_checked_at"
LATENCY_KEY = "soulseek_latency_ms"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def record(db_path: str, status: str, detail: str, ok: bool | None,
           latency_ms: float | None = None) -> None:
    """Persist the verdict for sync-api to read."""
    set_config(db_path, STATUS_KEY, status)
    set_config(db_path, DETAIL_KEY, detail)
    set_config(db_path, CHECKED_AT_KEY, _now_iso())
    set_config(db_path, OK_KEY, "1" if ok is True else "0" if ok is False else "")
    set_config(db_path, LATENCY_KEY, "" if latency_ms is None else str(round(latency_ms, 1)))


def check(db_path: str) -> tuple[str, str]:
    """Probe slskd once and record the result. Returns (status, detail)."""
    # Imported lazily: soulseek_fallback pulls in the whole sourcing stack, and this
    # module should stay importable (and testable) on its own.
    from tasks import soulseek_fallback as sf

    if not sf.is_enabled(db_path):
        record(db_path, "disabled", "Soulseek fallback is disabled in settings", None)
        return "disabled", "disabled"

    try:
        client = sf.build_client(db_path)
    except Exception as e:  # noqa: BLE001
        detail = f"could not build slskd client: {e}"
        record(db_path, "unreachable", detail, False)
        return "unreachable", detail

    # An unconfigured install is not a fault -- don't page the user about a feature
    # they never turned on.
    if not client.configured:
        detail = "slskd URL or API key is not set"
        record(db_path, "not_configured", detail, None)
        return "not_configured", detail

    t0 = time.monotonic()
    try:
        state = client.server_state()
    except Exception as e:  # noqa: BLE001
        latency = (time.monotonic() - t0) * 1000
        detail = f"slskd API unreachable at {client.base}: {e}"
        record(db_path, "unreachable", detail, False, latency)
        log.warning("soulseek_health: %s", detail)
        return "unreachable", detail

    latency = (time.monotonic() - t0) * 1000

    if state.get("isLoggedIn"):
        # slskd's /api/v0/server reports address/state/isLoggedIn -- there is no
        # username field, so describe the connection rather than inventing one.
        server = state.get("address") or "the Soulseek network"
        detail = f"connected to {server} ({state.get('state') or 'LoggedIn'})"
        record(db_path, "ok", detail, True, latency)
        return "ok", detail

    detail = "slskd is running but not logged in to the Soulseek network"
    record(db_path, "logged_out", detail, False, latency)
    log.warning("soulseek_health: %s", detail)
    return "logged_out", detail


async def soulseek_health_check(db_path: str) -> None:
    """Worker entry point."""
    try:
        check(db_path)
    except Exception as e:  # noqa: BLE001
        # A health probe must never take the worker loop down with it.
        log.warning("soulseek_health check failed unexpectedly: %s", e)
        try:
            record(db_path, "unreachable", f"probe error: {e}", False)
        except Exception:  # noqa: BLE001
            pass
