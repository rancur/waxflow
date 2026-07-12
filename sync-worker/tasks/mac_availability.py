"""Lexicon-host (Mac) availability detection — Phase 3 sleep-tolerance.

The Mac that runs Lexicon is a machine Will actually uses; it goes to sleep. When
it does, every Lexicon-side write (import, playlist link, tag) must be HELD, not
attempted-and-errored. This module is the single detector that decides whether it
is safe to push Lexicon work right now, and records rolling samples into the v3
``mac_availability`` scaffold table.

It distinguishes the two failure modes that need different handling:

  * ``asleep``       — the Mac itself is unreachable on the LAN (asleep, off,
                       network gone). Nothing Lexicon-side can happen; hold work.
  * ``lexicon_down`` — the Mac is UP (a TCP port answers) but the Lexicon API does
                       not respond (Lexicon quit / not launched). Also hold work,
                       but it is a distinct, faster-to-recover condition (the app
                       can be relaunched without waking the machine).
  * ``available``    — the Lexicon API answered; safe to push / drain the queue.

The reachability probe is a cheap TCP connect to a port that is open whenever the
Mac is awake regardless of Lexicon (SSH :22 by default, configurable). The Lexicon
check reuses the same GET /v1/playlists probe the import-health canary uses, so
the two agree on "Lexicon reachable".

Pure stdlib + httpx; no new dependencies. Reads/writes only the scaffold
``mac_availability`` table and ``app_config`` — no schema rebuild.
"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from tasks.helpers import LEXICON_API_URL, get_config, get_db

log = logging.getLogger("worker.mac_availability")

# State values recorded in mac_availability.detail / returned to callers.
STATE_AVAILABLE = "available"
STATE_LEXICON_DOWN = "lexicon_down"
STATE_ASLEEP = "asleep"

# Defaults (all overridable via app_config so the batched deploy can tune live).
_DEFAULT_REACH_PORT = 22          # SSH: open whenever the Mac is awake, Lexicon or not
_DEFAULT_TCP_TIMEOUT = 3.0        # seconds — a sleeping host fails fast (RST/timeout)
_DEFAULT_API_TIMEOUT = 5.0        # seconds — Lexicon API probe


@dataclass
class Availability:
    """Result of one availability probe."""
    state: str
    reachable: bool
    api_ok: bool
    smb_mounted: bool | None
    detail: str

    @property
    def lexicon_available(self) -> bool:
        """True only when it is safe to push Lexicon-side work right now."""
        return self.state == STATE_AVAILABLE


def _lexicon_host(api_url: str) -> str:
    """Extract the host from the configured Lexicon API URL. Falls back to the
    Lexicon host env default when the URL has no hostname."""
    try:
        host = urlparse(api_url).hostname
    except Exception:
        host = None
    return host or "127.0.0.1"


def _tcp_reachable(host: str, port: int, timeout: float) -> bool:
    """Cheap liveness probe: can we open a TCP connection to host:port?

    A sleeping/offline Mac either refuses fast or times out; an awake Mac accepts
    even if Lexicon itself is not running. This is what separates ``asleep`` from
    ``lexicon_down``.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _lexicon_api_ok(api_url: str, timeout: float) -> tuple[bool, str]:
    """Reuse the import-health canary's reachability semantics: GET /v1/playlists
    == 200 means the Lexicon API is truly answering."""
    try:
        with httpx.Client(base_url=api_url, timeout=timeout) as client:
            r = client.get("/v1/playlists")
        if r.status_code == 200:
            return True, f"Lexicon API 200 at {api_url}"
        return False, f"Lexicon API {r.status_code} at {api_url}"
    except Exception as e:  # noqa: BLE001 — any transport error == not available
        return False, f"Lexicon API unreachable at {api_url} ({e})"


def probe(db_path: str, *, record: bool = True) -> Availability:
    """Probe Mac + Lexicon availability, optionally recording a mac_availability row.

    Order: cheapest-first. The Lexicon API probe doubles as a reachability probe
    (a 200 proves the host is up too), so we only fall back to the raw TCP probe
    when the API is NOT answering — that TCP result is what distinguishes an
    asleep Mac from a merely Lexicon-down one.
    """
    api_url = get_config(db_path, "lexicon_api_url") or LEXICON_API_URL
    host = _lexicon_host(api_url)
    try:
        reach_port = int(get_config(db_path, "mac_reachability_port") or _DEFAULT_REACH_PORT)
    except (TypeError, ValueError):
        reach_port = _DEFAULT_REACH_PORT

    api_ok, api_detail = _lexicon_api_ok(api_url, _DEFAULT_API_TIMEOUT)

    if api_ok:
        reachable = True
        state = STATE_AVAILABLE
        detail = api_detail
    else:
        reachable = _tcp_reachable(host, reach_port, _DEFAULT_TCP_TIMEOUT)
        if reachable:
            state = STATE_LEXICON_DOWN
            detail = f"Mac up (tcp {host}:{reach_port} open) but {api_detail}"
        else:
            state = STATE_ASLEEP
            detail = f"Mac unreachable (tcp {host}:{reach_port} closed) — {api_detail}"

    # smb_mounted: the worker cannot see the Mac's SMB mount directly. The
    # authoritative signal is the empty-import detector, persisted by the
    # import-health recorder into lexicon_mount_ok. Surface it here as tri-state.
    mount_flag = get_config(db_path, "lexicon_mount_ok")
    smb_mounted: bool | None
    if mount_flag == "1":
        smb_mounted = True
    elif mount_flag == "0":
        smb_mounted = False
    else:
        smb_mounted = None

    result = Availability(
        state=state,
        reachable=reachable,
        api_ok=api_ok,
        smb_mounted=smb_mounted,
        detail=detail,
    )

    if record:
        try:
            with get_db(db_path) as conn:
                conn.execute(
                    """INSERT INTO mac_availability (reachable, smb_mounted, api_ok, detail)
                       VALUES (?, ?, ?, ?)""",
                    (
                        1 if reachable else 0,
                        None if smb_mounted is None else (1 if smb_mounted else 0),
                        1 if api_ok else 0,
                        f"[{state}] {detail}",
                    ),
                )
        except Exception as e:  # never let sampling break the loop
            log.warning("mac_availability: failed to record sample: %s", e)

    return result


def latest(db_path: str) -> Availability | None:
    """Read back the most recent recorded sample (for callers that want the last
    known state without probing the network again)."""
    try:
        with get_db(db_path) as conn:
            row = conn.execute(
                """SELECT reachable, smb_mounted, api_ok, detail
                   FROM mac_availability ORDER BY id DESC LIMIT 1"""
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    detail = row["detail"] or ""
    state = STATE_AVAILABLE
    if detail.startswith("["):
        state = detail[1:detail.index("]")] if "]" in detail else STATE_AVAILABLE
    return Availability(
        state=state,
        reachable=bool(row["reachable"]),
        api_ok=bool(row["api_ok"]),
        smb_mounted=None if row["smb_mounted"] is None else bool(row["smb_mounted"]),
        detail=detail,
    )


async def sample_availability(db_path: str) -> None:
    """Async worker-task entry point: take + record one availability sample.

    Registered on its own short interval so mac_availability holds a fresh rolling
    history the offline queue reads. Pure observability — it never enqueues, holds,
    or drains, so it is safe to run even with the offline queue flag off.
    """
    import asyncio

    await asyncio.to_thread(probe, db_path)
