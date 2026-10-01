"""Acquire-source order, toggles and availability (2.20.0).

GET  /api/sources   the configured download order, each source's toggle and
                    whether it is usable right now (slskd reachable, Tidal login ok)
PUT  /api/sources   set the order and/or toggles. Names are validated against the
                    known sources; unknown or link-only names in the order are a 400.

The worker reads the same app_config keys on every cycle, so a change takes effect
without a restart. See sources_config.py for the config model.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import sources_config as sc
from db import get_db

router = APIRouter(prefix="/api", tags=["sources"])


class SourcesUpdate(BaseModel):
    priority: Optional[Any] = None   # list[str] or "a,b"
    enabled: Optional[Any] = None    # {name: bool}


def _load_cfg(conn) -> dict:
    keys = (sc.PRIORITY_KEY, *sc.TOGGLE_KEYS.values())
    rows = conn.execute(
        f"SELECT key, value FROM app_config WHERE key IN ({','.join('?' * len(keys))})",
        keys,
    ).fetchall()
    return {r["key"]: r["value"] for r in rows}


def _availability(name: str, enabled: bool) -> tuple[str, Optional[str]]:
    """("ok"|"error"|"unknown"|"disabled", detail) for an acquire source."""
    if not enabled:
        return "disabled", "switched off in Settings"
    if name == "soulseek":
        from routes.dashboard import _soulseek_service
        svc = _soulseek_service()
        return svc.status, svc.error
    if name == "tidal":
        from routes.tidal import tidal_auth_health
        return tidal_auth_health()
    return "unknown", None


def build_sources_payload(cfg: dict) -> dict:
    priority = sc.parse_priority(cfg.get(sc.PRIORITY_KEY))
    acquire = []
    for pos, name in enumerate(priority, start=1):
        enabled = sc.is_enabled(cfg, name)
        status, detail = _availability(name, enabled)
        acquire.append({
            "name": name, "label": sc.LABELS[name], "kind": "acquire",
            "position": pos, "enabled": enabled, "toggle_key": sc.TOGGLE_KEYS[name],
            "status": status, "detail": detail,
        })
    links = [{
        "name": name, "label": sc.LABELS[name], "kind": "link",
        "enabled": sc.is_enabled(cfg, name), "toggle_key": sc.TOGGLE_KEYS[name],
    } for name in sc.LINK_SOURCES]
    return {
        "priority": priority,
        "active_order": [s["name"] for s in acquire if s["enabled"]],
        "default_priority": list(sc.DEFAULT_PRIORITY),
        "acquire": acquire,
        "links": links,
    }


@router.get("/sources")
async def get_sources():
    with get_db() as conn:
        cfg = _load_cfg(conn)
    return build_sources_payload(cfg)


@router.put("/sources")
async def put_sources(body: SourcesUpdate):
    if body.priority is None and body.enabled is None:
        raise HTTPException(status_code=400, detail="nothing to update: send priority and/or enabled")
    try:
        priority = sc.validate_priority(body.priority) if body.priority is not None else None
        enabled = sc.validate_enabled(body.enabled) if body.enabled is not None else {}
    except sc.SourceConfigError as e:
        raise HTTPException(status_code=400, detail=str(e))

    with get_db() as conn:
        cfg = _load_cfg(conn)
        after = dict(cfg)
        for name, on in enabled.items():
            after[sc.TOGGLE_KEYS[name]] = "1" if on else "0"
        if not any(sc.is_enabled(after, n) for n in sc.ACQUIRE_SOURCES):
            raise HTTPException(
                status_code=400,
                detail="at least one download source (soulseek or tidal) must stay enabled")

        writes: dict[str, str] = {}
        if priority is not None:
            writes[sc.PRIORITY_KEY] = ",".join(priority)
        for name, on in enabled.items():
            writes[sc.TOGGLE_KEYS[name]] = "1" if on else "0"
        for key, value in writes.items():
            conn.execute(
                "INSERT INTO app_config (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        conn.execute(
            "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
            ("sources_updated",
             "Download sources updated: " + ", ".join(f"{k}={v}" for k, v in writes.items()),
             None),
        )
        cfg = _load_cfg(conn)
    return build_sources_payload(cfg)
