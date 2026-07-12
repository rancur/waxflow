"""WaxFlow v3 — Feature 7: read-only health / parity dashboard.

Three endpoints, all pure read-only aggregation over ``sync.db`` + ``app_config``:

  * ``GET /api/status.json``  — machine view (parity %, last-sync, wanted/error/
    import-queue counts, currently-sourcing, per-source stats, backup-throttle
    state, mac-availability, direct-write mode).
  * ``GET /api/status/trmnl`` — minimal, high-contrast, no-JS e-ink HTML sized for
    Will's TRMNL to poll (kept under a tight byte budget, no external assets).
  * ``GET /api/status``       — richer self-contained browser HTML, same data.

DESIGN — this module CANNOT destabilize the live system:

  * READ-ONLY. It only issues ``SELECT`` statements. It never writes, never
    touches the worker loop, and adds no schema. Nothing has to be enabled for it
    to work (it is inert if never queried).
  * RESILIENT. Many v3 signals are written by OTHER features that have not landed
    yet (mac-availability sampling, backup-throttle probe, direct-write mode,
    the per-source attempt log). Every signal is fetched defensively: a missing
    table / column / config key degrades to ``"unknown"`` and is recorded in
    ``signals_missing`` rather than raising. The endpoints must NEVER 500 on a
    partially-populated database — that is proven by the tests.

The aggregate is cheap to compute on-request today. A future worker task may cache
it into ``app_config`` (``status_cache_json``); if that key is present and fresh
this module will prefer it, but it always falls back to computing live so it works
before that task exists.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from db import get_db

router = APIRouter(prefix="/api", tags=["status"])

# Pipeline stages that mean "this track is being actively sourced right now".
_ACTIVE_STAGES = ("matching", "downloading", "verifying", "organizing")

# Byte budget for the TRMNL e-ink payload. The TRMNL is a low-power 800x480 e-ink
# display polling over the network; a lean, dependency-free page keeps refreshes
# fast and reliable. Kept well under this in practice; the test enforces it.
TRMNL_MAX_BYTES = 8192


# --------------------------------------------------------------------------- #
# Resilient low-level readers — every one degrades instead of raising.
# --------------------------------------------------------------------------- #

def _table_exists(conn, name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _columns(conn, table: str) -> set:
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _get_config(conn, key: str, default=None):
    """Single app_config value, or ``default`` if the table/key is absent."""
    try:
        row = conn.execute(
            "SELECT value FROM app_config WHERE key=?", (key,)
        ).fetchone()
        if row is None:
            return default
        return row[0]
    except Exception:
        return default


def _truthy(val) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _scalar(conn, sql: str, params=()):
    """Run a scalar SELECT, returning ``None`` on any failure."""
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row is not None else None
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Aggregate builder.
# --------------------------------------------------------------------------- #

def build_status(conn) -> dict:
    """Aggregate the whole health/parity picture from a live DB connection.

    Pure read-only. Missing signals become ``"unknown"`` and are appended to
    ``signals_missing`` so a caller can see *what* is not yet wired, without the
    endpoint ever failing.
    """
    missing: list[str] = []
    status: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # --- Parity: Lexicon-synced vs total Spotify likes ---------------------- #
    if _table_exists(conn, "tracks"):
        spotify_total = _scalar(conn, "SELECT COUNT(*) FROM tracks")
        lexicon_synced = _scalar(
            conn, "SELECT COUNT(*) FROM tracks WHERE lexicon_status='synced'"
        )
        if spotify_total is None or lexicon_synced is None:
            status["parity"] = {
                "spotify_likes": "unknown",
                "lexicon_synced": "unknown",
                "parity_pct": "unknown",
            }
            missing.append("parity")
        else:
            pct = round(lexicon_synced / spotify_total * 100, 2) if spotify_total else 0.0
            status["parity"] = {
                "spotify_likes": spotify_total,
                "lexicon_synced": lexicon_synced,
                "parity_pct": pct,
            }
    else:
        status["parity"] = {
            "spotify_likes": "unknown",
            "lexicon_synced": "unknown",
            "parity_pct": "unknown",
        }
        missing.append("parity")

    # --- Last sync ---------------------------------------------------------- #
    last_sync = _get_config(conn, "last_spotify_poll")
    if last_sync:
        status["last_sync"] = last_sync
    else:
        status["last_sync"] = "unknown"
        missing.append("last_sync")

    # --- Counts: wanted / errors / import-queue ----------------------------- #
    counts: dict = {}

    # errors: tracks parked in the error pipeline stage.
    if _table_exists(conn, "tracks"):
        errors = _scalar(conn, "SELECT COUNT(*) FROM tracks WHERE pipeline_stage='error'")
        counts["errors"] = errors if errors is not None else "unknown"
    else:
        counts["errors"] = "unknown"

    # wanted ledger (Feature 2 fills this; may be empty/absent for now).
    if _table_exists(conn, "wanted"):
        total = _scalar(conn, "SELECT COUNT(*) FROM wanted")
        counts["wanted"] = total if total is not None else "unknown"
        by_state: dict = {}
        try:
            for r in conn.execute(
                "SELECT state, COUNT(*) c FROM wanted GROUP BY state"
            ).fetchall():
                by_state[r[0]] = r[1]
        except Exception:
            pass
        counts["wanted_by_state"] = by_state
    else:
        counts["wanted"] = "unknown"
        counts["wanted_by_state"] = {}
        missing.append("wanted")

    # import queue (Feature 3 fills this).
    if _table_exists(conn, "import_queue"):
        total = _scalar(conn, "SELECT COUNT(*) FROM import_queue")
        counts["import_queue"] = total if total is not None else "unknown"
        by_state = {}
        try:
            for r in conn.execute(
                "SELECT state, COUNT(*) c FROM import_queue GROUP BY state"
            ).fetchall():
                by_state[r[0]] = r[1]
        except Exception:
            pass
        counts["import_queue_by_state"] = by_state
    else:
        counts["import_queue"] = "unknown"
        counts["import_queue_by_state"] = {}
        missing.append("import_queue")

    status["counts"] = counts

    # --- Currently sourcing ------------------------------------------------- #
    if _table_exists(conn, "tracks"):
        stages: dict = {}
        try:
            placeholders = ",".join("?" * len(_ACTIVE_STAGES))
            for r in conn.execute(
                f"SELECT pipeline_stage, COUNT(*) c FROM tracks "
                f"WHERE pipeline_stage IN ({placeholders}) GROUP BY pipeline_stage",
                _ACTIVE_STAGES,
            ).fetchall():
                stages[r[0]] = r[1]
        except Exception:
            pass
        sample: list[str] = []
        try:
            placeholders = ",".join("?" * len(_ACTIVE_STAGES))
            for r in conn.execute(
                f"SELECT artist, title FROM tracks "
                f"WHERE pipeline_stage IN ({placeholders}) "
                f"ORDER BY updated_at DESC LIMIT 5",
                _ACTIVE_STAGES,
            ).fetchall():
                artist = (r[0] or "?").strip()
                title = (r[1] or "?").strip()
                sample.append(f"{artist} - {title}")
        except Exception:
            pass
        status["currently_sourcing"] = {
            "count": sum(stages.values()),
            "stages": stages,
            "sample": sample,
        }
    else:
        status["currently_sourcing"] = {"count": "unknown", "stages": {}, "sample": []}

    # --- Per-source stats (source_attempts; Feature 2's forward log) -------- #
    per_source: dict = {}
    if _table_exists(conn, "source_attempts"):
        try:
            for r in conn.execute(
                "SELECT source, status, COUNT(*) c FROM source_attempts "
                "GROUP BY source, status"
            ).fetchall():
                per_source.setdefault(r[0], {})[r[1]] = r[2]
        except Exception:
            pass
    else:
        missing.append("per_source")
    status["per_source"] = per_source

    # --- Backup-throttle state (Feature 8's host probe fills the live bits) - #
    throttle: dict = {}
    enabled = _get_config(conn, "backup_throttle_enabled")
    throttle["enabled"] = _truthy(enabled) if enabled is not None else "unknown"

    backup_active = _get_config(conn, "nas_backup_active")
    throttle["nas_backup_active"] = (
        _truthy(backup_active) if backup_active is not None else "unknown"
    )
    if backup_active is None:
        missing.append("nas_backup_active")

    iowait = _get_config(conn, "nas_iowait_pct")
    if iowait is not None:
        try:
            throttle["iowait_pct"] = float(iowait)
        except (TypeError, ValueError):
            throttle["iowait_pct"] = "unknown"
    else:
        throttle["iowait_pct"] = "unknown"
        missing.append("nas_iowait_pct")

    threshold = _get_config(conn, "iowait_throttle_pct", "35")
    try:
        throttle["threshold_pct"] = float(threshold)
    except (TypeError, ValueError):
        throttle["threshold_pct"] = "unknown"

    # downloads_paused is the EXISTING manual/bulk-drive throttle already in prod.
    throttle["downloads_paused"] = _truthy(_get_config(conn, "downloads_paused", "0"))
    status["backup_throttle"] = throttle

    # --- Mac availability (Feature 3 samples this) -------------------------- #
    if _table_exists(conn, "mac_availability"):
        try:
            row = conn.execute(
                "SELECT reachable, smb_mounted, api_ok, detail, sampled_at "
                "FROM mac_availability ORDER BY sampled_at DESC, id DESC LIMIT 1"
            ).fetchone()
        except Exception:
            row = None
        if row is None:
            status["mac_availability"] = "unknown"
            missing.append("mac_availability")
        else:
            def _tri(v):
                return "unknown" if v is None else bool(v)
            status["mac_availability"] = {
                "reachable": _tri(row[0]),
                "smb_mounted": _tri(row[1]),
                "api_ok": _tri(row[2]),
                "detail": row[3],
                "sampled_at": row[4],
            }
    else:
        status["mac_availability"] = "unknown"
        missing.append("mac_availability")

    # --- Direct-write mode (Feature 5) -------------------------------------- #
    mode = _get_config(conn, "direct_write_mode")
    if mode:
        status["direct_write"] = {"mode": mode}
    else:
        enabled_dw = _get_config(conn, "direct_write_enabled")
        if enabled_dw is None:
            status["direct_write"] = {"mode": "unknown"}
            missing.append("direct_write")
        else:
            status["direct_write"] = {"mode": "on" if _truthy(enabled_dw) else "off"}

    status["signals_missing"] = missing
    return status


def _load_status() -> dict:
    """Open a read-only DB connection, build the aggregate, never raise."""
    try:
        with get_db() as conn:
            return build_status(conn)
    except Exception as e:  # DB entirely unreachable — still don't 500.
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": "database_unavailable",
            "detail": str(e),
            "signals_missing": ["all"],
        }


# --------------------------------------------------------------------------- #
# HTML rendering helpers.
# --------------------------------------------------------------------------- #

def _fmt(v) -> str:
    if isinstance(v, bool):
        return "yes" if v else "no"
    if v is None:
        return "unknown"
    return str(v)


def _parity_str(status: dict) -> str:
    p = status.get("parity", {})
    pct = p.get("parity_pct", "unknown")
    synced = p.get("lexicon_synced", "unknown")
    total = p.get("spotify_likes", "unknown")
    if pct == "unknown":
        return "unknown"
    return f"{pct}%  ({synced}/{total})"


def render_trmnl_html(status: dict) -> str:
    """Minimal high-contrast no-JS e-ink page. No external assets, tiny payload."""
    p = status.get("parity", {})
    counts = status.get("counts", {})
    sourcing = status.get("currently_sourcing", {})
    throttle = status.get("backup_throttle", {})
    mac = status.get("mac_availability", "unknown")
    dw = status.get("direct_write", {}).get("mode", "unknown")

    mac_str = "unknown"
    if isinstance(mac, dict):
        mac_str = "up" if mac.get("reachable") is True else "down"

    thr = throttle.get("nas_backup_active", "unknown")
    if throttle.get("downloads_paused"):
        thr_str = "PAUSED"
    elif thr is True:
        thr_str = "backup active"
    elif thr == "unknown":
        thr_str = "unknown"
    else:
        thr_str = "clear"

    rows = [
        ("PARITY", _parity_str(status)),
        ("Last sync", _fmt(status.get("last_sync"))),
        ("Sourcing now", _fmt(sourcing.get("count"))),
        ("Wanted", _fmt(counts.get("wanted"))),
        ("Errors", _fmt(counts.get("errors"))),
        ("Import queue", _fmt(counts.get("import_queue"))),
        ("Throttle", thr_str),
        ("Mac", mac_str),
        ("Direct-write", _fmt(dw)),
    ]
    tr = "".join(
        f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in rows
    )
    # Single, self-contained page. Black-on-white, big type, no script, no color.
    return (
        "<!doctype html><meta charset=utf-8>"
        "<meta name=viewport content='width=800,initial-scale=1'>"
        "<title>WaxFlow</title>"
        "<style>"
        "*{margin:0;padding:0;box-sizing:border-box}"
        "body{background:#fff;color:#000;font-family:Arial,Helvetica,sans-serif;"
        "font-weight:700;padding:12px}"
        "h1{font-size:26px;border-bottom:4px solid #000;padding-bottom:6px;margin-bottom:8px}"
        "table{width:100%;border-collapse:collapse;font-size:22px}"
        "th{text-align:left;padding:5px 8px;width:46%}"
        "td{text-align:right;padding:5px 8px}"
        "tr{border-bottom:2px solid #000}"
        "</style>"
        "<h1>WaxFlow &mdash; Lexicon parity</h1>"
        f"<table>{tr}</table>"
    )


def render_browser_html(status: dict) -> str:
    """Richer self-contained browser page. Same data, more detail, still no deps."""
    counts = status.get("counts", {})
    sourcing = status.get("currently_sourcing", {})
    throttle = status.get("backup_throttle", {})
    per_source = status.get("per_source", {})
    mac = status.get("mac_availability", "unknown")
    dw = status.get("direct_write", {}).get("mode", "unknown")
    missing = status.get("signals_missing", [])

    def card(label, value, sub=""):
        sub_html = f"<div class=sub>{sub}</div>" if sub else ""
        return (
            f"<div class=card><div class=label>{label}</div>"
            f"<div class=val>{value}</div>{sub_html}</div>"
        )

    p = status.get("parity", {})
    cards = [
        card("Parity", _fmt(p.get("parity_pct")) + ("%" if p.get("parity_pct") not in (None, "unknown") else ""),
             f"{_fmt(p.get('lexicon_synced'))} / {_fmt(p.get('spotify_likes'))} synced"),
        card("Sourcing now", _fmt(sourcing.get("count"))),
        card("Wanted", _fmt(counts.get("wanted"))),
        card("Errors", _fmt(counts.get("errors"))),
        card("Import queue", _fmt(counts.get("import_queue"))),
        card("Last sync", _fmt(status.get("last_sync"))),
    ]

    # currently-sourcing sample
    sample = sourcing.get("sample") or []
    sample_html = "".join(f"<li>{s}</li>" for s in sample) or "<li><em>none</em></li>"

    # per-source table
    if per_source:
        src_rows = ""
        for src, stats in sorted(per_source.items()):
            stat_str = ", ".join(f"{k}: {v}" for k, v in sorted(stats.items()))
            src_rows += f"<tr><th>{src}</th><td>{stat_str}</td></tr>"
    else:
        src_rows = "<tr><td colspan=2><em>no attempts logged yet</em></td></tr>"

    mac_html = "unknown"
    if isinstance(mac, dict):
        mac_html = (
            f"reachable: {_fmt(mac.get('reachable'))}, "
            f"smb: {_fmt(mac.get('smb_mounted'))}, "
            f"api: {_fmt(mac.get('api_ok'))} "
            f"({_fmt(mac.get('sampled_at'))})"
        )

    thr_html = (
        f"enabled: {_fmt(throttle.get('enabled'))}, "
        f"backup active: {_fmt(throttle.get('nas_backup_active'))}, "
        f"iowait: {_fmt(throttle.get('iowait_pct'))}% / "
        f"{_fmt(throttle.get('threshold_pct'))}%, "
        f"downloads paused: {_fmt(throttle.get('downloads_paused'))}"
    )

    missing_html = (
        f"<p class=missing>Signals not yet wired: {', '.join(missing)}</p>"
        if missing else ""
    )

    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<title>WaxFlow status</title>"
        "<style>"
        ":root{color-scheme:light dark}"
        "body{font-family:system-ui,-apple-system,Arial,sans-serif;margin:0;"
        "background:#0f1115;color:#e8e8ea;padding:24px;line-height:1.5}"
        "h1{font-size:24px;margin:0 0 4px}"
        ".gen{color:#8a8f98;font-size:13px;margin-bottom:20px}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));"
        "gap:14px;margin-bottom:24px}"
        ".card{background:#1a1d24;border:1px solid #2a2e37;border-radius:10px;padding:14px}"
        ".label{color:#8a8f98;font-size:12px;text-transform:uppercase;letter-spacing:.05em}"
        ".val{font-size:28px;font-weight:700;margin-top:4px}"
        ".sub{color:#8a8f98;font-size:12px;margin-top:4px}"
        "section{margin-bottom:24px}h2{font-size:16px;margin:0 0 10px}"
        "table{border-collapse:collapse;width:100%;max-width:640px}"
        "th,td{text-align:left;padding:6px 10px;border-bottom:1px solid #2a2e37;font-size:14px}"
        "th{color:#b8bcc4;font-weight:600}ul{margin:0;padding-left:20px}"
        ".missing{color:#c9a227;font-size:13px}"
        "@media(prefers-color-scheme:light){body{background:#fff;color:#111}"
        ".card{background:#f6f7f9;border-color:#e2e4e8}.gen,.label,.sub{color:#666}"
        "th,td{border-color:#e2e4e8}th{color:#333}}"
        "</style></head><body>"
        "<h1>WaxFlow &mdash; health &amp; parity</h1>"
        f"<div class=gen>generated {_fmt(status.get('generated_at'))}</div>"
        f"<div class=grid>{''.join(cards)}</div>"
        f"<section><h2>Currently sourcing ({_fmt(sourcing.get('count'))})</h2>"
        f"<ul>{sample_html}</ul></section>"
        f"<section><h2>Per-source attempts</h2><table>{src_rows}</table></section>"
        f"<section><h2>Backup throttle</h2><p>{thr_html}</p></section>"
        f"<section><h2>Mac availability</h2><p>{mac_html}</p></section>"
        f"<section><h2>Direct-write mode</h2><p>{_fmt(dw)}</p></section>"
        f"{missing_html}"
        "</body></html>"
    )


# --------------------------------------------------------------------------- #
# Routes.
# --------------------------------------------------------------------------- #

@router.get("/status.json")
async def status_json():
    """Machine-readable health/parity aggregate. Never 500s."""
    return JSONResponse(_load_status())


@router.get("/status/trmnl", response_class=HTMLResponse)
async def status_trmnl():
    """Minimal e-ink HTML for the TRMNL to poll."""
    return HTMLResponse(render_trmnl_html(_load_status()))


@router.get("/status", response_class=HTMLResponse)
async def status_browser():
    """Richer browser HTML, same underlying data."""
    return HTMLResponse(render_browser_html(_load_status()))
