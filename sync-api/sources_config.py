"""Acquire-source order + toggles as the API sees them (2.20.0).

sync-api cannot import worker code (separate image), so the source names and their
app_config toggle keys are mirrored here from sync-worker/tasks/sources/. A worker
test (tests/test_source_priority.py::TestApiMirror) loads this file and fails if
the two ever disagree, so adding a source means touching both.

Config model:
    source_priority      comma-separated ACQUIRE source names, first = tried first
    <toggle key>         per-source enable flag ("1"/"0")
"""

from __future__ import annotations

PRIORITY_KEY = "source_priority"

# Sources that can actually fetch audio, and the default order (Soulseek first:
# Tidal is being phased out). Must match tasks/sources/order.DEFAULT_PRIORITY.
ACQUIRE_SOURCES: tuple[str, ...] = ("soulseek", "tidal")
DEFAULT_PRIORITY: tuple[str, ...] = ("soulseek", "tidal")

# Buy-link-only stores: they never download, they only produce store search links.
LINK_SOURCES: tuple[str, ...] = ("qobuz", "beatport", "bandcamp")

# app_config key holding each source's enable toggle. Soulseek keeps its historical
# key so existing installs (and every worker module that reads it) stay in step.
TOGGLE_KEYS: dict[str, str] = {
    "soulseek": "soulseek_fallback_enabled",
    "tidal": "source_tidal_enabled",
    "qobuz": "source_qobuz_enabled",
    "beatport": "source_beatport_enabled",
    "bandcamp": "source_bandcamp_enabled",
}

LABELS: dict[str, str] = {
    "soulseek": "Soulseek",
    "tidal": "Tidal",
    "qobuz": "Qobuz",
    "beatport": "Beatport",
    "bandcamp": "Bandcamp",
}

ALL_SOURCES: tuple[str, ...] = ACQUIRE_SOURCES + LINK_SOURCES


class SourceConfigError(ValueError):
    """A source-order / toggle update that must be rejected (HTTP 400)."""


def truthy(val) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def is_enabled(cfg: dict, name: str) -> bool:
    """Every source defaults ON when its toggle has never been written."""
    raw = cfg.get(TOGGLE_KEYS[name])
    return True if raw is None else truthy(raw)


def parse_priority(raw: str | None) -> list[str]:
    """Tolerant read of a stored value (same rules as the worker): unknown names
    and duplicates dropped, missing acquire sources appended in default order."""
    out: list[str] = []
    for part in (raw or "").split(","):
        name = part.strip().lower()
        if name in ACQUIRE_SOURCES and name not in out:
            out.append(name)
    for name in DEFAULT_PRIORITY + ACQUIRE_SOURCES:
        if name not in out:
            out.append(name)
    return out


def validate_priority(value) -> list[str]:
    """Strict check of a priority the user is SETTING. Accepts a list or a
    comma-separated string. Returns the normalised full order.

    Rejects unknown names, link-only stores (they cannot download), duplicates and
    an empty list. Acquire sources left out are appended in default order, so a
    client that only knows about some sources cannot make the others unreachable.
    """
    if isinstance(value, str):
        names = [p.strip().lower() for p in value.split(",") if p.strip()]
    elif isinstance(value, (list, tuple)):
        if not all(isinstance(v, str) for v in value):
            raise SourceConfigError("priority must be a list of source names")
        names = [v.strip().lower() for v in value if v.strip()]
    else:
        raise SourceConfigError("priority must be a list of source names")
    if not names:
        raise SourceConfigError("priority must name at least one source")
    unknown = [n for n in names if n not in ALL_SOURCES]
    if unknown:
        raise SourceConfigError(
            f"unknown source(s): {', '.join(unknown)} "
            f"(known: {', '.join(ALL_SOURCES)})")
    link_only = [n for n in names if n in LINK_SOURCES]
    if link_only:
        raise SourceConfigError(
            f"{', '.join(link_only)} only produce buy-links and cannot be in the "
            f"download order (acquire sources: {', '.join(ACQUIRE_SOURCES)})")
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise SourceConfigError(f"duplicate source(s): {', '.join(dupes)}")
    return parse_priority(",".join(names))


def validate_enabled(value) -> dict[str, bool]:
    if not isinstance(value, dict):
        raise SourceConfigError("enabled must be an object of {source: bool}")
    unknown = [k for k in value if k not in ALL_SOURCES]
    if unknown:
        raise SourceConfigError(
            f"unknown source(s): {', '.join(unknown)} "
            f"(known: {', '.join(ALL_SOURCES)})")
    out: dict[str, bool] = {}
    for k, v in value.items():
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, (int, str)) and str(v).strip().lower() in (
                "0", "1", "true", "false", "yes", "no", "on", "off"):
            out[k] = truthy(v)
        else:
            raise SourceConfigError(f"enabled[{k}] must be true/false")
    return out


def migrate_source_priority(conn) -> bool:
    """Seed ``source_priority`` with the Soulseek-first default when it is absent.

    Runs on every API start (init_db). An install that predates 2.20.0 has no such
    key, so this is what moves it to Soulseek first; a value the user has chosen
    since is never overwritten. Returns True when it wrote the default.
    """
    row = conn.execute(
        "SELECT value FROM app_config WHERE key = ?", (PRIORITY_KEY,)
    ).fetchone()
    if row is not None and (row[0] or "").strip():
        return False
    conn.execute(
        "INSERT INTO app_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (PRIORITY_KEY, ",".join(DEFAULT_PRIORITY)),
    )
    conn.execute(
        "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
        ("source_priority_migrated",
         f"Acquire source order set to {', '.join(DEFAULT_PRIORITY)} (2.20.0 default)",
         None),
    )
    return True
