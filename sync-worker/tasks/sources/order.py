"""User-selectable acquire-source order (2.20.0).

The operator picks which ACQUIRE sources WaxFlow may download from and in what
order. Two pieces of ``app_config`` drive it:

    source_priority            comma-separated acquire source names, first = tried
                               first. Default ``soulseek,tidal``.
    <source>.toggle_key        per-source enable flag (``soulseek_fallback_enabled``,
                               ``source_tidal_enabled``, ...). See each Source class.

A new track walks the enabled sources in this order. It moves on to the next one
only when the current one MISSES (no candidates / nothing passed the lossless gate /
no match), so a Soulseek-first install never touches Tidal for a track Soulseek
delivered, and a Tidal-disabled install never calls Tidal at all.

This module only answers "which source next?". The pipeline (process_pipeline) and
the Soulseek stage (soulseek_fallback) act on the answer.

Kept free of process_pipeline imports so it is importable (and testable) on its own.
"""

from __future__ import annotations

from tasks.helpers import get_config
from tasks.sources import registry

CONFIG_KEY = "source_priority"

# Soulseek first: Tidal is being phased out, and tiddl stalls. Mirrors
# sync-api/sources_config.py DEFAULT_PRIORITY (a worker test asserts they agree).
DEFAULT_PRIORITY: tuple[str, ...] = ("soulseek", "tidal")

# Prefix written into the Soulseek queue row (fallback_attempts.search_query) when a
# track is sent to Soulseek because Soulseek is FIRST in the order, as opposed to
# being sent there after a Tidal miss. On a Soulseek miss this is what tells the
# fallback stage that the later sources have not been tried yet.
SOULSEEK_FIRST_REASON = "source_priority: soulseek first"


def known_acquire_names() -> list[str]:
    """Registered ACQUIRE-capable source names (registry order)."""
    return [s.name for s in registry.acquire_sources()]


def parse_priority(raw: str | None) -> list[str]:
    """Normalise a stored ``source_priority`` value into a full ordered list.

    Tolerant on READ (the API is strict on write): unknown names and duplicates are
    dropped, and any registered acquire source the value does not mention is
    appended in default order, so a source added in a later release is never
    silently unreachable. An empty/missing value yields the default order.
    """
    known = known_acquire_names()
    out: list[str] = []
    for part in (raw or "").split(","):
        name = part.strip().lower()
        if name in known and name not in out:
            out.append(name)
    if not out:
        out = [n for n in DEFAULT_PRIORITY if n in known]
    for name in list(DEFAULT_PRIORITY) + known:
        if name in known and name not in out:
            out.append(name)
    return out


def configured_priority(db_path: str) -> list[str]:
    """Every acquire source in the configured order (enabled or not)."""
    return parse_priority(get_config(db_path, CONFIG_KEY))


def source_enabled(db_path: str, name: str) -> bool:
    src = registry.get_source(name)
    return bool(src and src.is_enabled(db_path))


def enabled_order(db_path: str) -> list[str]:
    """Enabled acquire sources, in the configured order."""
    return [n for n in configured_priority(db_path) if source_enabled(db_path, n)]


def tidal_allowed(db_path: str) -> bool:
    """False when the operator has switched Tidal off: no Tidal search, download or
    token refresh may happen anywhere in the worker."""
    return source_enabled(db_path, "tidal")


def first_source(db_path: str) -> str | None:
    order = enabled_order(db_path)
    return order[0] if order else None


def next_after(db_path: str, current: str, tried: set[str] | None = None) -> str | None:
    """The next enabled source after ``current`` in the order, skipping ``tried``.

    Returns None when ``current`` was the last one (the track has exhausted every
    enabled source). A ``current`` that is not in the enabled order (e.g. it has
    just been disabled) starts the walk from the top.
    """
    order = enabled_order(db_path)
    tried = set(tried or ()) | {current}
    start = order.index(current) + 1 if current in order else 0
    for name in order[start:]:
        if name not in tried:
            return name
    return None
