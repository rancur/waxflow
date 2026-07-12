"""Source registry (Phase A foundation).

Central list of available source plugins + priority-sorted views the pipeline (or
Phase B code) can iterate without hard-coding which sources exist. Registration is
static for Phase A (Tidal + Soulseek); Beatport/Qobuz/Bandcamp will register here
in Phase B. Enable/disable is per-source via ``app_config`` (each source's
``is_enabled(db_path)``), so the registry never needs a schema change to gate a
source.

Inert: constructing the registry has no side effects and nothing here is invoked
by the live worker loop yet.
"""

from __future__ import annotations

from tasks.sources.bandcamp import BandcampSource
from tasks.sources.base import Source, SourceCapability
from tasks.sources.beatport import BeatportSource
from tasks.sources.qobuz import QobuzSource
from tasks.sources.soulseek import SoulseekSource
from tasks.sources.tidal import TidalSource

# Static registry. Instances are cheap + stateless (all state lives in the DB), so
# a module-level singleton list is fine.
#
# ACQUIRE sources (Tidal, Soulseek) come first by priority; the Phase 4 SEARCH_LINK
# stores (Qobuz/Beatport/Bandcamp) generate buy-links only and NEVER auto-purchase.
_REGISTRY: list[Source] = [
    TidalSource(),
    SoulseekSource(),
    QobuzSource(),
    BeatportSource(),
    BandcampSource(),
]


def all_sources() -> list[Source]:
    """Every registered source, in registration order."""
    return list(_REGISTRY)


def _by_capability(cap: SourceCapability) -> list[Source]:
    return sorted(
        (s for s in _REGISTRY if s.has(cap)),
        key=lambda s: s.priority,
    )


def acquire_sources() -> list[Source]:
    """ACQUIRE-capable sources, priority-sorted (lowest number first)."""
    return _by_capability(SourceCapability.ACQUIRE)


def link_sources() -> list[Source]:
    """SEARCH_LINK-capable sources, priority-sorted (lowest number first)."""
    return _by_capability(SourceCapability.SEARCH_LINK)


def get_source(name: str) -> Source | None:
    """Look up a registered source by its ``name``."""
    for s in _REGISTRY:
        if s.name == name:
            return s
    return None


def enabled_acquire_sources(db_path: str) -> list[Source]:
    """ACQUIRE sources that are both enabled (app_config) and available, priority-sorted."""
    return [
        s for s in acquire_sources()
        if s.is_enabled(db_path) and s.is_available(db_path)
    ]


def enabled_link_sources(db_path: str) -> list[Source]:
    """SEARCH_LINK (buy-link) sources that are enabled, priority-sorted.

    These never acquire audio — they only produce buy/search links — so availability
    is just the enable toggle.
    """
    return [s for s in link_sources() if s.is_enabled(db_path)]
