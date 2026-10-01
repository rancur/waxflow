"""Source registry (Phase A foundation).

Central list of available source plugins + ordered views the pipeline iterates
without hard-coding which sources exist. Static ``priority`` gives the DEFAULT order
(Soulseek, then Tidal, since 2.20.0); the live order is the user-selectable
``source_priority`` app_config key (tasks/sources/order.py). Registration is
static for Phase A (Tidal + Soulseek); Beatport/Qobuz/Bandcamp will register here
in Phase B. Enable/disable is per-source via ``app_config`` (each source's
``is_enabled(db_path)``), so the registry never needs a schema change to gate a
source.

Constructing the registry has no side effects. Since 2.20.0 the live pipeline
routes new tracks through it (process_pipeline._dispatch_acquire).
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
# ACQUIRE sources (Soulseek, Tidal) come first by priority; the Phase 4 SEARCH_LINK
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


def ordered_acquire_sources(db_path: str) -> list[Source]:
    """Every ACQUIRE source in the USER-CONFIGURED order (``source_priority``).

    Falls back to the static ``priority`` order when nothing is configured.
    """
    from tasks.sources import order  # late: order imports this module
    by_name = {s.name: s for s in acquire_sources()}
    return [by_name[n] for n in order.configured_priority(db_path) if n in by_name]


def enabled_acquire_sources(db_path: str) -> list[Source]:
    """ACQUIRE sources that are both enabled (app_config) and available, in the
    user-configured order (``source_priority``)."""
    return [
        s for s in ordered_acquire_sources(db_path)
        if s.is_enabled(db_path) and s.is_available(db_path)
    ]


def enabled_link_sources(db_path: str) -> list[Source]:
    """SEARCH_LINK (buy-link) sources that are enabled, priority-sorted.

    These never acquire audio — they only produce buy/search links — so availability
    is just the enable toggle.
    """
    return [s for s in link_sources() if s.is_enabled(db_path)]
