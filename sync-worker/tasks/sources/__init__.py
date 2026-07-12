"""WaxFlow v3 source-plugin abstraction (Phase A foundation).

A ``Source`` is a pluggable music origin (Tidal, Soulseek today; Beatport, Qobuz,
Bandcamp in Phase B) exposed behind one small interface so the pipeline can ask any
of them to search, acquire, or produce a purchase link without knowing the details.

Phase A wires only Tidal and Soulseek, wrapping the EXISTING pipeline logic behind
the interface with ZERO behavior change (see tasks/sources/tidal.py and
tasks/sources/soulseek.py). Nothing new is added to the live worker loop.
"""

from tasks.sources.base import (
    Source,
    SourceBackoff,
    SourceCapability,
    SourceResult,
    TrackQuery,
)

__all__ = [
    "Source",
    "SourceBackoff",
    "SourceCapability",
    "SourceResult",
    "TrackQuery",
]
