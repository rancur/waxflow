"""Soulseek source plugin (Phase A foundation).

Wraps the EXISTING Soulseek lossless-fallback logic (``tasks.soulseek_fallback``
+ ``tasks.slskd_client``) behind the ``Source`` interface, with ZERO behavior
change. The fallback logic already lives in its own module, so this adapter is a
thin facade: it re-exports the exact functions the pipeline uses (so process_pipeline
imports its Soulseek surface from the sources package) and adds a ``SoulseekSource``
for the registry. No behavior is copied or altered — the same functions run.

Capabilities: ACQUIRE + LOSSLESS. Soulseek is the lossless FALLBACK after Tidal,
so it has a lower priority (higher number: 20) than Tidal.
"""

from __future__ import annotations

# Re-export the existing soulseek_fallback surface UNCHANGED. These are the exact
# functions process_pipeline used inline; importing them here lets the pipeline
# route its Soulseek calls through the sources package without any behavior change.
from tasks.soulseek_fallback import (  # noqa: F401  (re-exported facade)
    already_attempted,
    is_enabled,
    process_soulseek_fallback,
    queue_for_fallback,
    reject_nonlossless_for_import,
)
from tasks.sources.base import Source, SourceCapability, SourceResult, TrackQuery


# Alias matching the Source verb vocabulary; identical to process_soulseek_fallback.
def run_fallback(db_path: str) -> None:
    """Run one Soulseek fallback pass — identical to
    ``soulseek_fallback.process_soulseek_fallback(db_path)``."""
    return process_soulseek_fallback(db_path)


class SoulseekSource(Source):
    name = "soulseek"
    capabilities = frozenset({SourceCapability.ACQUIRE, SourceCapability.LOSSLESS})
    priority = 20  # lossless fallback — after Tidal

    def is_enabled(self, db_path: str) -> bool:
        # Delegates to the existing soulseek_fallback toggle (unchanged semantics).
        return is_enabled(db_path)

    def already_attempted(self, db_path: str, track_id: int) -> bool:
        return already_attempted(db_path, track_id)

    def queue(self, db_path: str, track_id: int, reason: str) -> None:
        return queue_for_fallback(db_path, track_id, reason)

    def reject_nonlossless_for_import(self, db_path: str, track: dict) -> bool:
        return reject_nonlossless_for_import(db_path, track)

    def run_fallback(self, db_path: str) -> None:
        """Process the queued Soulseek fallback batch (the pipeline stage)."""
        return process_soulseek_fallback(db_path)
