"""Tidal source plugin (Phase A foundation).

Wraps the EXISTING Tidal search + tiddl download logic behind the ``Source``
interface, with ZERO behavior change. The real implementations still live in
``tasks.process_pipeline`` (``_tidal_search`` and ``_download_track_via_tiddl``)
because the tiddl CLI bootstrap — auth conversion, TIDDL_PATH, _TIDDL_AVAILABLE —
is set up there at import time, and ``lossless_upgrade`` also imports those two
functions directly. This adapter therefore DELEGATES to them (a classic
strangler-fig seam) rather than copying the logic: a single implementation, one
behavior, wrapped so the pipeline can call it through the source abstraction and
so Phase B sources slot in beside it. A later idle-window refactor can move the
body in here without touching call-sites.

Delegation is intentional and load-bearing for byte-identical behavior; the
characterization tests assert the source path is identical to calling the inline
functions directly.

Capabilities: ACQUIRE + LOSSLESS. Tidal is the primary source (priority 10).
"""

from __future__ import annotations

from tasks.helpers import get_config
from tasks.sources.base import Source, SourceCapability, SourceResult, TrackQuery


# --------------------------------------------------------------------- raw passthrough
# These module-level helpers are the EXACT seam the pipeline hot path uses. They
# return the same raw shapes the inline functions always returned (list[dict] /
# dest path str) so swapping the call-sites is byte-identical. They delegate via a
# late import to avoid an import cycle with process_pipeline (which imports this
# package at module load).

def search_raw(query: str) -> list[dict]:
    """Raw Tidal search — identical to ``process_pipeline._tidal_search(query)``."""
    from tasks import process_pipeline
    return process_pipeline._tidal_search(query)


def acquire_raw(db_path: str, track: dict) -> str:
    """Raw tiddl download — identical to
    ``process_pipeline._download_track_via_tiddl(db_path, track)``."""
    from tasks import process_pipeline
    return process_pipeline._download_track_via_tiddl(db_path, track)


class TidalSource(Source):
    name = "tidal"
    capabilities = frozenset({SourceCapability.ACQUIRE, SourceCapability.LOSSLESS})
    priority = 10  # primary source — tried first

    # ------------------------------------------------------------- availability
    def is_enabled(self, db_path: str) -> bool:
        # Tidal is the primary source: default ON, overridable via app_config.
        val = get_config(db_path, "source_tidal_enabled")
        if val is None:
            return True
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    def is_available(self, db_path: str) -> bool:
        # Downloads require the tiddl CLI, whose availability the pipeline probes
        # at import time. Search alone works without it, but ACQUIRE does not.
        from tasks import process_pipeline
        return bool(getattr(process_pipeline, "_TIDDL_AVAILABLE", False))

    # -------------------------------------------------------------------- verbs
    def search(self, db_path: str, q: TrackQuery) -> list[SourceResult]:
        """Typed search over the raw Tidal results.

        Prefers ISRC as the query when present (matching the pipeline's ISRC-first
        strategy), else ``artist title``. Confidence is left at 0.0 here — the
        pipeline's own scorer (which needs the raw payload) makes the real call, so
        each result carries its native dict in ``raw``.
        """
        query = (q.isrc or f"{q.artist} {q.title}").strip()
        results: list[SourceResult] = []
        for item in search_raw(query):
            ext_id = item.get("id")
            results.append(
                SourceResult(
                    source=self.name,
                    confidence=0.0,
                    kind="acquire",
                    external_id=str(ext_id) if ext_id is not None else None,
                    format_hint="lossless",
                    raw=item,
                )
            )
        return results

    def acquire(self, db_path: str, q: TrackQuery, result: SourceResult | None = None) -> str | None:
        """Download the track and return the destination path.

        Builds the ``track`` dict the downloader expects from the query + chosen
        result's ``external_id`` (the Tidal id).
        """
        track: dict = {"artist": q.artist, "title": q.title}
        if result is not None and result.external_id:
            track["tidal_id"] = result.external_id
        return acquire_raw(db_path, track)
