"""Beatport source plugin (Phase 4) — BUY-LINKS ONLY.

Beatport is the leading electronic-music store. WaxFlow cannot auto-download from
it (paid, DRM-scoped) and — per the hard constraint — MUST NOT ever purchase. This
plugin therefore advertises SEARCH_LINK only: it turns a track query into an
accurate Beatport track-search URL a human can open to buy the lossless (WAV/AIFF/
FLAC) copy. No ACQUIRE, no LOSSLESS auto-fetch, no credentials, no spending.

URL shape (verified July 2026): the track-scoped search endpoint
``https://www.beatport.com/search/tracks?q=<terms>`` with the query in the ``q``
parameter (spaces -> ``+``). This narrows results to individual tracks rather than
the mixed all-types search.
"""

from __future__ import annotations

from tasks.sources.base import Source, SourceCapability, SourceResult, TrackQuery
from tasks.sources.linkbuild import query_encode, search_terms

BEATPORT_TRACK_SEARCH = "https://www.beatport.com/search/tracks?q="


def build_url(q: TrackQuery) -> str:
    """Well-formed Beatport track-search URL for the query."""
    return BEATPORT_TRACK_SEARCH + query_encode(search_terms(q))


class BeatportSource(Source):
    name = "beatport"
    # SEARCH_LINK only — deliberately NOT ACQUIRE/LOSSLESS. Beatport is a paid store
    # and this phase never buys.
    capabilities = frozenset({SourceCapability.SEARCH_LINK})
    priority = 40

    def is_enabled(self, db_path: str) -> bool:
        from tasks.helpers import get_config
        val = get_config(db_path, "source_beatport_enabled")
        if val is None:
            return True  # buy-links are safe + free to generate; default on
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    def purchase_link(self, q: TrackQuery) -> SourceResult | None:
        terms = search_terms(q)
        if not terms:
            return None
        return SourceResult(
            source=self.name,
            confidence=0.5,  # search link, not an exact-product match
            kind="link",
            url=build_url(q),
            format_hint="lossless-purchase",
            price="paid",
        )
