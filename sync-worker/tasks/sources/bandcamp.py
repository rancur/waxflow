"""Bandcamp source plugin (Phase 4).

Bandcamp sells lossless directly from artists and, for some releases, offers genuine
FREE / name-your-price downloads (where the artist set price 0). That makes Bandcamp
the one platform here where a *legit* free lossless acquisition can exist. However,
detecting a truly-free track reliably requires per-release scraping that can slip
into paid territory, so — per the hard constraint's "when unsure, make it a
buy-link" rule — this plugin ships SEARCH_LINK ONLY. A future, carefully-gated
free-only downloader can add ACQUIRE behind a real free/NYP check; until then we
never buy and never guess.

URL shape (verified July 2026): ``https://bandcamp.com/search?q=<terms>&item_type=t``
— the ``q`` query param plus ``item_type=t`` to scope results to tracks.
"""

from __future__ import annotations

from tasks.sources.base import Source, SourceCapability, SourceResult, TrackQuery
from tasks.sources.linkbuild import query_encode, search_terms

BANDCAMP_SEARCH = "https://bandcamp.com/search?q="


def build_url(q: TrackQuery) -> str:
    """Well-formed Bandcamp track-search URL for the query."""
    return f"{BANDCAMP_SEARCH}{query_encode(search_terms(q))}&item_type=t"


class BandcampSource(Source):
    name = "bandcamp"
    # SEARCH_LINK only for now. Free/NYP lossless genuinely exists on Bandcamp but is
    # not safe to auto-detect yet, so we default to a buy/search link (never buy).
    capabilities = frozenset({SourceCapability.SEARCH_LINK})
    priority = 50

    def is_enabled(self, db_path: str) -> bool:
        from tasks.helpers import get_config
        val = get_config(db_path, "source_bandcamp_enabled")
        if val is None:
            return True
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    def purchase_link(self, q: TrackQuery) -> SourceResult | None:
        terms = search_terms(q)
        if not terms:
            return None
        return SourceResult(
            source=self.name,
            confidence=0.5,
            kind="link",
            url=build_url(q),
            format_hint="lossless-purchase-or-free",
            price="varies",  # some Bandcamp tracks are name-your-price / free
        )
