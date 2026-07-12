"""Shared buy-link URL helpers (Phase 4).

Small, pure helpers the SEARCH_LINK source plugins (Beatport / Qobuz / Bandcamp)
use to turn a ``TrackQuery`` into a well-formed store search URL. Kept in one place
so the query-normalization + encoding rules are identical across platforms and easy
to unit-test.

No network, no side effects — pure string building.
"""

from __future__ import annotations

import re
from urllib.parse import quote, quote_plus

from tasks.sources.base import TrackQuery


def search_terms(q: TrackQuery) -> str:
    """Collapse a query into a single "artist title" search string.

    Whitespace-normalized and trimmed. Empty parts are dropped so a query with only
    a title (or only an artist) still yields a usable term.
    """
    parts = [p for p in (q.artist or "", q.title or "") if p and p.strip()]
    raw = " ".join(parts)
    return re.sub(r"\s+", " ", raw).strip()


def dedup_key(source: str, q: TrackQuery) -> str:
    """Stable dedup key for a (source, track) buy-link.

    Prefers ISRC (globally unique per recording) when present so the same recording
    never gets two rows for one platform; falls back to the normalized search terms.
    Lower-cased for case-insensitive dedup.
    """
    ident = (q.isrc or search_terms(q)).strip().lower()
    return f"{source}:{ident}"


def query_encode(term: str) -> str:
    """URL-encode a search term for a ``?q=`` query parameter (spaces -> ``+``)."""
    return quote_plus(term)


def path_encode(term: str) -> str:
    """URL-encode a search term for use inside a URL path segment (spaces -> ``%20``)."""
    return quote(term, safe="")
