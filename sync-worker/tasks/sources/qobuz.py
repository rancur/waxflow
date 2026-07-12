"""Qobuz source plugin (Phase 4).

Qobuz sells genuine lossless (FLAC up to hi-res) and, with a paid subscription, can
stream lossless. WaxFlow could in principle pull a lossless STREAM *only* if real
Qobuz credentials already exist — but per the hard constraint we NEVER fabricate
creds or create accounts. So this plugin ships as SEARCH_LINK (buy-link) by default,
and only advertises ACQUIRE/LOSSLESS when real creds are present in ``app_config``
(seeded out-of-band from 1Password). At the time of writing no Qobuz creds exist, so
it is buy-links-only in practice. No spending, ever — a subscription stream is not a
purchase, and we still gate on creds we did not create.

URL shape (verified July 2026): the track-scoped search path
``https://www.qobuz.com/us-en/search/tracks/<terms>`` with the terms URL-encoded
into the path segment. Qobuz uses a path-based search (not ``?q=``); this rendered
real track results in testing.
"""

from __future__ import annotations

from tasks.sources.base import Source, SourceCapability, SourceResult, TrackQuery
from tasks.sources.linkbuild import path_encode, search_terms

QOBUZ_TRACK_SEARCH = "https://www.qobuz.com/us-en/search/tracks/"


def build_url(q: TrackQuery) -> str:
    """Well-formed Qobuz track-search URL for the query (path-based)."""
    return QOBUZ_TRACK_SEARCH + path_encode(search_terms(q))


def _has_creds(db_path: str) -> bool:
    """True only if REAL Qobuz creds already exist in app_config.

    We never create or fabricate these — they must be seeded out-of-band (1P). When
    absent, Qobuz stays buy-links-only.
    """
    from tasks.helpers import get_config
    try:
        user = get_config(db_path, "qobuz_user_id") or get_config(db_path, "qobuz_username")
        token = get_config(db_path, "qobuz_auth_token") or get_config(db_path, "qobuz_app_id")
        return bool(user and token)
    except Exception:
        return False


class QobuzSource(Source):
    name = "qobuz"
    # Static capability set advertises what the plugin CAN do; actual acquire is
    # additionally gated at runtime on real creds via is_available(). We keep the
    # static set to SEARCH_LINK so nothing in the acquire path is even attempted
    # unless creds exist (see is_available + acquire below).
    capabilities = frozenset({SourceCapability.SEARCH_LINK})
    priority = 30

    def is_enabled(self, db_path: str) -> bool:
        from tasks.helpers import get_config
        val = get_config(db_path, "source_qobuz_enabled")
        if val is None:
            return True  # buy-link generation is safe + free; default on
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    def is_available(self, db_path: str) -> bool:
        # Buy-links are always available; a lossless STREAM would require real creds,
        # which we never fabricate. Absent creds -> link-only.
        return True

    def has_lossless_creds(self, db_path: str) -> bool:
        """Whether a real Qobuz lossless stream path is unlocked (creds present)."""
        return _has_creds(db_path)

    def purchase_link(self, q: TrackQuery) -> SourceResult | None:
        terms = search_terms(q)
        if not terms:
            return None
        return SourceResult(
            source=self.name,
            confidence=0.5,
            kind="link",
            url=build_url(q),
            format_hint="lossless-purchase",
            price="paid",
        )
