"""Source-plugin interface + shared backoff helper (Phase A foundation).

Defines the small, stable contract every music source implements so the pipeline
can treat Tidal, Soulseek (and later Beatport/Qobuz/Bandcamp) uniformly:

    SourceCapability  — what a source can do (search-link / acquire / lossless).
    TrackQuery        — the normalized "what track do I want" request.
    SourceResult      — one candidate a source returned (a hit or a link).
    Source            — the base class: name, capabilities, priority + methods.
    SourceBackoff     — shared exponential-backoff attempt log on source_attempts.

Everything here is pure/inert: importing this module has no side effects and no
source is invoked by the live worker loop yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum

from tasks.helpers import get_db


class SourceCapability(Enum):
    """What a source is able to do. A source advertises a subset of these."""

    SEARCH_LINK = "search_link"  # can produce a human buy/download link
    ACQUIRE = "acquire"          # can fetch the actual audio file
    LOSSLESS = "lossless"        # the audio it provides can be genuinely lossless


@dataclass
class TrackQuery:
    """Normalized request describing the track we want a source to find.

    Built from a ``tracks`` row via ``from_row`` so call-sites stay decoupled from
    the DB schema.
    """

    artist: str = ""
    title: str = ""
    album: str | None = None
    isrc: str | None = None
    duration_ms: int | None = None
    spotify_id: str | None = None

    @classmethod
    def from_row(cls, track: dict) -> "TrackQuery":
        return cls(
            artist=track.get("artist") or "",
            title=track.get("title") or "",
            album=track.get("album"),
            isrc=track.get("isrc"),
            duration_ms=track.get("duration_ms"),
            spotify_id=track.get("spotify_id"),
        )


@dataclass
class SourceResult:
    """One candidate returned by a source — either an acquirable hit or a link.

    ``raw`` optionally carries the source-native payload (e.g. the raw Tidal track
    dict) so existing pipeline logic that needs the full response is not lost when
    it goes through the abstraction.
    """

    source: str
    confidence: float
    kind: str  # 'acquire' | 'link'
    url: str | None = None
    external_id: str | None = None
    price: str | None = None
    format_hint: str | None = None
    raw: dict | None = field(default=None, repr=False)


class Source:
    """Base class for a music source plugin.

    Subclasses set ``name``/``capabilities``/``priority`` and override the methods
    relevant to their capabilities. Lower ``priority`` number == tried first.

    Default method bodies are deliberately inert (return empty/None) so a source
    that only, say, produces links does not have to implement ``acquire``.
    """

    name: str = "base"
    capabilities: frozenset[SourceCapability] = frozenset()
    priority: int = 100

    # ----------------------------------------------------------------- capability
    def has(self, cap: SourceCapability) -> bool:
        return cap in self.capabilities

    # -------------------------------------------------------------- availability
    def is_enabled(self, db_path: str) -> bool:
        """Whether the operator has this source turned on (app_config toggle)."""
        return True

    def is_available(self, db_path: str) -> bool:
        """Whether the source is usable right now (creds/binary/host present)."""
        return True

    # -------------------------------------------------------------------- verbs
    def search(self, db_path: str, q: TrackQuery) -> list[SourceResult]:
        """Search for candidates matching ``q``. Empty list if none/unsupported."""
        return []

    def acquire(self, db_path: str, q: TrackQuery, result: SourceResult) -> str | None:
        """Fetch the audio for ``result`` and return the destination path.

        Only meaningful for ACQUIRE sources; returns None otherwise.
        """
        return None

    def purchase_link(self, q: TrackQuery) -> SourceResult | None:
        """Produce a human buy/download link for ``q``.

        Only meaningful for SEARCH_LINK sources; returns None otherwise.
        """
        return None


class SourceBackoff:
    """Shared exponential-backoff attempt log, written to ``source_attempts``.

    Records one row per (track, source) attempt and schedules the next retry with
    exponential backoff: ``base * factor**(attempt_no - 1)``, capped at ``MAX``.
    This is the forward replacement for the ad-hoc fallback_attempts bookkeeping;
    it is inert until a source actually calls ``record``.
    """

    BASE_SECONDS = 60
    FACTOR = 2
    MAX_SECONDS = 86_400  # 24h ceiling

    # ---------------------------------------------------------------- math
    @classmethod
    def delay_for(cls, attempt_no: int) -> int:
        """Backoff delay (seconds) for the given 1-based attempt number."""
        n = max(1, int(attempt_no))
        return min(cls.MAX_SECONDS, cls.BASE_SECONDS * (cls.FACTOR ** (n - 1)))

    # -------------------------------------------------------------- queries
    @classmethod
    def attempt_count(cls, db_path: str, track_id: int, source: str) -> int:
        """How many attempts have already been recorded for (track, source)."""
        with get_db(db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM source_attempts WHERE track_id = ? AND source = ?",
                (track_id, source),
            ).fetchone()
            return int(row[0]) if row else 0

    @classmethod
    def is_ready(cls, db_path: str, track_id: int, source: str) -> bool:
        """True if there is no pending backoff window (safe to try now).

        Ready when there is no prior attempt, or the most recent attempt's
        ``next_retry_at`` is in the past / null.
        """
        with get_db(db_path) as conn:
            row = conn.execute(
                """SELECT next_retry_at FROM source_attempts
                    WHERE track_id = ? AND source = ?
                    ORDER BY id DESC LIMIT 1""",
                (track_id, source),
            ).fetchone()
        if not row or row[0] is None:
            return True
        try:
            nxt = datetime.fromisoformat(row[0])
        except (TypeError, ValueError):
            return True
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= nxt

    # --------------------------------------------------------------- mutation
    @classmethod
    def record(
        cls,
        db_path: str,
        track_id: int,
        source: str,
        status: str,
        error: str | None = None,
        search_query: str | None = None,
        result_count: int | None = None,
    ) -> dict:
        """Append an attempt row with the next exponential-backoff window.

        Returns the computed ``{attempt_no, backoff_seconds, next_retry_at}``.
        """
        attempt_no = cls.attempt_count(db_path, track_id, source) + 1
        backoff = cls.delay_for(attempt_no)
        next_retry = datetime.now(timezone.utc) + timedelta(seconds=backoff)
        next_retry_iso = next_retry.isoformat()
        with get_db(db_path) as conn:
            conn.execute(
                """INSERT INTO source_attempts
                    (track_id, source, status, error, search_query, result_count,
                     attempt_no, backoff_seconds, next_retry_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    track_id, source, status, error, search_query, result_count,
                    attempt_no, backoff, next_retry_iso,
                ),
            )
        return {
            "attempt_no": attempt_no,
            "backoff_seconds": backoff,
            "next_retry_at": next_retry_iso,
        }
