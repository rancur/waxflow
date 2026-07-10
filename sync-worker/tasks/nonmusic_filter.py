"""Non-music ingest filter.

A DJ library (Lexicon) should contain music tracks only. Spotify's "Liked Songs"
can nonetheless carry non-music that must never enter the pipeline:

  * Podcast episodes / audiobook objects (Spotify item ``type`` is not ``track``).
  * Spoken-word / audiobook recordings published as ordinary tracks — e.g.
    LibriVox releases like "The Hound of the Baskervilles" appear as normal
    album tracks, so the only reliable signals are an unusually long duration and
    audiobook-ish keywords.

``is_nonmusic`` returns ``(True, reason)`` for anything that should be skipped, or
``(False, "")`` for a real music track. It is deliberately conservative and every
rule is data-driven so it can be unit-tested and tuned via config.
"""

import re

# Default: flag tracks longer than 30 minutes. DJ tracks/edits are essentially
# never this long; audiobook chapters and podcast episodes routinely are.
DEFAULT_MAX_DURATION_MS = 30 * 60 * 1000  # 1_800_000

# Strong audiobook / spoken-word markers. Kept tight to avoid false positives on
# legitimate music (e.g. a song literally titled "Chapters" won't match the
# "chapter <n>" pattern).
_KEYWORD_PATTERNS = [
    re.compile(r"\blibrivox\b", re.I),
    re.compile(r"\baudiobook\b", re.I),
    re.compile(r"\bunabridged\b", re.I),
    re.compile(r"\bpodcast\b", re.I),
    re.compile(r"\bspoken word\b", re.I),
    re.compile(r"\bchapter\s+\d+\b", re.I),
    re.compile(r"\bchapitre\s+\d+\b", re.I),
    re.compile(r"\bpart\s+\d+\s+of\s+\d+\b", re.I),
    re.compile(r"\bread by\b", re.I),
]


def is_nonmusic(
    track_meta: dict,
    max_duration_ms: int = DEFAULT_MAX_DURATION_MS,
) -> tuple[bool, str]:
    """Decide whether a Spotify item is non-music and must be skipped at ingest.

    ``track_meta`` keys (all optional): ``type`` (Spotify object type),
    ``duration_ms`` (int), ``title``, ``album``, ``artists`` (already-joined
    artist string). Returns ``(skip, reason)``.
    """
    # 1) Not a music track object at all (podcast episode / audiobook object).
    obj_type = (track_meta.get("type") or "track").strip().lower()
    if obj_type and obj_type != "track":
        return True, f"non_track_type:{obj_type}"
    if track_meta.get("episode") is True:
        return True, "episode"

    # 2) Audiobook/spoken-word keyword in title or album.
    haystack = " ".join(
        str(track_meta.get(k) or "") for k in ("title", "album")
    )
    for pat in _KEYWORD_PATTERNS:
        if pat.search(haystack):
            return True, f"keyword:{pat.pattern}"

    # 3) Duration sanity cap (blunt but effective; configurable).
    dur = track_meta.get("duration_ms")
    try:
        dur = int(dur) if dur is not None else None
    except (TypeError, ValueError):
        dur = None
    if dur is not None and max_duration_ms and dur > max_duration_ms:
        return True, f"long_duration:{dur}ms>{max_duration_ms}ms"

    return False, ""
