"""MusicBrainz client — free, keyless secondary metadata source.

WaxFlow caches a liked track's ISRC + title/artist/album/duration at like-time.
When Spotify later removes the track, that cached metadata is all we have — and a
straight Tidal search on the original ISRC often misses because Tidal carries a
DIFFERENT release (hence a different ISRC) of the same recording. MusicBrainz maps
an ISRC to a *recording*, and a recording back to ALL of its ISRCs across every
release, plus a canonical title/artist. That is exactly the bridge the
metadata-fallback needs to re-find a removed track on Tidal.

Design notes:
  * Keyless. MusicBrainz asks only for a descriptive User-Agent (configurable via
    the ``musicbrainz_user_agent`` app_config key) and a courteous <=1 req/sec.
    Callers are responsible for pacing; this module does not sleep.
  * Read-only HTTP. Nothing here writes the DB or the filesystem.
  * Fail-soft. Every call returns ``None``/empty on any error or non-200 so a
    MusicBrainz outage can never break the worker loop.
"""

from __future__ import annotations

import logging

import httpx

log = logging.getLogger("worker.musicbrainz")

_BASE = "https://musicbrainz.org/ws/2"
_DEFAULT_UA = "WaxFlow/2.9 (https://github.com/rancur/waxflow)"
_TIMEOUT = 15.0


def _get(path: str, params: dict, user_agent: str) -> dict | None:
    """GET a MusicBrainz endpoint as JSON. Returns None on any non-200/error."""
    p = dict(params)
    p.setdefault("fmt", "json")
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            r = client.get(f"{_BASE}/{path}", params=p, headers={"User-Agent": user_agent})
        if r.status_code == 200:
            return r.json()
        if r.status_code != 404:
            # 404 = simply not catalogued; anything else is worth a breadcrumb.
            log.debug("MusicBrainz %s -> HTTP %s", path, r.status_code)
        return None
    except Exception as e:  # noqa: BLE001 — never let a MB hiccup break the pipeline
        log.debug("MusicBrainz %s failed: %s", path, e)
        return None


def _artist_from_credit(recording: dict) -> str:
    """Flatten a MusicBrainz artist-credit array into a display string."""
    ac = recording.get("artist-credit") or []
    return "".join((a.get("name", "") + a.get("joinphrase", "")) for a in ac).strip()


def recording_from_isrc(isrc: str, user_agent: str = _DEFAULT_UA) -> dict | None:
    """Resolve an ISRC to a canonical recording via MusicBrainz.

    Returns ``{mbid, title, artist, isrcs[]}`` for the first linked recording (its
    full ISRC set across every release), or None if the ISRC is not catalogued.
    """
    if not isrc:
        return None
    data = _get(f"isrc/{isrc.strip().upper()}", {}, user_agent)
    if not data:
        return None
    recs = data.get("recordings") or []
    if not recs:
        return None
    mbid = recs[0].get("id")
    if not mbid:
        return None
    return recording_detail(mbid, user_agent)


def recording_detail(mbid: str, user_agent: str = _DEFAULT_UA) -> dict | None:
    """Fetch a recording's canonical title/artist + its full ISRC set."""
    data = _get(f"recording/{mbid}", {"inc": "isrcs+artist-credits"}, user_agent)
    if not data:
        return None
    return {
        "mbid": mbid,
        "title": data.get("title"),
        "artist": _artist_from_credit(data),
        "isrcs": [i.upper() for i in (data.get("isrcs") or [])],
    }


def _lucene_escape(s: str) -> str:
    """Escape Lucene special chars for a MusicBrainz search query term."""
    out = []
    for ch in s or "":
        if ch in '+-&|!(){}[]^"~*?:\\/':
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def search_recording(
    artist: str,
    title: str,
    duration_ms: int | None = None,
    user_agent: str = _DEFAULT_UA,
    min_score: int = 90,
) -> dict | None:
    """Search MusicBrainz for a recording by artist+title (duration-tie-broken).

    Used when the cached ISRC is absent or not catalogued. Returns the best
    high-confidence recording as ``{mbid, title, artist, isrcs[]}`` (with its full
    ISRC set fetched), or None. Duration, when known, filters obviously-wrong hits
    (>15s off) so a same-name different-length track is not accepted.
    """
    if not (artist and title):
        return None
    q = f'recording:"{_lucene_escape(title)}" AND artist:"{_lucene_escape(artist)}"'
    data = _get("recording", {"query": q, "limit": 5}, user_agent)
    if not data:
        return None
    best = None
    for rec in data.get("recordings") or []:
        if (rec.get("score") or 0) < min_score:
            continue
        if duration_ms and rec.get("length"):
            if abs(int(rec["length"]) - int(duration_ms)) > 15000:
                continue
        best = rec
        break
    if not best or not best.get("id"):
        return None
    # Re-fetch the recording to get its authoritative full ISRC set.
    return recording_detail(best["id"], user_agent)
