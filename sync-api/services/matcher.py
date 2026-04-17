import logging
import os
import json
import httpx

from db import get_db

logger = logging.getLogger(__name__)

TIDARR_API = os.environ.get("TIDARR_URL", "http://localhost:8484")  # optional legacy fallback


class MatcherService:
    """Matches Spotify tracks to Tidal equivalents for lossless download.

    NOTE: This legacy matcher uses the Tidarr API for search.
    Primary matching now uses Tidal API directly in the sync-worker.
    """

    def __init__(self):
        self.tidarr_url = TIDARR_API

    async def match_track(self, track: dict) -> dict:
        """
        Try to match a track. Returns dict with:
        - matched: bool
        - tidal_id: str or None
        - confidence: float
        - source: str (isrc|metadata|failed)
        """
        # Strategy 1: ISRC lookup (highest confidence)
        if track.get("isrc"):
            result = await self._match_by_isrc(track["isrc"])
            if result:
                return {
                    "matched": True,
                    "tidal_id": result["tidal_id"],
                    "confidence": 0.95,
                    "source": "isrc",
                }

        # Strategy 2: Metadata search
        result = await self._match_by_metadata(track)
        if result:
            return {
                "matched": True,
                "tidal_id": result["tidal_id"],
                "confidence": result["confidence"],
                "source": "metadata",
            }

        return {
            "matched": False,
            "tidal_id": None,
            "confidence": 0.0,
            "source": "failed",
        }

    async def _match_by_isrc(self, isrc: str) -> dict | None:
        """Search Tidal by ISRC (via legacy Tidarr API)."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{self.tidarr_url}/api/search",
                    params={"query": isrc, "type": "track"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    tracks = data.get("tracks", data.get("items", []))
                    if tracks:
                        return {"tidal_id": str(tracks[0].get("id", ""))}
        except Exception:
            logger.warning("ISRC match failed for %s", isrc, exc_info=True)
        return None

    async def _match_by_metadata(self, track: dict) -> dict | None:
        """Search by artist + title."""
        artist = track.get("artist", "")
        title = track.get("title", "")
        if not artist or not title:
            return None

        query = f"{artist} {title}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{self.tidarr_url}/api/search",
                    params={"query": query, "type": "track"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    tracks = data.get("tracks", data.get("items", []))
                    if tracks:
                        best = tracks[0]
                        confidence = self._compute_confidence(track, best)
                        if confidence >= 0.7:
                            return {
                                "tidal_id": str(best.get("id", "")),
                                "confidence": confidence,
                            }

                    # Log fallback attempt
                    with get_db() as conn:
                        conn.execute(
                            """INSERT INTO fallback_attempts
                               (track_id, source, status, search_query, result_count)
                               VALUES (?, 'tidal_metadata', 'no_match', ?, ?)""",
                            (track.get("id", 0), query, len(tracks)),
                        )
        except Exception:
            logger.warning("Metadata match failed for '%s' by '%s'", title, artist, exc_info=True)
        return None

    def _compute_confidence(self, spotify_track: dict, tidal_track: dict) -> float:
        """Compute match confidence based on metadata similarity."""
        score = 0.0
        checks = 0

        # Title comparison
        sp_title = (spotify_track.get("title") or "").lower().strip()
        ti_title = (tidal_track.get("title", tidal_track.get("name", ""))).lower().strip()
        if sp_title and ti_title:
            checks += 1
            if sp_title == ti_title:
                score += 1.0
            elif sp_title in ti_title or ti_title in sp_title:
                score += 0.7

        # Artist comparison
        sp_artist = (spotify_track.get("artist") or "").lower().strip()
        ti_artist = ""
        if "artist" in tidal_track:
            if isinstance(tidal_track["artist"], dict):
                ti_artist = tidal_track["artist"].get("name", "").lower().strip()
            else:
                ti_artist = str(tidal_track["artist"]).lower().strip()
        elif "artists" in tidal_track:
            names = [a.get("name", "") for a in tidal_track.get("artists", [])]
            ti_artist = ", ".join(names).lower().strip()

        if sp_artist and ti_artist:
            checks += 1
            if sp_artist == ti_artist:
                score += 1.0
            elif sp_artist.split(",")[0].strip() in ti_artist:
                score += 0.7

        # Duration comparison (within 3 seconds)
        sp_dur = spotify_track.get("duration_ms", 0)
        ti_dur = tidal_track.get("duration", 0) * 1000 if tidal_track.get("duration") else 0
        if sp_dur and ti_dur:
            checks += 1
            diff = abs(sp_dur - ti_dur)
            if diff < 3000:
                score += 1.0
            elif diff < 10000:
                score += 0.5

        return round(score / max(checks, 1), 2)
