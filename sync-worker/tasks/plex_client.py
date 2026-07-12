"""Thin Plex Media Server HTTP client (WaxFlow v3 — Feature 4).

Deliberately dependency-light: talks to the Plex REST API over ``httpx`` with a
``X-Plex-Token`` header and ``Accept: application/json``, instead of pulling in
the heavy ``plexapi`` package. Only the handful of endpoints the Plex/Plexamp
mirror needs are wrapped:

  * list library sections + iterate a music section's tracks (for path matching),
  * targeted PATH-SCOPED library refresh (never a global full scan),
  * search a section (artist+title fuzzy fallback),
  * list / create / reconcile audio playlists (so monthly lists show in Plexamp).

The client is a READ-heavy consumer of Plex; the only writes it performs are to
Plex's OWN playlist objects and its own scan trigger. It never touches audio
files on disk.

Testability: the constructor accepts an optional ``httpx.BaseTransport`` so a
test can inject an ``httpx.MockTransport`` and assert the exact method / path /
query of every request the mirror makes — the tests exercise real request
shapes, not a stubbed-out client object.
"""

from __future__ import annotations

import logging
from typing import Iterator

import httpx

log = logging.getLogger("worker.plex_client")

# The Plex "library" agent identifier used when building the ``uri`` that adds
# items to a playlist: server://<machineId>/com.plexapp.plugins.library/...
_LIBRARY_AGENT = "com.plexapp.plugins.library"

# Plex metadata ``type`` code for an individual track.
PLEX_TYPE_TRACK = 10


class PlexClient:
    """Minimal Plex Media Server client over httpx.

    Args:
        base_url: e.g. ``http://192.168.1.221:32400``.
        token: the ``X-Plex-Token`` (self-generated from the server's
            Preferences.xml; stored in 1Password + app_config, never in git).
        timeout: per-request timeout in seconds.
        transport: optional httpx transport (tests inject an ``httpx.MockTransport``).
        machine_id: optional pre-known server machineIdentifier; fetched lazily
            from ``/identity`` when omitted.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        machine_id: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._machine_id = machine_id
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            transport=transport,
            headers={
                "X-Plex-Token": token,
                "Accept": "application/json",
            },
        )

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "PlexClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- low-level --------------------------------------------------------
    @staticmethod
    def _metadata(resp: httpx.Response) -> list[dict]:
        """Return the MediaContainer's item list (Metadata/Directory), or []."""
        resp.raise_for_status()
        try:
            container = resp.json().get("MediaContainer", {})
        except ValueError:
            return []
        # Tracks/playlists live under "Metadata"; sections under "Directory".
        return container.get("Metadata") or container.get("Directory") or []

    @staticmethod
    def _container(resp: httpx.Response) -> dict:
        resp.raise_for_status()
        try:
            return resp.json().get("MediaContainer", {})
        except ValueError:
            return {}

    def machine_id(self) -> str:
        """Server machineIdentifier (cached), needed to build playlist uris."""
        if not self._machine_id:
            resp = self._client.get("/identity")
            self._machine_id = self._container(resp).get("machineIdentifier", "")
        return self._machine_id

    def _metadata_uri(self, rating_keys: list[str]) -> str:
        """Build the ``server://…`` uri that references a set of library items."""
        keys = ",".join(str(k) for k in rating_keys)
        return f"server://{self.machine_id()}/{_LIBRARY_AGENT}/library/metadata/{keys}"

    # -- sections / tracks ------------------------------------------------
    def sections(self) -> list[dict]:
        """All library sections as ``{key, type, title}`` dicts."""
        resp = self._client.get("/library/sections")
        out = []
        for d in self._metadata(resp):
            out.append({"key": str(d.get("key")), "type": d.get("type"), "title": d.get("title")})
        return out

    @staticmethod
    def _track_file(meta: dict) -> str | None:
        """Extract the on-disk file path from a track's Media/Part."""
        for media in meta.get("Media", []) or []:
            for part in media.get("Part", []) or []:
                if part.get("file"):
                    return part["file"]
        return None

    def _track_dict(self, meta: dict) -> dict:
        return {
            "rating_key": str(meta.get("ratingKey")),
            "file": self._track_file(meta),
            "artist": meta.get("grandparentTitle") or meta.get("originalTitle"),
            "title": meta.get("title"),
        }

    def iter_section_tracks(self, section_id: str | int, page_size: int = 200) -> Iterator[dict]:
        """Paginate every track in a music section.

        Yields ``{rating_key, file, artist, title}``. Used to build a
        path -> ratingKey index once, rather than one lookup per track.
        """
        start = 0
        while True:
            resp = self._client.get(
                f"/library/sections/{section_id}/all",
                params={
                    "type": PLEX_TYPE_TRACK,
                    "X-Plex-Container-Start": start,
                    "X-Plex-Container-Size": page_size,
                },
            )
            container = self._container(resp)
            metas = container.get("Metadata") or []
            for meta in metas:
                yield self._track_dict(meta)
            total = int(container.get("totalSize", container.get("size", 0)) or 0)
            start += page_size
            if start >= total or not metas:
                break

    def search_tracks(self, section_id: str | int, query: str, limit: int = 20) -> list[dict]:
        """Fuzzy fallback: search a section for tracks matching ``query``."""
        resp = self._client.get(
            f"/library/sections/{section_id}/search",
            params={"type": PLEX_TYPE_TRACK, "query": query, "limit": limit},
        )
        return [self._track_dict(m) for m in self._metadata(resp)]

    # -- library scan (PATH-SCOPED only) ---------------------------------
    def refresh_path(self, section_id: str | int, path: str) -> bool:
        """Trigger a PATH-SCOPED library refresh for a single directory/file.

        Maps to ``PUT /library/sections/{id}/refresh?path=…``. This is the
        ONLY scan verb the mirror uses — a global ``/refresh`` with no path is a
        library-wide rescan (storm risk on a 5k+ track library) and is never
        issued here.
        """
        resp = self._client.put(
            f"/library/sections/{section_id}/refresh",
            params={"path": path},
        )
        ok = resp.status_code in (200, 201, 204)
        if not ok:
            log.warning("Plex path-scoped refresh failed for %s: HTTP %d", path, resp.status_code)
        return ok

    # -- playlists --------------------------------------------------------
    def list_audio_playlists(self) -> list[dict]:
        """All audio playlists as ``{rating_key, title}``."""
        resp = self._client.get("/playlists", params={"playlistType": "audio"})
        return [
            {"rating_key": str(m.get("ratingKey")), "title": m.get("title")}
            for m in self._metadata(resp)
        ]

    def playlist_items(self, playlist_rating_key: str | int) -> list[dict]:
        """Items in a playlist as ``{rating_key, playlist_item_id, title}``.

        ``playlist_item_id`` is the per-membership id required to remove an item
        (a track can appear via a distinct playlistItemID than its ratingKey).
        """
        resp = self._client.get(f"/playlists/{playlist_rating_key}/items")
        out = []
        for m in self._metadata(resp):
            out.append({
                "rating_key": str(m.get("ratingKey")),
                "playlist_item_id": str(m.get("playlistItemID")),
                "title": m.get("title"),
            })
        return out

    def create_audio_playlist(self, title: str, rating_keys: list[str]) -> str | None:
        """Create an audio playlist seeded with ``rating_keys``.

        Plex requires at least one item to create a non-smart playlist, so the
        caller must pass a non-empty list. Returns the new playlist ratingKey.
        """
        if not rating_keys:
            raise ValueError("create_audio_playlist requires at least one rating_key")
        resp = self._client.post(
            "/playlists",
            params={
                "type": "audio",
                "title": title,
                "smart": 0,
                "uri": self._metadata_uri(rating_keys),
            },
        )
        items = self._metadata(resp)
        if items:
            return str(items[0].get("ratingKey"))
        # Some Plex versions return the new playlist in the container root.
        return str(self._container(resp).get("ratingKey")) if resp.content else None

    def add_playlist_items(self, playlist_rating_key: str | int, rating_keys: list[str]) -> bool:
        """Add items to an existing playlist (PUT …/items?uri=…)."""
        if not rating_keys:
            return True
        resp = self._client.put(
            f"/playlists/{playlist_rating_key}/items",
            params={"uri": self._metadata_uri(rating_keys)},
        )
        return resp.status_code in (200, 201, 204)

    def remove_playlist_item(self, playlist_rating_key: str | int, playlist_item_id: str | int) -> bool:
        """Remove a single membership from a playlist (by playlistItemID)."""
        resp = self._client.delete(f"/playlists/{playlist_rating_key}/items/{playlist_item_id}")
        return resp.status_code in (200, 201, 204)

    def delete_playlist(self, playlist_rating_key: str | int) -> bool:
        resp = self._client.delete(f"/playlists/{playlist_rating_key}")
        return resp.status_code in (200, 201, 204)
