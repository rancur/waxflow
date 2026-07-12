"""WaxFlow v3 — Feature #1/#2 API surface: wanted tracks + their buy-links.

Read-only endpoints that surface the missing-track HUNTER's ``wanted`` ledger and
the ``purchase_links`` it generates, so buy-links are a real, usable WaxFlow feature
(for every installer — nothing here is user-specific) rather than just DB rows:

  * ``GET /api/wanted``            — wanted tracks (optionally filtered by state)
                                     each with its active buy-links.
  * ``GET /api/wanted/{id}/links`` — buy-links for a single track.

DESIGN: purely read-only SELECTs, no writes, no schema. Degrades gracefully if the
v3 tables are absent (returns empty), so it never 500s on a pre-v3 / partially
migrated database. It advertises NO auto-purchase — links are search/store URLs the
user opens themselves.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from db import get_db

router = APIRouter(prefix="/api", tags=["wanted"])

# States considered "still needs attention" (not yet resolved).
_OPEN_STATES = ("wanted", "sourcing", "exhausted")


def _table_exists(conn, name: str) -> bool:
    try:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone() is not None
    except Exception:
        return False


def _links_for(conn, track_id: int) -> list[dict]:
    if not _table_exists(conn, "purchase_links"):
        return []
    rows = conn.execute(
        """SELECT source, url, format_hint, price, confidence, status,
                  first_generated_at, last_refreshed_at
             FROM purchase_links
            WHERE track_id = ? AND status = 'active'
            ORDER BY source""",
        (track_id,),
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/wanted")
async def list_wanted(
    state: str | None = Query(
        default=None,
        description="Filter by wanted state (wanted|sourcing|exhausted|resolved). "
        "Omit for all open (non-resolved) items.",
    ),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """List wanted tracks with their buy-links.

    NO auto-purchase: ``links`` are store search/product URLs the user opens
    themselves. Always 200s; returns ``[]`` when the hunter has nothing queued or
    the v3 tables are absent.
    """
    with get_db() as conn:
        if not _table_exists(conn, "wanted"):
            return {"count": 0, "items": [], "buy_links_note": "Links only — WaxFlow never auto-purchases."}

        if state:
            where = "w.state = ?"
            params: tuple = (state,)
        else:
            where = "w.state IN (%s)" % ",".join("?" for _ in _OPEN_STATES)
            params = _OPEN_STATES

        rows = conn.execute(
            f"""SELECT w.id AS wanted_id, w.track_id, w.state, w.attempts,
                       w.reason, w.last_source, w.last_attempt_at, w.next_retry_at,
                       t.title, t.artist, t.album, t.isrc, t.spotify_id
                  FROM wanted w
                  LEFT JOIN tracks t ON t.id = w.track_id
                 WHERE {where}
                 ORDER BY w.updated_at DESC
                 LIMIT ?""",
            (*params, limit),
        ).fetchall()

        items = []
        for r in rows:
            item = dict(r)
            item["links"] = _links_for(conn, r["track_id"]) if r["track_id"] else []
            items.append(item)

    return {
        "count": len(items),
        "items": items,
        "buy_links_note": "Links only — WaxFlow never auto-purchases.",
    }


@router.get("/wanted/{track_id}/links")
async def wanted_links(track_id: int):
    """Buy-links for one track. Always 200s (empty list if none)."""
    with get_db() as conn:
        links = _links_for(conn, track_id)
    return {"track_id": track_id, "count": len(links), "links": links,
            "buy_links_note": "Links only — WaxFlow never auto-purchases."}
