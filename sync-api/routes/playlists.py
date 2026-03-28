import json
from fastapi import APIRouter, HTTPException

from db import get_db
from models import PlaylistOut, PlaylistDetailOut, TrackOut
from routes.tracks import row_to_track
from services.lexicon_sync import LexiconSyncService

router = APIRouter(prefix="/api/playlists", tags=["playlists"])


@router.get("")
async def list_playlists():
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM playlists ORDER BY year DESC, month DESC"
            ).fetchall()
            playlists = [PlaylistOut(**dict(r)) for r in rows]

            # Group by year
            by_year: dict[int, list] = {}
            for p in playlists:
                by_year.setdefault(p.year, []).append(p)

            return {"playlists": playlists, "by_year": {str(k): v for k, v in by_year.items()}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{playlist_id}", response_model=PlaylistDetailOut)
async def get_playlist(playlist_id: int):
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM playlists WHERE id = ?", (playlist_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Playlist not found")

            track_rows = conn.execute(
                """SELECT t.* FROM tracks t
                   JOIN playlist_tracks pt ON pt.track_id = t.id
                   WHERE pt.playlist_id = ?
                   ORDER BY pt.position ASC, pt.added_at ASC""",
                (playlist_id,),
            ).fetchall()

            tracks = [TrackOut(**row_to_track(r)) for r in track_rows]
            playlist_data = dict(row)
            playlist_data["tracks"] = tracks
            return PlaylistDetailOut(**playlist_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync")
async def sync_playlists():
    try:
        lexicon = LexiconSyncService()

        with get_db() as conn:
            # Get all tracks that are synced to Lexicon with spotify_added_at
            rows = conn.execute(
                """SELECT * FROM tracks
                   WHERE lexicon_status = 'synced'
                   AND spotify_added_at IS NOT NULL
                   ORDER BY spotify_added_at ASC"""
            ).fetchall()

            created_playlists = 0
            assigned_tracks = 0

            for row in rows:
                track = dict(row)
                added_at = track["spotify_added_at"]
                if not added_at or len(added_at) < 7:
                    continue

                # Parse year/month from spotify_added_at (ISO format)
                year = int(added_at[:4])
                month = int(added_at[5:7])

                # Ensure playlist exists
                pl = conn.execute(
                    "SELECT * FROM playlists WHERE year = ? AND month = ?",
                    (year, month),
                ).fetchone()

                if not pl:
                    import calendar
                    month_name = calendar.month_name[month]
                    playlist_name = f"{month:02d}. {month_name} {year}"
                    folder_name = str(year)

                    # Create in Lexicon
                    try:
                        lexicon_folder_id = await lexicon.create_folder(None, folder_name)
                        lexicon_playlist_id = await lexicon.create_playlist(lexicon_folder_id, playlist_name)
                    except Exception:
                        lexicon_folder_id = None
                        lexicon_playlist_id = None

                    conn.execute(
                        """INSERT INTO playlists (folder_name, playlist_name, year, month,
                           lexicon_folder_id, lexicon_playlist_id, track_count)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (folder_name, playlist_name, year, month,
                         lexicon_folder_id, lexicon_playlist_id),
                    )
                    created_playlists += 1
                    pl = conn.execute(
                        "SELECT * FROM playlists WHERE year = ? AND month = ?",
                        (year, month),
                    ).fetchone()

                # Assign track to playlist if not already
                existing = conn.execute(
                    "SELECT id FROM playlist_tracks WHERE playlist_id = ? AND track_id = ?",
                    (pl["id"], track["id"]),
                ).fetchone()

                if not existing:
                    position = conn.execute(
                        "SELECT COALESCE(MAX(position), 0) + 1 FROM playlist_tracks WHERE playlist_id = ?",
                        (pl["id"],),
                    ).fetchone()[0]
                    conn.execute(
                        "INSERT INTO playlist_tracks (playlist_id, track_id, position) VALUES (?, ?, ?)",
                        (pl["id"], track["id"], position),
                    )
                    conn.execute(
                        "UPDATE playlists SET track_count = track_count + 1 WHERE id = ?",
                        (pl["id"],),
                    )

                    # Sync to Lexicon playlist if IDs available
                    if pl["lexicon_playlist_id"] and track.get("lexicon_track_id"):
                        try:
                            await lexicon.add_track_to_playlist(
                                pl["lexicon_playlist_id"], track["lexicon_track_id"]
                            )
                        except Exception:
                            pass

                    assigned_tracks += 1

            conn.execute(
                "INSERT INTO activity_log (event_type, message, details) VALUES (?, ?, ?)",
                ("playlist_sync", "Playlist sync completed",
                 json.dumps({"created_playlists": created_playlists, "assigned_tracks": assigned_tracks})),
            )

            return {
                "status": "complete",
                "created_playlists": created_playlists,
                "assigned_tracks": assigned_tracks,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
