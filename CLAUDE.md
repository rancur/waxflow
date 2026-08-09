# CLAUDE.md -- WaxFlow / Spotify-Lexicon-Sync

## Project Overview
WaxFlow syncs Spotify Liked Songs to Lexicon DJ with lossless FLAC downloads from Tidal. It runs as 3 Docker containers backed by a shared SQLite database.

## Architecture
```
Spotify API --> sync-worker --> [Scan Library] --> [Match via Tidal] --> [Download via tiddl] --> [Verify] --> [Lexicon API]
                                       |
                                 sync-api (FastAPI + SQLite)
                                       |
                                 sync-web (Next.js Dashboard)
```

| Service | Port | Stack |
|---------|------|-------|
| sync-api | 8402 | Python 3.12, FastAPI, SQLite (WAL mode) |
| sync-worker | 8403 | Python 3.12, asyncio, tiddl CLI, ffprobe, chromaprint |
| sync-web | 8400 | Next.js 14, Tailwind CSS, TypeScript |

## Key Directories
- `sync-api/` -- FastAPI REST API: routes, services, models, database
- `sync-api/routes/` -- API endpoint modules (tracks, matching, downloads, spotify, tidal, admin, etc.)
- `sync-api/services/` -- Business logic (matcher, downloader, spotify client, lexicon sync, verifier)
- `sync-worker/` -- Background pipeline processor
- `sync-worker/tasks/` -- Individual pipeline tasks (poll_spotify, retry_unmatched, process_pipeline, etc.)
- `sync-web/` -- Next.js web dashboard (TypeScript, Tailwind)
- `scripts/` -- Ops scripts (deploy, monitor, backup, auto-update, deep-repair)
- `docker-compose.yml` -- Full stack orchestration

## Running the Stack
```bash
cp .env.example .env  # configure Spotify/Tidal/Lexicon credentials
docker compose up -d
```

## Testing
22 test modules exist across `sync-worker/tests/` and `sync-api/tests/`.
```bash
cd sync-worker && python3 -m pytest tests/ -v     # or: python3 -m unittest discover tests
cd sync-api    && python3 -m pytest tests/ -v
```
Most worker tests import `tasks.helpers`, which pulls in `spotipy` — run them inside
the worker image (`docker exec waxflow-worker python3 -m pytest tests/`) or in a venv
with `sync-worker/requirements.txt` installed. Leaf modules with no heavy imports
(e.g. `tasks/sync_gate.py`) run anywhere.

## Coding Standards
- Python 3.12, type hints encouraged
- FastAPI for all API routes, Pydantic models
- Conventional commits (feat:, fix:, chore:, docs:, refactor:, test:)
- Never hardcode secrets -- all credentials come from environment variables / `.env`
- SQLite with WAL mode; shared via Docker volume (`sync-data`)
- Scripts support `--dry-run` where applicable

## Docker
- `docker compose up -d` to start all services
- `docker compose logs -f sync-worker` to tail worker logs
- Shared volume `sync-data` holds the SQLite DB at `/app/data/sync.db`
- Health checks built into all services (30s API, 60s worker)

## Environment Variables
Key variables (see `.env.example` or README for full list):
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` -- Spotify Developer App
- `LEXICON_API_URL` -- Lexicon DJ REST API endpoint
- `MUSIC_LIBRARY_PATH` -- music library mount inside containers
- `SLS_DB_PATH` -- SQLite database path

## Key Concepts
- **Parity**: percentage of Spotify Liked Songs that exist in Lexicon
- **Pipeline stages**: new -> matching -> downloading -> verifying -> organizing -> complete
- **5-layer dedup**: ISRC file index, Lexicon DB lookup, on-disk scan, Tidal ISRC, Tidal metadata
- **Scan mode vs Full mode**: scan mode only matches existing library; full mode downloads new tracks

## The path contract (READ THIS BEFORE TOUCHING PATHS)
Rewritten 2026-08-08 after the library ended up split across two roots and Engine DJ
lost its entire database. Three rules, and they are load-bearing:

1. **The library root is `Database/`, not the share root.**
   `MUSIC_LIBRARY_PATH=/music/Database` — the worker writes there and `index_library`
   scans there. The bind mount deliberately still points at the share ROOT
   (`MUSIC_HOST_PATH=/volume1/music`) so Plex path translation
   (`plex_music_container_prefix=/music` -> `/volume1/music`) and the ~4,300 existing
   `/music/Database/...` `tracks.file_path` rows keep resolving. Do NOT "simplify"
   this by remapping the mount — it would break every one of those rows.

2. **Lexicon gets LOCAL Mac paths, never `/Volumes/*`.**
   `lexicon_library_path=/Users/willcurran/Music/Database`. Engine DJ refuses
   `/Volumes/*` locations, which is why tracks imported under the old SMB model were
   invisible to Engine export. Engine also stores paths relative to its own folder
   (`../Database/<Artist>/...`), so `Engine Library/` must remain a sibling of
   `Database/` inside `~/Music`.

3. **Replication is one-way NAS -> Mac, and imports wait for it.**
   `scripts/sync-nas-to-mac.sh` (launchd, 120 s) pulls `Database/` and `Input/` down;
   `tasks/sync_gate.py` holds each import until the file has landed. Synology Drive is
   NOT involved — two-way syncing the whole share produced 12 conflict copies of
   Engine's database and jammed permanently on SoundSwitch project files.

`scripts/repoint-lexicon-local.sh` exists to clean up pre-2026-08-08 rows. If it ever
finds work again, treat that as a regression signal, not routine maintenance.
