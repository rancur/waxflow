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
No test suite exists yet. This is a priority backlog item. When tests are added:
```bash
python3 -m pytest tests/ -v
```

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
