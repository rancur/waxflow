# :headphones: WaxFlow

**All your music, flowing home. Sync your Spotify Liked Songs to Lexicon DJ with lossless FLAC downloads from Tidal.**

![Version](https://img.shields.io/badge/version-v2.0.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Parity](https://img.shields.io/badge/parity-tracking-orange)

---

## What It Does

1. **Polls Spotify** for new Liked Songs on a configurable interval
2. **Scans your existing music library** to avoid re-downloading tracks you already own
3. **Matches tracks to Tidal** using ISRC codes, metadata search, and fuzzy title/artist matching
4. **Downloads lossless FLAC** from Tidal via the `tiddl` CLI at max quality
5. **Verifies downloads** are genuinely lossless with `ffprobe` codec checks and chromaprint fingerprinting
6. **Imports into Lexicon DJ** with automatic playlist creation organized by month and year
7. **Tracks parity** between your Spotify library and Lexicon in real time

<!-- ![Dashboard Screenshot](docs/dashboard.png) -->

---

## Features

### Spotify Integration
- OAuth-based authentication from the web UI
- Automatic polling for new Liked Songs (configurable interval)
- Full liked songs import on first connection
- Tracks Spotify metadata: ISRC, duration, album art, added date

### Intelligent Matching
- **5-layer deduplication pipeline**: ISRC file index, Lexicon database lookup, on-disk library scan, Tidal ISRC search, Tidal metadata search
- ISRC-based matching (guaranteed same recording, 1.0 confidence)
- Fuzzy title/artist matching with Unicode normalization (accented characters, special chars)
- Remix/edit suffix stripping for base title comparison
- Artist name splitting and order-independent matching (handles "feat.", "x", "vs", "&")
- Duration-based confidence scoring to avoid wrong versions
- Match review UI for manual approval/rejection of borderline matches

### Lossless Downloads
- Direct Tidal API downloads via `tiddl` CLI (no external downloader service)
- FLAC at master quality by default
- Tidal device code authentication from the Settings page
- Configurable download path and batch sizes
- Automatic retry on transient failures

### Verification
- `ffprobe` codec analysis to confirm files are genuinely lossless
- Chromaprint audio fingerprinting for content verification
- Duration matching between Spotify metadata and downloaded file
- Files that fail verification are flagged for re-download

### Lexicon DJ Sync
- Automatic playlist creation organized by month/year (e.g., "March 2026")
- Year-level folder hierarchy in Lexicon
- Track import via Lexicon REST API
- Playlist assignment and cue/tag triggers
- Tracks already in Lexicon are detected and skipped (no duplicate downloads)

### Library Scan Mode
- On first launch, scans your entire music library before downloading anything
- Matches existing files by ISRC, title, and artist against Spotify tracks
- Switches to download mode only when you click "Start Downloads"
- Prevents unnecessary re-downloads of tracks you already own

### Web Dashboard
- 8 pages: Dashboard, Tracks, Match Review, Downloads, Missing Tracks, Errored Tracks, Playlists, Settings
- Real-time parity percentage tracking (Spotify vs. Lexicon)
- Monthly progress chart showing sync velocity
- Pipeline stage breakdown (new, matching, downloading, verifying, organizing, complete)
- Activity feed with detailed event logging
- Service health indicators for API, worker, Lexicon, and Tidal

### Self-Healing
- 30-minute monitoring loop (`monitor-parity.sh`) detects stalled pipelines and errors
- Automatic container restart on API failure
- 2-hour cooldown between repeated fixes to prevent repair loops
- Worker stall detection (5-minute threshold)
- Error track retry with configurable backoff

### Notifications
- Webhook support for parity milestones
- Detailed activity log for every pipeline event (matches, downloads, errors)
- Service health dashboard with real-time status

---

## Architecture

```
Spotify API --> sync-worker --> [Scan Library] --> [Match via Tidal] --> [Download via tiddl] --> [Verify] --> [Lexicon API]
                                       |
                                 sync-api (FastAPI + SQLite)
                                       |
                                 sync-web (Next.js Dashboard)
```

| Service | Port | Description |
|---------|------|-------------|
| `sync-web` | 8400 | Next.js web dashboard |
| `sync-api` | 8402 | FastAPI REST API + SQLite database |
| `sync-worker` | 8403 | Background pipeline processor + health endpoint |

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/rancur/waxflow.git
cd waxflow
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your Spotify Developer App credentials:

```env
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_REDIRECT_URI=http://your-ip:8402/api/spotify/callback
NEXT_PUBLIC_API_URL=http://your-ip:8402
LEXICON_API_URL=http://lexicon-host:48624
```

### 3. Start the services

```bash
docker compose up -d
```

### 4. Connect your accounts

1. Visit `http://your-ip:8400`
2. Go to **Settings** and connect your Spotify account (OAuth redirect)
3. Connect your Tidal account using the device code flow from Settings
4. The system starts scanning your existing music library automatically

### 5. Start syncing

Once the library scan completes, click **"Start Downloads"** from the dashboard to switch from scan mode to full sync mode. New liked songs will be downloaded, verified, and imported into Lexicon automatically.

---

## Configuration

All settings are configurable from the **Settings** page in the web UI. Key settings include:

| Setting | Description | Default |
|---------|-------------|---------|
| Sync Mode | `scan` (library match only) or `full` (download new tracks) | `scan` |
| Poll Interval | How often to check Spotify for new liked songs | Configurable |
| Download Quality | Tidal download quality | `master` (FLAC) |
| Music Library Path | Path to your music library (inside container) | `/music` |
| Lexicon API URL | Lexicon DJ REST API endpoint | `http://localhost:48624` |

---

## Web UI Pages

| Page | Description |
|------|-------------|
| **Dashboard** | Parity meter, pipeline stage breakdown, service health, activity feed, monthly progress chart |
| **Tracks** | Browse all tracked songs with filtering by pipeline stage, match status, and search |
| **Match Review** | Review and approve/reject borderline Tidal matches; manual match override |
| **Downloads** | Active and recent downloads, retry failed downloads, download statistics |
| **Missing Tracks** | Tracks that failed matching -- upload manual files or bulk-ignore |
| **Errored Tracks** | Pipeline errors with details; retry or ignore individual tracks |
| **Playlists** | Lexicon playlist overview organized by month/year; trigger playlist sync |
| **Settings** | Spotify/Tidal account connection, sync mode toggle, all configuration options |

---

## Self-Healing Monitor

The `scripts/monitor-parity.sh` script runs every 30 minutes and performs:

1. **API health check** -- restarts containers if the API is unresponsive
2. **Lexicon connectivity check** -- verifies Lexicon DJ API is reachable
3. **Pipeline stall detection** -- flags if the worker hasn't processed tracks in 5+ minutes
4. **Error accumulation check** -- alerts if error count is growing
5. **Cooldown enforcement** -- 2-hour minimum between repeated fixes to prevent loops

The worker itself exposes a `/health` endpoint on port 8403 that reports `ok`, `starting`, or `stalled` status.

---

## Docker Compose

Three services, all with health checks and automatic restart:

```yaml
services:
  sync-api:        # FastAPI REST API
    ports: ["8402:8402"]
    volumes:
      - sync-data:/app/data
      - /path/to/music:/music

  sync-worker:     # Background pipeline processor
    ports: ["8403:8403"]
    volumes:
      - sync-data:/app/data
      - /path/to/music:/music
      - /path/to/downloads:/downloads
    depends_on: [sync-api]

  sync-web:        # Next.js dashboard
    ports: ["8400:3000"]
    depends_on: [sync-api]

volumes:
  sync-data:       # Shared SQLite database
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SPOTIFY_CLIENT_ID` | Spotify Developer App client ID | *required* |
| `SPOTIFY_CLIENT_SECRET` | Spotify Developer App client secret | *required* |
| `SPOTIFY_REDIRECT_URI` | OAuth callback URL | `http://localhost:8402/api/spotify/callback` |
| `LEXICON_API_URL` | Lexicon DJ REST API base URL | `http://localhost:48624` |
| `NEXT_PUBLIC_API_URL` | Public API URL for the web frontend | `http://localhost:8402` |
| `INTERNAL_API_URL` | Internal Docker network API URL | `http://sync-api:8402` |
| `CORS_ORIGINS` | Allowed CORS origins (comma-separated) | `http://localhost:8400,http://localhost:8400` |
| `MUSIC_LIBRARY_PATH` | Music library path inside container | `/music` |
| `SLS_DB_PATH` | SQLite database path | `/app/data/sync.db` |
| `SLS_HEALTH_PORT` | Worker health check port | `8403` |
| `TIDARR_URL` | Legacy Tidarr fallback URL (optional) | `http://localhost:8484` |
| `TIDDL_PATH` | tiddl config directory | `/tiddl-config` |

---

## API Endpoints

### Dashboard
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/dashboard` | Full dashboard stats: parity, pipeline stages, activity |
| GET | `/api/dashboard/monthly` | Monthly sync progress data |

### Tracks
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/tracks` | List tracks with filtering and pagination |
| GET | `/api/tracks/parity` | Parity statistics |
| GET | `/api/tracks/errors` | Tracks in error state |
| GET | `/api/tracks/{id}` | Single track detail |
| GET | `/api/tracks/{id}/activity` | Activity log for a track |
| PATCH | `/api/tracks/{id}` | Update track metadata |
| POST | `/api/tracks/{id}/retry` | Retry a failed track |
| POST | `/api/tracks/{id}/ignore` | Ignore a track |
| POST | `/api/tracks/{id}/unignore` | Un-ignore a track |
| POST | `/api/tracks/bulk-ignore` | Bulk-ignore tracks |

### Matching
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/matching/review` | Tracks pending match review |
| POST | `/api/matching/{id}/approve` | Approve a match |
| POST | `/api/matching/{id}/reject` | Reject a match |
| POST | `/api/matching/{id}/skip` | Skip match review |
| POST | `/api/matching/{id}/manual` | Manual match override |

### Downloads
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/downloads` | All downloads |
| GET | `/api/downloads/active` | Currently downloading |
| GET | `/api/downloads/recent` | Recently completed |
| GET | `/api/downloads/stats` | Download statistics |
| POST | `/api/downloads/{id}/retry` | Retry a failed download |

### Spotify
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/spotify/auth` | Start OAuth flow |
| GET | `/api/spotify/callback` | OAuth callback |
| GET | `/api/spotify/status` | Connection status |
| POST | `/api/spotify/poll` | Trigger manual poll |

### Tidal
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/tidal/status` | Connection status |
| POST | `/api/tidal/auth/start` | Start device code flow |
| POST | `/api/tidal/auth/poll` | Poll device code status |

### Admin
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/settings` | All settings |
| PATCH | `/api/settings` | Update settings |
| GET | `/api/admin/health` | Health check |
| GET | `/api/admin/version` | Version info |
| GET | `/api/admin/sync-mode` | Current sync mode |
| POST | `/api/admin/sync-mode` | Change sync mode |
| POST | `/api/admin/update` | Trigger update |
| GET | `/api/admin/export` | Export database |

### Lexicon
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/lexicon/status` | Lexicon connection status |
| POST | `/api/lexicon/backup` | Trigger Lexicon backup |
| GET | `/api/lexicon/backups` | List backups |
| GET | `/api/lexicon/protected` | Protected tracks list |

### Playlists
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/playlists` | All playlists |
| GET | `/api/playlists/{id}` | Playlist detail |
| POST | `/api/playlists/sync` | Trigger playlist sync |

---

## Troubleshooting

### Spotify won't connect
- Verify `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` are correct in `.env`
- Ensure `SPOTIFY_REDIRECT_URI` matches the redirect URI in your Spotify Developer App settings
- The callback URL must be reachable from your browser (not just inside Docker)

### Tidal downloads fail
- Check Tidal connection status on the Settings page
- Device code auth expires -- re-authenticate if `hours_left` is negative
- Verify `tiddl` CLI is available inside the worker container (`docker exec sync-worker which tiddl`)

### Tracks stuck in "matching" stage
- If in scan mode, unmatched tracks go to "waiting" -- switch to full mode to start downloads
- Check Tidal API connectivity from the worker container
- Review the Match Review page for tracks that need manual approval

### Tracks stuck in "organizing"
- Verify Lexicon DJ is running and the API URL is correct
- Check `docker logs sync-worker` for Lexicon API errors
- Ensure the music library path is mounted correctly in both containers

### High error count
- Go to the Errored Tracks page to see specific failure reasons
- Use the retry button for transient failures
- Bulk-ignore tracks that are genuinely unavailable on Tidal (exclusives, region-locked)

### Worker shows "stalled" health status
- The worker reports stalled if no pipeline cycle completes within 5 minutes
- Check `docker logs sync-worker` for exceptions
- Restart the worker: `docker compose restart sync-worker`

### Database issues
- SQLite database is at `/app/data/sync.db` inside the `sync-data` volume
- Export via `GET /api/admin/export` for backup
- WAL mode is enabled for concurrent read/write safety

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| API | Python 3.12, FastAPI, SQLite (WAL mode) |
| Worker | Python 3.12, asyncio, tiddl CLI, ffprobe, chromaprint |
| Web UI | Next.js 14, Tailwind CSS, TypeScript |
| Downloads | tiddl (Tidal CLI downloader) |
| Auth | Spotify OAuth 2.0, Tidal device code flow |
| Deployment | Docker Compose (3 services + shared volume) |
| Monitoring | Self-healing bash monitor, HTTP health checks |
