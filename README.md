# spotify-lexicon-sync

Self-hosted tool that syncs Spotify Liked Songs to Lexicon DJ with lossless FLAC downloads.

## What it does

1. Monitors your Spotify Liked Songs
2. Downloads lossless FLAC via Tidarr (Tidal)
3. Verifies files are genuinely lossless (spectral analysis + fingerprinting)
4. Organizes into Lexicon DJ playlists by month/year
5. Tracks parity between Spotify and Lexicon libraries

## Architecture

- **sync-api** (FastAPI) — REST API on port 8402
- **sync-worker** (Python) — Background pipeline processor
- **sync-web** (Next.js) — Web UI on port 8400

## Quick Start

```bash
cp .env.example .env
# Edit .env with your Spotify credentials
docker compose up -d
```

Then visit `http://your-nas-ip:8400` and connect your Spotify account.

## Auto-Update

Add to cron on NAS:
```
*/5 * * * * /path/to/spotify-lexicon-sync/scripts/auto-update.sh
```

Or trigger updates from the Settings page in the web UI.
