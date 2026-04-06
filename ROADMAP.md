# ROADMAP -- WaxFlow Feature Backlog

## High Priority

### Investigate and auto-categorize the 162 no-Tidal-match tracks
- Export the full list of unmatched tracks with metadata (artist, title, ISRC, album)
- Categorize by failure reason: Tidal region lock, exclusive to other platform, remix/edit variant, metadata mismatch
- Suggest alternative sources or manual match candidates for each category
- Build a report page in the web UI showing categorized unmatched tracks

### Add automatic Tidal match retry with fuzzy search for failed matches
- When ISRC lookup fails, automatically fall back to progressively fuzzier title/artist search
- Strip featuring artists, remix tags, parenthetical suffixes in retry passes
- Log each retry attempt with the search query used and why it failed
- Add configurable retry depth (number of fuzzy passes before giving up)

### Add comprehensive test suite for API endpoints
- pytest-based tests for all sync-api routes (tracks, matching, downloads, spotify, tidal, admin)
- Test pipeline stage transitions and edge cases
- Mock external APIs (Spotify, Tidal, Lexicon) for deterministic testing
- Add CI integration to run tests on every PR

## Medium Priority

### Add health check endpoint for API and Worker
- Extend `/api/admin/health` with detailed subsystem checks (DB connectivity, Tidal auth freshness, Spotify token validity, disk space)
- Add structured JSON response with per-subsystem status and latency
- Worker `/health` endpoint should report queue depth and last successful cycle time

### Add parity dashboard to web UI showing sync status breakdown
- Visual breakdown of pipeline stages with counts and percentages
- Historical parity trend chart (daily/weekly/monthly)
- Drill-down from parity percentage to specific tracks in each stage
- Export parity report as CSV

### Add automatic notification when parity drops below threshold
- Configurable parity threshold (e.g., alert if parity drops below 95%)
- Webhook notification (Discord, Slack, generic HTTP)
- Cooldown period to avoid notification spam
- Include delta information (what caused the drop -- new liked songs, failed downloads, etc.)

### Add support for Spotify playlist sync (not just Liked Songs)
- Allow selecting specific Spotify playlists to sync
- Map Spotify playlists to Lexicon playlists (configurable naming)
- Handle playlist additions and removals (bidirectional awareness)
- Playlist sync status on the dashboard

### Add duplicate detection across Spotify and Lexicon
- Detect duplicate tracks in Spotify Liked Songs (same ISRC, different album versions)
- Detect duplicate files in Lexicon library (same audio, different file paths)
- Suggest dedup actions: keep highest quality, keep preferred version
- Dedup report page in web UI

## Lower Priority

### Add audio fingerprint verification for all downloads (not just spot checks)
- Run chromaprint fingerprint comparison on every downloaded track vs Spotify reference
- Flag tracks where fingerprint confidence is below threshold
- Queue flagged tracks for manual review or re-download
- Track fingerprint match rates in dashboard stats

### Add download queue priority system (new tracks first)
- Priority levels: new liked songs > retry failures > backfill old tracks
- Configurable priority weights
- Dashboard visibility into queue ordering
- Manual priority override from the web UI

### Add automatic Lexicon playlist generation by genre/mood
- Analyze track metadata (genre, BPM, energy, key) from Spotify/Tidal
- Generate smart playlists in Lexicon based on configurable rules
- Update smart playlists as new tracks are synced
- Genre/mood tagging visible in the tracks list

### Add rate limiting and backoff for Tidal API calls
- Implement exponential backoff on 429 responses
- Per-endpoint rate limit tracking
- Configurable rate limit ceiling
- Dashboard metrics for API call rates and throttle events
