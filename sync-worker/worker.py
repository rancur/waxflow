"""
Spotify-Lexicon Sync Worker
Async pipeline: poll Spotify -> match -> download -> verify -> sync to Lexicon.
Runs as standalone asyncio event loop with scheduled tasks.
"""

import asyncio
import json
import logging
import os
import signal
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

from tasks.poll_spotify import poll_spotify
from tasks.process_pipeline import process_pipeline
from tasks.parity_check import parity_check
from tasks.retry_unmatched import retry_unmatched
from tasks.cleanup_activity import cleanup_activity
from tasks.backup_lexicon import backup_lexicon
from tasks.index_library import index_library
from tasks.analyze_tracks import analyze_tracks
from tasks.helpers import get_config, get_db

DB_PATH = os.environ.get("SLS_DB_PATH", "/app/data/sync.db")
HEALTH_PORT = int(os.environ.get("SLS_HEALTH_PORT", "8403"))
STALL_THRESHOLD = 300  # 5 minutes

log = logging.getLogger("worker")

# Shared health state
_health_state = {
    "last_cycle_time": 0.0,
    "tracks_processed": 0,
}


class HealthHandler(BaseHTTPRequestHandler):
    """Lightweight HTTP handler for health and stats endpoints."""

    def log_message(self, format, *args):
        pass  # suppress request logging

    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/stats":
            self._handle_stats()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_health(self):
        now = time.time()
        last = _health_state["last_cycle_time"]
        if last == 0:
            status = "starting"
        elif now - last > STALL_THRESHOLD:
            status = "stalled"
        else:
            status = "ok"
        body = json.dumps({
            "status": status,
            "last_cycle": last,
            "tracks_processed": _health_state["tracks_processed"],
        })
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def _handle_stats(self):
        try:
            if os.path.exists(DB_PATH):
                with get_db(DB_PATH) as conn:
                    rows = conn.execute(
                        "SELECT pipeline_stage, COUNT(*) FROM tracks GROUP BY pipeline_stage"
                    ).fetchall()
                    stages = {r[0]: r[1] for r in rows}
            else:
                stages = {}
        except Exception:
            stages = {"error": "db_unavailable"}
        body = json.dumps(stages)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())


def _start_health_server():
    """Start the health HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("Health server listening on port %d", HEALTH_PORT)

# Graceful shutdown
shutdown_event = asyncio.Event()


def handle_signal(sig, frame):
    log.info(f"Received signal {sig}, shutting down...")
    shutdown_event.set()


async def run_task(name: str, coro, interval_key: str | None = None, default_interval: int = 30):
    """Run a task on a fixed interval. Reads interval from DB config if interval_key is set."""
    while not shutdown_event.is_set():
        interval = default_interval
        if interval_key:
            try:
                val = await asyncio.to_thread(get_config, DB_PATH, interval_key)
                if val:
                    interval = int(val)
            except Exception:
                pass

        start = time.monotonic()
        try:
            await coro(DB_PATH)
            if name == "process_pipeline":
                _health_state["last_cycle_time"] = time.time()
                try:
                    with get_db(DB_PATH) as conn:
                        count = conn.execute(
                            "SELECT COUNT(*) FROM tracks WHERE pipeline_stage = 'complete'"
                        ).fetchone()[0]
                        _health_state["tracks_processed"] = count
                except Exception:
                    pass
        except Exception as e:
            log.error(f"Task '{name}' failed: {e}", exc_info=True)

        elapsed = time.monotonic() - start
        sleep_time = max(0, interval - elapsed)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
            break  # shutdown requested
        except asyncio.TimeoutError:
            pass  # interval elapsed, run again


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log.info("Worker starting...")

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Start health endpoint
    _start_health_server()

    # Wait for DB to be ready (API container creates it)
    while not os.path.exists(DB_PATH):
        log.info("Waiting for database at %s...", DB_PATH)
        await asyncio.sleep(5)

    # Auto-detect: if tracks are already complete, we're not a fresh install — use full mode
    try:
        from tasks.helpers import get_db as _get_db, get_config as _get_config
        with _get_db(DB_PATH) as conn:
            complete = conn.execute("SELECT COUNT(*) FROM tracks WHERE pipeline_stage = 'complete'").fetchone()[0]
            if complete > 0:
                current_mode = _get_config(DB_PATH, "sync_mode")
                if current_mode != "full":
                    conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES ('sync_mode', 'full')")
                    log.info("Existing install detected (%d complete tracks). Auto-set sync_mode to 'full'.", complete)
    except Exception as e:
        log.warning("Failed to auto-detect sync mode: %s", e)

    log.info("Database found. Starting task loops.")

    # Launch all task loops concurrently
    tasks = [
        asyncio.create_task(
            run_task("poll_spotify", poll_spotify, interval_key="spotify_poll_interval_seconds", default_interval=300)
        ),
        asyncio.create_task(
            run_task("process_pipeline", process_pipeline, default_interval=10)
        ),
        asyncio.create_task(
            run_task("parity_check", parity_check, default_interval=600)
        ),
        asyncio.create_task(
            run_task("retry_unmatched", retry_unmatched, interval_key="retry_search_interval_seconds", default_interval=43200)
        ),
        asyncio.create_task(
            run_task("cleanup_activity", cleanup_activity, default_interval=86400)
        ),
        asyncio.create_task(
            run_task("backup_lexicon", backup_lexicon, interval_key="lexicon_backup_interval_seconds", default_interval=86400)
        ),
        asyncio.create_task(
            run_task("index_library", index_library, interval_key="library_index_interval_seconds", default_interval=3600)
        ),
        asyncio.create_task(
            run_task("analyze_tracks", analyze_tracks, interval_key="analyze_interval_seconds", default_interval=3600)
        ),
    ]

    # Wait until shutdown signal
    await shutdown_event.wait()
    log.info("Cancelling tasks...")

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    log.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
