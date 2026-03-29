"""
Spotify-Lexicon Sync Worker
Async pipeline: poll Spotify -> match -> download -> verify -> sync to Lexicon.
Runs as standalone asyncio event loop with scheduled tasks.
"""

import asyncio
import logging
import os
import signal
import time

from tasks.poll_spotify import poll_spotify
from tasks.process_pipeline import process_pipeline
from tasks.parity_check import parity_check
from tasks.retry_unmatched import retry_unmatched
from tasks.cleanup_activity import cleanup_activity
from tasks.backup_lexicon import backup_lexicon
from tasks.index_library import index_library
from tasks.helpers import get_config

DB_PATH = os.environ.get("SLS_DB_PATH", "/app/data/sync.db")

log = logging.getLogger("worker")

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
