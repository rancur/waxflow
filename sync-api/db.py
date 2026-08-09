import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("SLS_DB_PATH", "/app/data/sync.db")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # timeout=30 + PRAGMA busy_timeout make the API wait for a lock instead of
    # instantly raising "database is locked" when the worker holds a write lock
    # (the root cause of intermittent 500s on approve/reject during heavy soak).
    #
    # busy_timeout MUST match the connect timeout. It was 5000 ms against a
    # timeout=30 s, and busy_timeout is what actually governs, so the effective
    # wait was 5 s, not 30 — PATCH /api/settings still returned
    # {"detail":"database is locked"} whenever the worker was mid-index and had
    # to be retried by hand. Both are 30 s now.
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
