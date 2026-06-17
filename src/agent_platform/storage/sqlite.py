"""SQLite connection setup for platform-backed apps."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def open_sqlite(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection configured for explicit transactions and WAL."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {"autocommit": True, "check_same_thread": False}
    conn = sqlite3.connect(db_path, **kwargs)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn
