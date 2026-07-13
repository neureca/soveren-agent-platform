"""SQLite connection setup for platform-backed apps."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class PlatformSQLiteConnection(sqlite3.Connection):
    """SQLite connection with a lock shared by every async platform adapter."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.platform_lock = threading.RLock()


_EXTERNAL_CONNECTION_LOCK = threading.RLock()
_CONNECTION_SETUP_LOCK = threading.Lock()


def run_sqlite_sync(
    conn: sqlite3.Connection,
    operation: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Serialize one complete SQLite operation, including its transaction."""
    lock = conn.platform_lock if isinstance(conn, PlatformSQLiteConnection) else _EXTERNAL_CONNECTION_LOCK
    with lock:
        return operation(conn, *args, **kwargs)


async def run_sqlite(
    conn: sqlite3.Connection,
    operation: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    operation_task = asyncio.create_task(asyncio.to_thread(run_sqlite_sync, conn, operation, *args, **kwargs))
    try:
        return await asyncio.shield(operation_task)
    except asyncio.CancelledError as cancellation:
        cancellations: list[BaseException] = [cancellation]
        while not operation_task.done():
            try:
                await asyncio.shield(operation_task)
            except asyncio.CancelledError as repeated_cancellation:
                cancellations.append(repeated_cancellation)
        try:
            operation_task.result()
        except BaseException as operation_error:
            raise BaseExceptionGroup(
                "SQLite operation failed while its caller was being cancelled",
                [*cancellations, operation_error],
            ) from None
        raise cancellation


def open_sqlite(db_path: Path) -> PlatformSQLiteConnection:
    """Open a SQLite connection configured for explicit transactions and WAL."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {"autocommit": True, "check_same_thread": False}
    conn = sqlite3.connect(db_path, factory=PlatformSQLiteConnection, **kwargs)
    try:
        conn.row_factory = sqlite3.Row
        with _CONNECTION_SETUP_LOCK:
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA journal_mode=WAL").fetchone()
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except BaseException:
        conn.close()
        raise
