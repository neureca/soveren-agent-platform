"""Lifecycle support for consumer-facing asynchronous SQLite adapters."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Self, cast

from soveren_agent_platform.storage.sqlite import open_sqlite, run_sqlite


def _close_connection(conn: sqlite3.Connection) -> None:
    conn.close()


@dataclass(frozen=True, slots=True)
class SQLiteConnectionHandle:
    conn: sqlite3.Connection
    owns_connection: bool


class SQLiteAdapter:
    """Own an SQLite connection opened without blocking the event loop."""

    def __init__(self, handle: SQLiteConnectionHandle) -> None:
        self._handle = handle
        self._closed = False

    @property
    def _conn(self) -> sqlite3.Connection:
        return self._handle.conn

    @classmethod
    async def open(cls, db_path: Path, /, *args: Any, **kwargs: Any) -> Self:
        conn = await asyncio.to_thread(open_sqlite, db_path)
        factory = cast(Callable[..., Self], cls)
        try:
            adapter = factory(SQLiteConnectionHandle(conn=conn, owns_connection=True), *args, **kwargs)
        except BaseException:
            await asyncio.to_thread(conn.close)
            raise
        return adapter

    @classmethod
    def _from_connection(cls, conn: sqlite3.Connection, /, *args: Any, **kwargs: Any) -> Self:
        factory = cast(Callable[..., Self], cls)
        return factory(SQLiteConnectionHandle(conn=conn, owns_connection=False), *args, **kwargs)

    @classmethod
    def _from_owned_connection(cls, conn: sqlite3.Connection, /, *args: Any, **kwargs: Any) -> Self:
        factory = cast(Callable[..., Self], cls)
        return factory(SQLiteConnectionHandle(conn=conn, owns_connection=True), *args, **kwargs)

    async def close(self) -> None:
        if self._closed or not self._handle.owns_connection:
            return
        self._closed = True
        await run_sqlite(self._conn, _close_connection)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
