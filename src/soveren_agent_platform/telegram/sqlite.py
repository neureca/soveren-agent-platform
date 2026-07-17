"""SQLite adapter for registered Telegram chats."""

from __future__ import annotations

import sqlite3
import time

from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteTelegramChatRegistry(SQLiteAdapter):
    async def is_registered(self, *, tenant_id: str, chat_id: int) -> bool:
        return await run_sqlite(
            self._conn,
            _is_registered,
            tenant_id=tenant_id,
            chat_id=chat_id,
        )

    async def register(
        self,
        *,
        tenant_id: str,
        chat_id: int,
        registered_by_user_id: int,
    ) -> None:
        await run_sqlite(
            self._conn,
            _register,
            tenant_id=tenant_id,
            chat_id=chat_id,
            registered_by_user_id=registered_by_user_id,
        )

    async def revoke(self, *, tenant_id: str, chat_id: int) -> bool:
        return await run_sqlite(
            self._conn,
            _revoke,
            tenant_id=tenant_id,
            chat_id=chat_id,
        )


def _is_registered(conn: sqlite3.Connection, *, tenant_id: str, chat_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM telegram_chat_registrations WHERE tenant_id = ? AND chat_id = ? AND status = 'allowed'",
        (tenant_id, chat_id),
    ).fetchone()
    return row is not None


def _register(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    chat_id: int,
    registered_by_user_id: int,
) -> None:
    now = int(time.time())
    conn.execute(
        "INSERT INTO telegram_chat_registrations"
        " (tenant_id, chat_id, registered_by_user_id, status, created_at, updated_at)"
        " VALUES (?, ?, ?, 'allowed', ?, ?)"
        " ON CONFLICT(tenant_id,chat_id) DO UPDATE SET"
        "   registered_by_user_id = excluded.registered_by_user_id,"
        "   status = 'allowed',"
        "   updated_at = excluded.updated_at",
        (tenant_id, chat_id, registered_by_user_id, now, now),
    )


def _revoke(conn: sqlite3.Connection, *, tenant_id: str, chat_id: int) -> bool:
    updated = conn.execute(
        "UPDATE telegram_chat_registrations"
        " SET status = 'revoked', updated_at = ?"
        " WHERE tenant_id = ? AND chat_id = ? AND status = 'allowed'",
        (int(time.time()), tenant_id, chat_id),
    ).rowcount
    return updated == 1
