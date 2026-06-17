"""SQLite adapters for session and mailbox stores."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

import agent_platform.sessions.mailbox as mailbox_store
import agent_platform.sessions.store as session_store
from agent_platform.sessions.contracts import MailboxItem, RuntimeSession


class SQLiteSessionStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def get(self, session_id: str) -> RuntimeSession | None:
        row = await asyncio.to_thread(session_store.get_session, self.conn, session_id)
        return row_to_session(row) if row is not None else None

    async def set_status(
        self,
        session_id: str,
        status: str,
        *,
        current_action_id: str | None = None,
        last_error: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            session_store.set_session_status,
            self.conn,
            session_id,
            status,
            current_action_id=current_action_id,
            last_error=last_error,
        )


class SQLiteSessionMailboxStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def enqueue_prompt(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        prompt: str,
        action_id: str | None = None,
        source_event_id: str | None = None,
    ) -> tuple[str, bool]:
        return await asyncio.to_thread(
            mailbox_store.enqueue_prompt,
            self.conn,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            prompt=prompt,
            action_id=action_id,
            source_event_id=source_event_id,
        )

    async def ready_session_ids(self, *, tenant_id: str, limit: int) -> list[str]:
        return await asyncio.to_thread(
            mailbox_store.ready_session_ids,
            self.conn,
            tenant_id=tenant_id,
            limit=limit,
        )

    async def claim_next(self, session_id: str) -> MailboxItem | None:
        row = await asyncio.to_thread(mailbox_store.claim_next, self.conn, session_id)
        return row_to_mailbox_item(row) if row is not None else None

    async def mark_sent(self, mailbox_id: str, *, result: dict[str, Any] | None = None) -> None:
        await asyncio.to_thread(mailbox_store.mark_sent, self.conn, mailbox_id, result=result)

    async def requeue(self, mailbox_id: str, *, last_error: str) -> None:
        await asyncio.to_thread(mailbox_store.requeue, self.conn, mailbox_id, last_error=last_error)

    async def mark_failed(self, mailbox_id: str, *, last_error: str) -> None:
        await asyncio.to_thread(mailbox_store.mark_failed, self.conn, mailbox_id, last_error=last_error)

    async def fail_stale_sending(
        self,
        *,
        tenant_id: str,
        older_than_s: int,
        reason: str,
        limit: int,
    ) -> list[MailboxItem]:
        rows = await asyncio.to_thread(
            mailbox_store.fail_stale_sending,
            self.conn,
            tenant_id=tenant_id,
            older_than_s=older_than_s,
            reason=reason,
            limit=limit,
        )
        return [row_to_mailbox_item(row) for row in rows]


def row_to_session(row: sqlite3.Row) -> RuntimeSession:
    try:
        metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
    except Exception:
        metadata = {}
    return RuntimeSession(
        id=row["id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        owner_id=row["owner_id"],
        kind=row["kind"],
        backend=row["backend"],
        backend_session_id=row["backend_session_id"],
        title=row["title"],
        cwd=row["cwd"],
        status=row["status"],
        current_action_id=row["current_action_id"],
        last_error=row["last_error"],
        metadata=metadata,
    )


def row_to_mailbox_item(row: sqlite3.Row) -> MailboxItem:
    return MailboxItem(
        id=row["id"],
        session_id=row["session_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        source_event_id=row["source_event_id"],
        action_id=row["action_id"],
        prompt=row["prompt"],
        status=row["status"],
        last_error=row["last_error"],
    )
