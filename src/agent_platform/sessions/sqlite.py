"""SQLite adapters for session and mailbox stores."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

import agent_platform.sessions.events as event_store
import agent_platform.sessions.mailbox as mailbox_store
import agent_platform.sessions.snapshots as snapshot_store
import agent_platform.sessions.store as session_store
from agent_platform.sessions.contracts import (
    MailboxItem,
    RuntimeSession,
    RuntimeSessionContextSnapshot,
    RuntimeSessionEvent,
)


class SQLiteSessionStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def get(self, session_id: str) -> RuntimeSession | None:
        row = await asyncio.to_thread(session_store.get_session, self.conn, session_id)
        return row_to_session(row) if row is not None else None

    async def list_active(self, *, tenant_id: str, limit: int) -> list[RuntimeSession]:
        rows = await asyncio.to_thread(
            session_store.list_active_sessions,
            self.conn,
            tenant_id=tenant_id,
            limit=limit,
        )
        return [row_to_session(row) for row in rows]

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


class SQLiteSessionEventStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def record(
        self,
        *,
        session_id: str,
        direction: str,
        payload_text: str,
        action_id: str | None = None,
        marker: str | None = None,
    ) -> str:
        return await asyncio.to_thread(
            event_store.record_session_event,
            self.conn,
            session_id=session_id,
            direction=direction,
            payload_text=payload_text,
            action_id=action_id,
            marker=marker,
        )

    async def recent(self, session_id: str, *, limit: int) -> list[RuntimeSessionEvent]:
        rows = await asyncio.to_thread(_recent_events, self.conn, session_id, limit)
        return [row_to_session_event(row) for row in rows]


class SQLiteSessionSnapshotStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def refresh(self, session_id: str) -> str | None:
        return await asyncio.to_thread(snapshot_store.refresh_snapshot, self.conn, session_id)

    async def latest(self, session_id: str) -> RuntimeSessionContextSnapshot | None:
        row = await asyncio.to_thread(snapshot_store.latest_snapshot, self.conn, session_id)
        return row_to_context_snapshot(row) if row is not None else None


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


def row_to_session_event(row: sqlite3.Row) -> RuntimeSessionEvent:
    return RuntimeSessionEvent(
        id=row["id"],
        session_id=row["session_id"],
        action_id=row["action_id"],
        direction=row["direction"],
        payload_text=row["payload_text"],
        marker=row["marker"],
        created_at=row["created_at"],
    )


def row_to_context_snapshot(row: sqlite3.Row) -> RuntimeSessionContextSnapshot:
    return RuntimeSessionContextSnapshot(
        id=row["id"],
        session_id=row["session_id"],
        version=row["version"],
        source_event_id=row["source_event_id"],
        source_range=_json_dict(row["source_range_json"]),
        summary=row["summary"],
        keywords=_json_list(row["keywords_json"]),
        entities=_json_list(row["entities_json"]),
        files=_json_list(row["files_json"]),
        cwd=row["cwd"],
        branch=row["branch"],
        topic_key=row["topic_key"],
        open_questions=_json_list(row["open_questions_json"]),
        last_user_intent=row["last_user_intent"],
        last_agent_state=row["last_agent_state"],
        confidence=row["confidence"],
        created_at=row["created_at"],
    )


def _recent_events(conn: sqlite3.Connection, session_id: str, limit: int) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM runtime_session_events"
        " WHERE session_id = ?"
        " ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (session_id, limit),
    ))


def _json_dict(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, (str, int, float))]
