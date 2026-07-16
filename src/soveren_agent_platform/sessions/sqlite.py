"""SQLite adapters for session and mailbox stores."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

import soveren_agent_platform.sessions.events as event_store
import soveren_agent_platform.sessions.indexing as session_index
import soveren_agent_platform.sessions.mailbox as mailbox_store
import soveren_agent_platform.sessions.snapshots as snapshot_store
import soveren_agent_platform.sessions.store as session_store
from soveren_agent_platform.sessions.contracts import (
    MailboxItem,
    ReadySession,
    RuntimeSession,
    RuntimeSessionContextSnapshot,
    RuntimeSessionEvent,
    SessionIndexUpdate,
    SessionInspection,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteSessionStore(SQLiteAdapter):
    async def create(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        backend: str,
        backend_session_id: str,
        owner_id: str | None = None,
        title: str = "",
        cwd: str = "",
        status: str = "idle",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        return await run_sqlite(
            self._conn,
            session_store.insert_session,
            tenant_id=tenant_id,
            source_id=source_id,
            kind=kind,
            backend=backend,
            backend_session_id=backend_session_id,
            owner_id=owner_id,
            title=title,
            cwd=cwd,
            status=status,
            metadata=metadata,
        )

    async def get(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> RuntimeSession | None:
        row = await run_sqlite(
            self._conn,
            session_store.get_session,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        return row_to_session(row) if row is not None else None

    async def list_active(self, *, tenant_id: str, limit: int) -> list[RuntimeSession]:
        rows = await run_sqlite(
            self._conn,
            session_store.list_active_sessions,
            tenant_id=tenant_id,
            limit=limit,
        )
        return [row_to_session(row) for row in rows]

    async def set_status(
        self,
        session_id: str,
        status: str,
        *,
        tenant_id: str,
        source_id: str,
        current_action_id: str | None = None,
        last_error: str | None = None,
    ) -> None:
        await run_sqlite(
            self._conn,
            session_store.set_session_status,
            session_id,
            status,
            tenant_id=tenant_id,
            source_id=source_id,
            current_action_id=current_action_id,
            last_error=last_error,
        )


class SQLiteSessionMailboxStore(SQLiteAdapter):
    async def enqueue_prompt(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        prompt: str,
        action_id: str | None = None,
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        return await run_sqlite(
            self._conn,
            mailbox_store.enqueue_prompt,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            prompt=prompt,
            action_id=action_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
        )

    async def ready_sessions(self, *, tenant_id: str, limit: int) -> list[ReadySession]:
        rows = await run_sqlite(
            self._conn,
            mailbox_store.ready_sessions,
            tenant_id=tenant_id,
            limit=limit,
        )
        return [ReadySession(session_id=row["session_id"], source_id=row["source_id"]) for row in rows]

    async def claim_next(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> MailboxItem | None:
        row = await run_sqlite(
            self._conn,
            mailbox_store.claim_next,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        return row_to_mailbox_item(row) if row is not None else None

    async def mark_sent(
        self,
        mailbox_id: str,
        *,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.mark_sent,
            mailbox_id,
            tenant_id=tenant_id,
            source_id=source_id,
            result=result,
        )

    async def mark_accepted(
        self,
        mailbox_id: str,
        *,
        tenant_id: str,
        source_id: str,
        backend_receipt: dict[str, Any] | None = None,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.mark_accepted,
            mailbox_id,
            tenant_id=tenant_id,
            source_id=source_id,
            backend_receipt=backend_receipt,
        )

    async def defer_accepted(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        current_action_id: str | None,
        last_error: str,
        retry_after_s: int,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            mailbox_store.defer_accepted,
            mailbox_id,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            current_action_id=current_action_id,
            last_error=last_error,
            retry_after_s=retry_after_s,
        )

    async def defer_pending(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        current_action_id: str | None,
        last_error: str,
        retry_after_s: int,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.defer_pending,
            mailbox_id,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            current_action_id=current_action_id,
            last_error=last_error,
            retry_after_s=retry_after_s,
        )

    async def complete_delivery(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any],
        session_status: str,
        current_action_id: str | None = None,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.complete_delivery,
            mailbox_id,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            result=result,
            session_status=session_status,
            current_action_id=current_action_id,
        )

    async def fail_delivery(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        last_error: str,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.fail_delivery,
            mailbox_id,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            last_error=last_error,
        )

    async def requeue(
        self,
        mailbox_id: str,
        *,
        tenant_id: str,
        source_id: str,
        last_error: str,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.requeue,
            mailbox_id,
            tenant_id=tenant_id,
            source_id=source_id,
            last_error=last_error,
        )

    async def mark_failed(
        self,
        mailbox_id: str,
        *,
        tenant_id: str,
        source_id: str,
        last_error: str,
    ) -> None:
        await run_sqlite(
            self._conn,
            mailbox_store.mark_failed,
            mailbox_id,
            tenant_id=tenant_id,
            source_id=source_id,
            last_error=last_error,
        )

    async def fail_stale_sending(
        self,
        *,
        tenant_id: str,
        older_than_s: int,
        reason: str,
        limit: int,
    ) -> list[MailboxItem]:
        rows = await run_sqlite(
            self._conn,
            mailbox_store.fail_stale_sending,
            tenant_id=tenant_id,
            older_than_s=older_than_s,
            reason=reason,
            limit=limit,
        )
        return [row_to_mailbox_item(row) for row in rows]


class SQLiteSessionEventStore(SQLiteAdapter):
    async def record(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        direction: str,
        payload_text: str,
        action_id: str | None = None,
        marker: str | None = None,
    ) -> str:
        return await run_sqlite(
            self._conn,
            event_store.record_session_event,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            direction=direction,
            payload_text=payload_text,
            action_id=action_id,
            marker=marker,
        )

    async def recent(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
        limit: int,
    ) -> list[RuntimeSessionEvent]:
        rows = await run_sqlite(
            self._conn,
            _recent_events,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            limit=limit,
        )
        return [row_to_session_event(row) for row in rows]


class SQLiteSessionSnapshotStore(SQLiteAdapter):
    async def refresh(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> str | None:
        return await run_sqlite(
            self._conn,
            snapshot_store.refresh_snapshot,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )

    async def latest(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> RuntimeSessionContextSnapshot | None:
        row = await run_sqlite(
            self._conn,
            snapshot_store.latest_snapshot,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        return row_to_context_snapshot(row) if row is not None else None


class SQLiteSessionIndexStore(SQLiteAdapter):
    async def index_inspection(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        inspection: SessionInspection,
    ) -> SessionIndexUpdate:
        return await run_sqlite(
            self._conn,
            session_index.index_session_inspection,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            inspection=inspection,
        )


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
        accepted_at=row["accepted_at"],
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        run_after=int(row["run_after"]),
        backend_receipt=_json_dict(row["backend_receipt_json"]),
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


def _recent_events(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    tenant_id: str,
    source_id: str,
    limit: int,
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT event.* FROM runtime_session_events event"
            " JOIN runtime_sessions session ON session.id = event.session_id"
            " WHERE event.session_id = ? AND session.tenant_id = ? AND session.source_id = ?"
            " ORDER BY event.created_at DESC, event.rowid DESC LIMIT ?",
            (session_id, tenant_id, source_id, limit),
        )
    )


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
