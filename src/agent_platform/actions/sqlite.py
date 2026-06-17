"""SQLite adapter for action lifecycle storage."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

import agent_platform.actions.store as store
from agent_platform.actions.contracts import ActionRecord


class SQLiteActionStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def insert(
        self,
        *,
        tenant_id: str,
        kind: str,
        payload: dict[str, Any],
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_id: str | None = None,
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        return await asyncio.to_thread(
            store.insert_action,
            self.conn,
            tenant_id=tenant_id,
            kind=kind,
            payload=payload,
            run_id=run_id,
            approval_policy=approval_policy,
            source_id=source_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
        )

    async def get(self, action_id: str) -> ActionRecord | None:
        row = await asyncio.to_thread(store.get_action, self.conn, action_id)
        return row_to_action(row) if row is not None else None

    async def approve(self, action_id: str, *, approver_id: str) -> bool:
        return await asyncio.to_thread(store.approve_action, self.conn, action_id, approver_id=approver_id)

    async def deny(self, action_id: str, *, approver_id: str) -> bool:
        return await asyncio.to_thread(store.deny_action, self.conn, action_id, approver_id=approver_id)

    async def mark_executing(self, action_id: str) -> bool:
        return await asyncio.to_thread(store.mark_executing, self.conn, action_id)

    async def mark_queued(self, action_id: str, *, result: dict[str, Any] | None = None) -> None:
        await asyncio.to_thread(store.mark_queued, self.conn, action_id, result=result)

    async def mark_executed(self, action_id: str, *, result: dict[str, Any]) -> None:
        await asyncio.to_thread(store.mark_executed, self.conn, action_id, result=result)

    async def mark_failed(self, action_id: str, *, error: str) -> None:
        await asyncio.to_thread(store.mark_failed, self.conn, action_id, error=error)


def row_to_action(row: sqlite3.Row) -> ActionRecord:
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
    except Exception:
        payload = {}
    return ActionRecord(
        id=row["id"],
        tenant_id=row["tenant_id"],
        run_id=row["run_id"],
        kind=row["kind"],
        payload=payload,
        status=row["status"],
        approval_policy=row["approval_policy"],
        source_id=row["source_id"],
        source_event_id=row["source_event_id"],
        idempotency_key=row["idempotency_key"],
        approved_by=row["approved_by"],
        last_error=row["last_error"],
    )
