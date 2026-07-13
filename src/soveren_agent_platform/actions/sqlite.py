"""SQLite adapter for action lifecycle storage."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

import soveren_agent_platform.actions.store as store
from soveren_agent_platform.actions.contracts import ActionRecord
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteActionStore(SQLiteAdapter):
    async def insert(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        payload: dict[str, Any],
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        return await run_sqlite(
            self._conn,
            store.insert_action,
            tenant_id=tenant_id,
            kind=kind,
            payload=payload,
            run_id=run_id,
            approval_policy=approval_policy,
            source_id=source_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
        )

    async def get(self, action_id: str, *, tenant_id: str, source_id: str) -> ActionRecord | None:
        row = await run_sqlite(
            self._conn,
            store.get_action,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        return row_to_action(row) if row is not None else None

    async def approve(self, action_id: str, *, tenant_id: str, source_id: str, approver_id: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.approve_action,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            approver_id=approver_id,
        )

    async def deny(self, action_id: str, *, tenant_id: str, source_id: str, approver_id: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.deny_action,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            approver_id=approver_id,
        )

    async def mark_executing(self, action_id: str, *, tenant_id: str, source_id: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_executing,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )

    async def mark_queued(
        self,
        action_id: str,
        *,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any] | None = None,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_queued,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            result=result,
        )

    async def mark_executed(
        self,
        action_id: str,
        *,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any],
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_executed,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            result=result,
        )

    async def mark_failed(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_failed,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            error=error,
        )

    async def mark_retryable(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_retryable,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            error=error,
        )

    async def mark_uncertain(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_uncertain,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            error=error,
        )


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
