"""Tenant-scoped action approval orchestration."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from soveren_agent_platform.actions.store import approve_action, deny_action, get_action
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.storage.adapter import SQLiteAdapter, SQLiteConnectionHandle
from soveren_agent_platform.storage.sqlite import run_sqlite


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    action_id: str
    status: str
    transitioned: bool
    execution_event_id: str | None
    execution_event_created: bool


def approve_action_and_enqueue(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    action_id: str,
    approver_id: str,
    recipient: str = "actions",
) -> ApprovalResult:
    """Approve an action and durably enqueue execution in one transaction."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        action = get_action(conn, action_id, tenant_id=tenant_id, source_id=source_id)
        if action is None:
            raise KeyError(f"action not found: {action_id}")

        transitioned = approve_action(
            conn,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            approver_id=approver_id,
        )
        action = get_action(conn, action_id, tenant_id=tenant_id, source_id=source_id)
        if action is None:
            raise RuntimeError(f"action disappeared during approval: {action_id}")

        event_id: str | None = None
        event_created = False
        if action["status"] == "approved":
            idempotency_key = f"execute-action:{action_id}"
            event_id = enqueue(
                conn,
                tenant_id=tenant_id,
                recipient=recipient,
                message_type="ExecuteAction",
                payload={"action_id": action_id, "source_id": source_id},
                idempotency_key=idempotency_key,
                correlation_id=action_id,
                causation_id=action["source_event_id"],
            )
            event_created = event_id is not None
            if event_id is None:
                existing = conn.execute(
                    "SELECT id FROM event_queue WHERE tenant_id = ? AND idempotency_key = ?",
                    (tenant_id, idempotency_key),
                ).fetchone()
                if existing is None:
                    raise RuntimeError("approved action has no durable execution event")
                event_id = existing["id"]

        conn.execute("COMMIT")
        return ApprovalResult(
            action_id=action_id,
            status=str(action["status"]),
            transitioned=transitioned,
            execution_event_id=event_id,
            execution_event_created=event_created,
        )
    except Exception:
        conn.execute("ROLLBACK")
        raise


class SQLiteApprovalService(SQLiteAdapter):
    def __init__(self, handle: SQLiteConnectionHandle, *, recipient: str = "actions") -> None:
        super().__init__(handle)
        self.recipient = recipient

    async def approve(
        self,
        *,
        tenant_id: str,
        source_id: str,
        action_id: str,
        approver_id: str,
    ) -> ApprovalResult:
        return await run_sqlite(
            self._conn,
            approve_action_and_enqueue,
            tenant_id=tenant_id,
            source_id=source_id,
            action_id=action_id,
            approver_id=approver_id,
            recipient=self.recipient,
        )

    async def deny(
        self,
        *,
        tenant_id: str,
        source_id: str,
        action_id: str,
        approver_id: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            deny_action,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            approver_id=approver_id,
        )
