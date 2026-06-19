"""SQLite adapters for decision dispatch side effects."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

import soveren_agent_platform.actions.store as action_store
from soveren_agent_platform.actions.sqlite import SQLiteActionStore
from soveren_agent_platform.cron.sqlite import SQLiteCronStore
from soveren_agent_platform.decisions.effects import ActionDispatchResult, DecisionEffects
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.sessions.sqlite import SQLiteSessionMailboxStore


class SQLiteActionDispatchEffects:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def insert_action(
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
        enqueue_when_approved: bool = True,
    ) -> ActionDispatchResult:
        return await asyncio.to_thread(
            insert_action_and_execution_event,
            self.conn,
            tenant_id=tenant_id,
            kind=kind,
            payload=payload,
            run_id=run_id,
            approval_policy=approval_policy,
            source_id=source_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            enqueue_when_approved=enqueue_when_approved,
        )


def sqlite_decision_effects(conn: sqlite3.Connection) -> DecisionEffects:
    return DecisionEffects(
        actions=SQLiteActionStore(conn),
        outbound=SQLiteOutboundQueue(conn),
        events=SQLiteEventQueue(conn),
        session_mailbox=SQLiteSessionMailboxStore(conn),
        cron=SQLiteCronStore(conn),
        action_dispatch=SQLiteActionDispatchEffects(conn),
    )


def insert_action_and_execution_event(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    kind: str,
    payload: dict[str, Any],
    run_id: str | None = None,
    approval_policy: str = "manual",
    source_id: str | None = None,
    source_event_id: str | None = None,
    idempotency_key: str | None = None,
    enqueue_when_approved: bool = True,
) -> ActionDispatchResult:
    conn.execute("BEGIN IMMEDIATE")
    try:
        action_id, created = action_store.insert_action(
            conn,
            tenant_id=tenant_id,
            kind=kind,
            payload=payload,
            run_id=run_id,
            approval_policy=approval_policy,
            source_id=source_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
        )
        action = action_store.get_action(conn, action_id)
        status = action["status"] if action is not None else None
        if enqueue_when_approved and status == "approved":
            enqueue(
                conn,
                tenant_id=tenant_id,
                recipient="actions",
                message_type="ExecuteAction",
                payload={"action_id": action_id},
                idempotency_key=f"execute-action:{action_id}",
                correlation_id=action_id,
                causation_id=source_event_id,
            )
        conn.execute("COMMIT")
        return ActionDispatchResult(action_id=action_id, created=created, status=status)
    except Exception:
        conn.execute("ROLLBACK")
        raise
