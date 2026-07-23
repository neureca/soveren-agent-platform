"""SQLite adapters for decision dispatch side effects."""

from __future__ import annotations

import sqlite3

import soveren_agent_platform.actions.store as action_store
import soveren_agent_platform.decisions.receipt_store as receipt_store
from soveren_agent_platform.actions.sqlite import SQLiteActionStore
from soveren_agent_platform.cron.sqlite import SQLiteCronStore
from soveren_agent_platform.decisions.contracts import DecisionDispatchClaim
from soveren_agent_platform.decisions.effects import ActionDispatchResult, DecisionEffects
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.sessions.sqlite import SQLiteSessionMailboxStore
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteActionDispatchEffects(SQLiteAdapter):
    async def insert_action(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        payload: JsonObject,
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
        enqueue_when_approved: bool = True,
    ) -> ActionDispatchResult:
        return await run_sqlite(
            self._conn,
            insert_action_and_execution_event,
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


class SQLiteDecisionDispatchStore(SQLiteAdapter):
    async def claim(
        self,
        *,
        tenant_id: str,
        source_id: str,
        trigger_event_id: str,
        input_fingerprint: str,
        stale_after_s: int,
    ) -> DecisionDispatchClaim:
        return await run_sqlite(
            self._conn,
            receipt_store.claim_decision_dispatch,
            tenant_id=tenant_id,
            source_id=source_id,
            trigger_event_id=trigger_event_id,
            input_fingerprint=input_fingerprint,
            stale_after_s=stale_after_s,
        )

    async def accept(
        self,
        receipt_id: str,
        *,
        lease_token: str,
        run_id: str,
        model: str,
        prompt_version: str,
        decision: JsonObject,
        planner_result: JsonObject,
        dispatch_context: JsonObject,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            receipt_store.accept_decision_dispatch,
            receipt_id,
            lease_token=lease_token,
            run_id=run_id,
            model=model,
            prompt_version=prompt_version,
            decision=decision,
            planner_result=planner_result,
            dispatch_context=dispatch_context,
        )

    async def complete(
        self,
        receipt_id: str,
        *,
        lease_token: str,
        dispatch_result: JsonObject,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            receipt_store.complete_decision_dispatch,
            receipt_id,
            lease_token=lease_token,
            dispatch_result=dispatch_result,
        )

    async def release(self, receipt_id: str, *, lease_token: str) -> bool:
        return await run_sqlite(
            self._conn,
            receipt_store.release_decision_dispatch,
            receipt_id,
            lease_token=lease_token,
        )


def sqlite_decision_effects(conn: sqlite3.Connection) -> DecisionEffects:
    return DecisionEffects(
        actions=SQLiteActionStore._from_connection(conn),
        outbound=SQLiteOutboundQueue._from_connection(conn),
        events=SQLiteEventQueue._from_connection(conn),
        session_mailbox=SQLiteSessionMailboxStore._from_connection(conn),
        cron=SQLiteCronStore._from_connection(conn),
        action_dispatch=SQLiteActionDispatchEffects._from_connection(conn),
    )


def insert_action_and_execution_event(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    kind: str,
    payload: JsonObject,
    run_id: str | None = None,
    approval_policy: str = "manual",
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
        action = action_store.get_action(
            conn,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        status = action["status"] if action is not None else None
        if enqueue_when_approved and status == "approved":
            enqueue(
                conn,
                tenant_id=tenant_id,
                recipient="actions",
                message_type="ExecuteAction",
                payload={"action_id": action_id, "source_id": source_id},
                idempotency_key=f"execute-action:{action_id}",
                correlation_id=action_id,
                causation_id=source_event_id,
            )
        conn.execute("COMMIT")
        return ActionDispatchResult(action_id=action_id, created=created, status=status)
    except Exception:
        conn.execute("ROLLBACK")
        raise
