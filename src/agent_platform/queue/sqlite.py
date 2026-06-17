"""SQLite adapter for the durable event queue port."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

from agent_platform.queue import durable
from agent_platform.queue.contracts import QueueEvent


class SQLiteEventQueue:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def enqueue(
        self,
        *,
        tenant_id: str,
        recipient: str,
        message_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        priority: int = 100,
        run_after: int | None = None,
        max_attempts: int = 5,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> str | None:
        return await asyncio.to_thread(
            durable.enqueue,
            self.conn,
            tenant_id=tenant_id,
            recipient=recipient,
            message_type=message_type,
            payload=payload,
            idempotency_key=idempotency_key,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def claim_due(
        self,
        *,
        recipient: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[QueueEvent]:
        rows = await asyncio.to_thread(
            durable.claim_due,
            self.conn,
            recipient=recipient,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        return [row_to_queue_event(row) for row in rows]

    async def mark_done(self, event_id: str) -> None:
        await asyncio.to_thread(durable.mark_done, self.conn, event_id)

    async def mark_retry(self, event_id: str, *, run_after: int, last_error: str) -> None:
        await asyncio.to_thread(
            durable.mark_retry,
            self.conn,
            event_id,
            run_after=run_after,
            last_error=last_error,
        )


def row_to_queue_event(row: sqlite3.Row) -> QueueEvent:
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
    except json.JSONDecodeError:
        payload = {"_raw": row["payload_json"]}
    return QueueEvent(
        id=row["id"],
        tenant_id=row["tenant_id"],
        recipient=row["recipient"],
        message_type=row["message_type"],
        payload=payload,
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
    )
