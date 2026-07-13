"""SQLite adapter for the durable event queue port."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from soveren_agent_platform.queue import durable
from soveren_agent_platform.queue.contracts import QueueEvent
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteEventQueue(SQLiteAdapter):
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
        return await run_sqlite(
            self._conn,
            durable.enqueue,
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
        rows = await run_sqlite(
            self._conn,
            durable.claim_due,
            recipient=recipient,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        return [row_to_queue_event(row) for row in rows]

    async def mark_done(self, event_id: str, *, lease_token: str) -> bool:
        return await run_sqlite(
            self._conn,
            durable.mark_done,
            event_id,
            lease_token=lease_token,
        )

    async def renew_lease(
        self,
        event_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            durable.renew_lease,
            event_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
        )

    async def mark_retry(
        self,
        event_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str | None:
        return await run_sqlite(
            self._conn,
            durable.mark_retry,
            event_id,
            lease_token=lease_token,
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
        lease_token=row["lease_token"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
    )
