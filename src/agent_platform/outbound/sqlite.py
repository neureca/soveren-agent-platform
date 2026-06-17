"""SQLite adapter for outbound queue storage."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from agent_platform.outbound import store
from agent_platform.outbound.contracts import OutboundMessage


class SQLiteOutboundQueue:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def enqueue(
        self,
        *,
        tenant_id: str,
        channel: str,
        destination_id: str,
        text: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        run_after: int | None = None,
        max_attempts: int = 5,
        correlation_id: str | None = None,
    ) -> str | None:
        return await asyncio.to_thread(
            store.enqueue_outbound,
            self.conn,
            tenant_id=tenant_id,
            channel=channel,
            destination_id=destination_id,
            text=text,
            idempotency_key=idempotency_key,
            payload=payload,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
        )

    async def claim_due(
        self,
        *,
        channel: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[OutboundMessage]:
        rows = await asyncio.to_thread(
            store.claim_due,
            self.conn,
            channel=channel,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        return [store.row_to_message(row) for row in rows]

    async def mark_sent(self, message_id: str, *, result: dict[str, Any] | None = None) -> None:
        await asyncio.to_thread(store.mark_sent, self.conn, message_id, result=result)

    async def mark_retry(self, message_id: str, *, run_after: int, last_error: str) -> None:
        await asyncio.to_thread(
            store.mark_retry,
            self.conn,
            message_id,
            run_after=run_after,
            last_error=last_error,
        )
