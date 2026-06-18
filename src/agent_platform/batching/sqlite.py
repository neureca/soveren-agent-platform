"""SQLite adapter for inbound batch storage."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from agent_platform.batching.contracts import BatchDecision, BatchState, InboundMessage
from agent_platform.batching.rules import DEFAULT_MAX_COUNT, DEFAULT_MAX_WINDOW_S, DEFAULT_QUIET_WINDOW_S
from agent_platform.batching.store import (
    append_inbound_message,
    load_state,
    route_batch,
    store_decision,
)


class SQLiteBatchStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def append_inbound_message(self, message: InboundMessage) -> str | None:
        return await asyncio.to_thread(append_inbound_message, self.conn, message)

    async def load_state(
        self,
        batch_id: str,
        *,
        quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
        max_window_s: int = DEFAULT_MAX_WINDOW_S,
        max_count: int = DEFAULT_MAX_COUNT,
    ) -> BatchState | None:
        return await asyncio.to_thread(
            load_state,
            self.conn,
            batch_id,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )

    async def store_decision(
        self,
        batch_id: str,
        decision: BatchDecision,
        *,
        state: BatchState | None = None,
    ) -> None:
        await asyncio.to_thread(
            store_decision,
            self.conn,
            batch_id,
            decision,
            state=state,
        )

    async def route_batch(
        self,
        batch_id: str,
        *,
        tenant_id: str,
        recipient: str,
        message_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            route_batch,
            self.conn,
            batch_id,
            tenant_id=tenant_id,
            recipient=recipient,
            message_type=message_type,
            payload=payload,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
