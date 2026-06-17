"""SQLite adapter for inbound batch storage."""
from __future__ import annotations

import asyncio
import sqlite3

from agent_platform.batching.contracts import BatchDecision, BatchState, InboundMessage
from agent_platform.batching.rules import DEFAULT_MAX_COUNT, DEFAULT_MAX_WINDOW_S, DEFAULT_QUIET_WINDOW_S
from agent_platform.batching.store import (
    append_inbound_message,
    load_state,
    mark_routed,
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

    async def mark_routed(self, batch_id: str) -> bool:
        return await asyncio.to_thread(mark_routed, self.conn, batch_id)
