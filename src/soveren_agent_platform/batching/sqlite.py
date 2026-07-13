"""SQLite adapter for inbound batch storage."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.batching.contracts import BatchDecision, BatchState, InboundMessage
from soveren_agent_platform.batching.rules import DEFAULT_MAX_COUNT, DEFAULT_MAX_WINDOW_S, DEFAULT_QUIET_WINDOW_S
from soveren_agent_platform.batching.store import (
    append_inbound_message,
    load_state,
    route_batch,
    store_decision,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteBatchStore(SQLiteAdapter):
    async def append_inbound_message(self, message: InboundMessage) -> str | None:
        return await run_sqlite(self._conn, append_inbound_message, message)

    async def load_state(
        self,
        batch_id: str,
        *,
        tenant_id: str,
        source_id: str,
        quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
        max_window_s: int = DEFAULT_MAX_WINDOW_S,
        max_count: int = DEFAULT_MAX_COUNT,
    ) -> BatchState | None:
        return await run_sqlite(
            self._conn,
            load_state,
            batch_id,
            tenant_id=tenant_id,
            source_id=source_id,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )

    async def store_decision(
        self,
        batch_id: str,
        decision: BatchDecision,
        *,
        tenant_id: str,
        source_id: str,
        state: BatchState | None = None,
    ) -> None:
        await run_sqlite(
            self._conn,
            store_decision,
            batch_id,
            decision,
            tenant_id=tenant_id,
            source_id=source_id,
            state=state,
        )

    async def route_batch(
        self,
        batch_id: str,
        *,
        tenant_id: str,
        source_id: str,
        recipient: str,
        message_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            route_batch,
            batch_id,
            tenant_id=tenant_id,
            source_id=source_id,
            recipient=recipient,
            message_type=message_type,
            payload=payload,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
