"""SQLite adapter for outbound queue storage."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from soveren_agent_platform.outbound import store
from soveren_agent_platform.outbound.contracts import (
    OutboundEnqueueResult,
    OutboundMessage,
    OutboundRequest,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteOutboundQueue(SQLiteAdapter):
    async def enqueue_with_result(
        self,
        *,
        tenant_id: str,
        source_id: str,
        channel: str,
        destination_id: str,
        text: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        run_after: int | None = None,
        max_attempts: int = 5,
        correlation_id: str | None = None,
        ordering_key: str | None = None,
        ordering_position: int | None = None,
    ) -> OutboundEnqueueResult:
        return await run_sqlite(
            self._conn,
            store.enqueue_outbound_with_result,
            tenant_id=tenant_id,
            source_id=source_id,
            channel=channel,
            destination_id=destination_id,
            text=text,
            idempotency_key=idempotency_key,
            payload=payload,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            ordering_key=ordering_key,
            ordering_position=ordering_position,
        )

    async def enqueue(
        self,
        *,
        tenant_id: str,
        source_id: str,
        channel: str,
        destination_id: str,
        text: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        run_after: int | None = None,
        max_attempts: int = 5,
        correlation_id: str | None = None,
        ordering_key: str | None = None,
        ordering_position: int | None = None,
    ) -> str | None:
        return await run_sqlite(
            self._conn,
            store.enqueue_outbound,
            tenant_id=tenant_id,
            source_id=source_id,
            channel=channel,
            destination_id=destination_id,
            text=text,
            idempotency_key=idempotency_key,
            payload=payload,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            ordering_key=ordering_key,
            ordering_position=ordering_position,
        )

    async def enqueue_many(
        self,
        requests: Sequence[OutboundRequest],
    ) -> tuple[str | None, ...]:
        return await run_sqlite(
            self._conn,
            store.enqueue_outbound_many,
            requests,
        )

    async def claim_due(
        self,
        *,
        channel: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        tenant_id: str | None = None,
    ) -> list[OutboundMessage]:
        rows = await run_sqlite(
            self._conn,
            store.claim_due,
            channel=channel,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            tenant_id=tenant_id,
        )
        return [store.row_to_message(row) for row in rows]

    async def mark_sent(
        self,
        message_id: str,
        *,
        lease_token: str,
        result: dict[str, Any] | None = None,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_sent,
            message_id,
            lease_token=lease_token,
            result=result,
        )

    async def renew_lease(
        self,
        message_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.renew_lease,
            message_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
        )

    async def mark_sending(self, message_id: str, *, lease_token: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_sending,
            message_id,
            lease_token=lease_token,
        )

    async def mark_uncertain(
        self,
        message_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_uncertain,
            message_id,
            lease_token=lease_token,
            last_error=last_error,
        )

    async def mark_dead_letter(
        self,
        message_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_dead_letter,
            message_id,
            lease_token=lease_token,
            last_error=last_error,
        )

    async def mark_retry(
        self,
        message_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str | None:
        return await run_sqlite(
            self._conn,
            store.mark_retry,
            message_id,
            lease_token=lease_token,
            run_after=run_after,
            last_error=last_error,
        )
