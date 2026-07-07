"""Durable queue contracts independent from a concrete broker/storage."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class QueueEvent:
    id: str
    tenant_id: str
    recipient: str
    message_type: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    causation_id: str | None = None


class DurableQueue(Protocol):
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
        ...

    async def claim_due(
        self,
        *,
        recipient: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[QueueEvent]:
        ...

    async def mark_done(self, event_id: str) -> None:
        ...

    async def mark_retry(self, event_id: str, *, run_after: int, last_error: str) -> str | None:
        ...
