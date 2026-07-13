"""Contracts for outbound channel senders."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True)
class OutboundMessage:
    id: str
    tenant_id: str
    source_id: str
    channel: str
    destination_id: str
    text: str
    lease_token: str
    attempts: int
    max_attempts: int
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None


@dataclass(slots=True)
class SendResult:
    metadata: dict[str, Any] = field(default_factory=dict)


class SendNotStartedError(RuntimeError):
    """The sender can prove that no external send attempt was started."""


class ChannelSender(Protocol):
    async def send(self, message: OutboundMessage) -> SendResult: ...


class OutboundQueue(Protocol):
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
    ) -> str | None: ...

    async def claim_due(
        self,
        *,
        channel: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[OutboundMessage]: ...

    async def renew_lease(
        self,
        message_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool: ...

    async def mark_sending(self, message_id: str, *, lease_token: str) -> bool: ...

    async def mark_sent(
        self,
        message_id: str,
        *,
        lease_token: str,
        result: dict[str, Any] | None = None,
    ) -> bool: ...

    async def mark_uncertain(
        self,
        message_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool: ...

    async def mark_retry(
        self,
        message_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str | None: ...
