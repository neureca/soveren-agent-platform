"""Contracts for outbound channel senders."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True)
class OutboundMessage:
    id: str
    tenant_id: str
    channel: str
    destination_id: str
    text: str
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None


@dataclass(slots=True)
class SendResult:
    metadata: dict[str, Any] = field(default_factory=dict)


class ChannelSender(Protocol):
    async def send(self, message: OutboundMessage) -> SendResult:
        ...

