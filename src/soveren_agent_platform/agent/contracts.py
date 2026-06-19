"""Contracts for the platform agent runtime."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class AgentEvent:
    id: str
    tenant_id: str
    recipient: str
    message_type: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    causation_id: str | None = None


class AgentHandler(Protocol):
    async def handle(self, event: AgentEvent) -> None:
        ...

