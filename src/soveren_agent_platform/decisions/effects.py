"""Runtime side-effect ports used by decision dispatchers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from soveren_agent_platform.actions.contracts import ActionStore
from soveren_agent_platform.cron.contracts import CronStore
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.outbound.contracts import OutboundQueue, ReplayableOutboundQueue
from soveren_agent_platform.queue.contracts import DurableQueue
from soveren_agent_platform.sessions.contracts import SessionMailboxStore


@dataclass(slots=True)
class ActionDispatchResult:
    action_id: str
    created: bool
    status: str | None


class ActionDispatchEffects(Protocol):
    async def insert_action(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        payload: JsonObject,
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
        enqueue_when_approved: bool = True,
    ) -> ActionDispatchResult:
        ...


class DecisionOutboundQueue(OutboundQueue, ReplayableOutboundQueue, Protocol):
    """Outbound queue contract required by durable decision handlers."""


@dataclass(slots=True)
class DecisionEffects:
    actions: ActionStore
    outbound: DecisionOutboundQueue
    events: DurableQueue
    session_mailbox: SessionMailboxStore
    cron: CronStore
    action_dispatch: ActionDispatchEffects | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outbound, ReplayableOutboundQueue):
            raise TypeError(
                "decision effects require an outbound queue with stable replay results"
            )
