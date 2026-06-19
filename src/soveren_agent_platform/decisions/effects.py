"""Runtime side-effect ports used by decision dispatchers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from soveren_agent_platform.actions.contracts import ActionStore
from soveren_agent_platform.cron.contracts import CronStore
from soveren_agent_platform.outbound.contracts import OutboundQueue
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
        kind: str,
        payload: dict[str, Any],
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_id: str | None = None,
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
        enqueue_when_approved: bool = True,
    ) -> ActionDispatchResult:
        ...


@dataclass(slots=True)
class DecisionEffects:
    actions: ActionStore
    outbound: OutboundQueue
    events: DurableQueue
    session_mailbox: SessionMailboxStore
    cron: CronStore
    action_dispatch: ActionDispatchEffects | None = None
