"""Persistence contract for one accepted decision per source event."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class DecisionDispatchClaim:
    id: str
    status: str
    acquired: bool
    lease_token: str | None
    run_id: str | None
    model: str | None
    prompt_version: str | None
    decision: dict[str, Any] | None
    planner_result: dict[str, Any] | None
    dispatch_context: dict[str, Any] | None
    dispatch_result: dict[str, Any] | None


class DecisionDispatchStore(Protocol):
    async def claim(
        self,
        *,
        tenant_id: str,
        source_id: str,
        trigger_event_id: str,
        input_fingerprint: str,
        stale_after_s: int,
    ) -> DecisionDispatchClaim: ...

    async def accept(
        self,
        receipt_id: str,
        *,
        lease_token: str,
        run_id: str,
        model: str,
        prompt_version: str,
        decision: dict[str, Any],
        planner_result: dict[str, Any],
        dispatch_context: dict[str, Any],
    ) -> bool: ...

    async def complete(
        self,
        receipt_id: str,
        *,
        lease_token: str,
        dispatch_result: dict[str, Any],
    ) -> bool: ...

    async def release(self, receipt_id: str, *, lease_token: str) -> bool: ...
