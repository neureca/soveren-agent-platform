"""Persistence contract for one accepted decision per source event."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from soveren_agent_platform.json_types import JsonObject


class Decision(Protocol):
    kind: str

    @property
    def payload(self) -> JsonObject: ...


@dataclass(frozen=True, slots=True)
class DecisionDispatchClaim:
    id: str
    status: str
    acquired: bool
    lease_token: str | None
    run_id: str | None
    model: str | None
    prompt_version: str | None
    decision: JsonObject | None
    planner_result: JsonObject | None
    dispatch_context: JsonObject | None
    dispatch_result: JsonObject | None


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
        decision: JsonObject,
        planner_result: JsonObject,
        dispatch_context: JsonObject,
    ) -> bool: ...

    async def complete(
        self,
        receipt_id: str,
        *,
        lease_token: str,
        dispatch_result: JsonObject,
    ) -> bool: ...

    async def release(self, receipt_id: str, *, lease_token: str) -> bool: ...
