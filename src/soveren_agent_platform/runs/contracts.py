"""Agent run persistence contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from soveren_agent_platform.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class PlannerRunClaim:
    id: str
    status: str
    acquired: bool
    lease_token: str | None
    output: dict[str, Any] | None


class RunStore(Protocol):
    async def claim(
        self,
        *,
        tenant_id: str,
        source_id: str,
        trigger_event_id: str,
        model: str,
        prompt_version: str,
        input_summary: str | None,
        input_fingerprint: str,
        stale_after_s: int,
    ) -> PlannerRunClaim: ...

    async def finalize(
        self,
        run_id: str,
        *,
        lease_token: str,
        status: str,
        output: dict[str, Any] | None,
    ) -> bool: ...


@runtime_checkable
class RunCheckpointStore(Protocol):
    """Optional checkpoint capability fenced by the current planner run lease."""

    async def load_checkpoint(
        self,
        run_id: str,
        *,
        lease_token: str,
    ) -> JsonObject | None: ...

    async def save_checkpoint(
        self,
        run_id: str,
        *,
        lease_token: str,
        checkpoint: JsonObject,
    ) -> bool: ...


class RunCheckpointLeaseLostError(RuntimeError):
    """The current planner lease no longer owns the checkpoint row."""
