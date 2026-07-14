"""Agent run persistence contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


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
