"""Agent run persistence contracts."""
from __future__ import annotations

from typing import Any, Protocol


class RunStore(Protocol):
    async def insert(
        self,
        *,
        tenant_id: str,
        trigger_event_id: str,
        model: str,
        prompt_version: str,
        input_summary: str | None,
    ) -> str:
        ...

    async def finalize(
        self,
        run_id: str,
        *,
        status: str,
        output: dict[str, Any] | None,
    ) -> None:
        ...
