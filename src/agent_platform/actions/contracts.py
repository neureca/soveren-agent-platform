"""Contracts for action execution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True)
class ActionRecord:
    id: str
    tenant_id: str
    kind: str
    payload: dict[str, Any]
    status: str
    approval_policy: str
    run_id: str | None = None
    source_id: str | None = None
    source_event_id: str | None = None
    idempotency_key: str | None = None
    approved_by: str | None = None
    last_error: str | None = None


@dataclass(slots=True)
class ActionExecutionResult:
    result: dict[str, Any] = field(default_factory=dict)
    status: str = "executed"


class ActionExecutor(Protocol):
    async def execute(self, action: ActionRecord) -> ActionExecutionResult:
        ...


class ActionStore(Protocol):
    async def insert(
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
    ) -> tuple[str, bool]:
        ...

    async def get(self, action_id: str) -> ActionRecord | None:
        ...

    async def approve(self, action_id: str, *, approver_id: str) -> bool:
        ...

    async def deny(self, action_id: str, *, approver_id: str) -> bool:
        ...

    async def mark_executing(self, action_id: str) -> bool:
        ...

    async def mark_queued(self, action_id: str, *, result: dict[str, Any] | None = None) -> None:
        ...

    async def mark_executed(self, action_id: str, *, result: dict[str, Any]) -> None:
        ...

    async def mark_failed(self, action_id: str, *, error: str) -> None:
        ...
