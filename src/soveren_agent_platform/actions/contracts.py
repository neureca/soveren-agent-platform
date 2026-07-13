"""Contracts for action execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

ActionResultStatus = Literal["executed", "queued", "retryable_failure", "permanent_failure"]


@dataclass(slots=True)
class ActionRecord:
    id: str
    tenant_id: str
    source_id: str
    kind: str
    payload: dict[str, Any]
    status: str
    approval_policy: str
    run_id: str | None = None
    source_event_id: str | None = None
    idempotency_key: str | None = None
    approved_by: str | None = None
    last_error: str | None = None


@dataclass(slots=True)
class ActionExecutionResult:
    result: dict[str, Any] = field(default_factory=dict)
    status: ActionResultStatus = "executed"
    error: str | None = None
    retry_after_s: int | None = None

    @classmethod
    def executed(cls, result: dict[str, Any] | None = None) -> "ActionExecutionResult":
        return cls(result=result or {}, status="executed")

    @classmethod
    def queued(cls, result: dict[str, Any] | None = None) -> "ActionExecutionResult":
        return cls(result=result or {}, status="queued")

    @classmethod
    def retryable_failure(
        cls,
        error: str,
        *,
        retry_after_s: int | None = None,
        result: dict[str, Any] | None = None,
    ) -> "ActionExecutionResult":
        return cls(
            result=result or {},
            status="retryable_failure",
            error=error,
            retry_after_s=retry_after_s,
        )

    @classmethod
    def permanent_failure(
        cls,
        error: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> "ActionExecutionResult":
        return cls(result=result or {"error": error}, status="permanent_failure", error=error)


class ActionExecutor(Protocol):
    async def execute(self, action: ActionRecord) -> ActionExecutionResult: ...


class ActionNotStartedError(RuntimeError):
    """The executor can prove that no externally visible action was started."""


class ActionStore(Protocol):
    async def insert(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        payload: dict[str, Any],
        run_id: str | None = None,
        approval_policy: str = "manual",
        source_event_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]: ...

    async def get(self, action_id: str, *, tenant_id: str, source_id: str) -> ActionRecord | None: ...

    async def approve(self, action_id: str, *, tenant_id: str, source_id: str, approver_id: str) -> bool: ...

    async def deny(self, action_id: str, *, tenant_id: str, source_id: str, approver_id: str) -> bool: ...

    async def mark_executing(self, action_id: str, *, tenant_id: str, source_id: str) -> bool: ...

    async def mark_queued(
        self,
        action_id: str,
        *,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any] | None = None,
    ) -> bool: ...

    async def mark_executed(
        self,
        action_id: str,
        *,
        tenant_id: str,
        source_id: str,
        result: dict[str, Any],
    ) -> bool: ...

    async def mark_failed(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool: ...

    async def mark_retryable(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool: ...

    async def mark_uncertain(self, action_id: str, *, tenant_id: str, source_id: str, error: str) -> bool: ...
