"""Contracts for platform cron jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class CronJob:
    id: str
    tenant_id: str
    source_id: str
    name: str
    payload: dict[str, Any]
    run_at: int
    rrule: str | None
    timezone: str
    attempts: int
    lease_token: str
    retry_at: int | None = None


class CronHandler(Protocol):
    async def handle(self, job: CronJob) -> None: ...


class CronNotStartedError(RuntimeError):
    """The handler can prove that no externally visible work was started."""


class CronStore(Protocol):
    async def insert(
        self,
        *,
        tenant_id: str,
        source_id: str,
        name: str,
        payload: dict[str, Any],
        run_at: int,
        rrule: str | None = None,
        timezone: str = "UTC",
        max_attempts: int = 5,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]: ...

    async def claim_due(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        tenant_id: str | None = None,
    ) -> list[CronJob]: ...

    async def renew_lease(
        self,
        job_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool: ...

    async def start_execution(self, job_id: str, *, lease_token: str) -> bool: ...

    async def complete(self, job_id: str, *, lease_token: str) -> bool: ...

    async def mark_uncertain(
        self,
        job_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool: ...

    async def fail(
        self,
        job_id: str,
        *,
        lease_token: str,
        retry_at: int,
        last_error: str,
    ) -> bool: ...
