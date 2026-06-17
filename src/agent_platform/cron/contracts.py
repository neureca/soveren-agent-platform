"""Contracts for platform cron jobs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class CronJob:
    id: str
    tenant_id: str
    name: str
    payload: dict[str, Any]
    run_at: int
    rrule: str | None
    timezone: str
    attempts: int


class CronHandler(Protocol):
    async def handle(self, job: CronJob) -> None:
        ...


class CronStore(Protocol):
    async def insert(
        self,
        *,
        tenant_id: str,
        name: str,
        payload: dict[str, Any],
        run_at: int,
        rrule: str | None = None,
        timezone: str = "UTC",
        max_attempts: int = 5,
    ) -> str:
        ...

    async def claim_due(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[CronJob]:
        ...

    async def complete(self, job_id: str) -> None:
        ...

    async def fail(self, job_id: str, *, retry_at: int, last_error: str) -> None:
        ...
