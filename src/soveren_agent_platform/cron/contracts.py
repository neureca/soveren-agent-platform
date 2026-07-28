"""Contracts for platform cron jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

type ScheduledJobStatus = Literal[
    "pending",
    "leased",
]
type ScheduledJobCancellationOutcome = Literal[
    "cancelled",
    "current_run_may_complete",
    "already_cancelled",
    "already_finished",
    "not_found",
]


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


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    id: str
    name: str
    status: ScheduledJobStatus
    run_at: int
    rrule: str | None
    timezone: str


@dataclass(frozen=True, slots=True)
class ScheduledJobCancellation:
    job_id: str
    outcome: ScheduledJobCancellationOutcome


class ScheduledJobStore(Protocol):
    async def list_jobs(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 20,
    ) -> list[ScheduledJob]: ...

    async def cancel_job(
        self,
        job_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> ScheduledJobCancellation: ...


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

    async def dispatch_due_event(
        self,
        job_id: str,
        *,
        lease_token: str,
        recipient: str,
    ) -> bool: ...
