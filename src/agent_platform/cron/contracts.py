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

