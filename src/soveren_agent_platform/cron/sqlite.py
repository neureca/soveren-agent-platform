"""SQLite adapter for cron storage."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.cron import store
from soveren_agent_platform.cron.contracts import (
    CronJob,
    ScheduledJob,
    ScheduledJobCancellation,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteCronStore(SQLiteAdapter):
    async def dispatch_due_event(
        self,
        job_id: str,
        *,
        lease_token: str,
        recipient: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.dispatch_due_event,
            job_id,
            lease_token=lease_token,
            recipient=recipient,
        )

    async def list_jobs(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 20,
    ) -> list[ScheduledJob]:
        return await run_sqlite(
            self._conn,
            store.list_scheduled_jobs,
            tenant_id=tenant_id,
            source_id=source_id,
            limit=limit,
        )

    async def cancel_job(
        self,
        job_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> ScheduledJobCancellation:
        return await run_sqlite(
            self._conn,
            store.cancel_scheduled_job,
            job_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )

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
    ) -> tuple[str, bool]:
        return await run_sqlite(
            self._conn,
            store.insert_job,
            tenant_id=tenant_id,
            source_id=source_id,
            name=name,
            payload=payload,
            run_at=run_at,
            rrule=rrule,
            timezone=timezone,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
        )

    async def claim_due(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        tenant_id: str | None = None,
    ) -> list[CronJob]:
        return await run_sqlite(
            self._conn,
            store.claim_due_jobs,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            tenant_id=tenant_id,
        )

    async def complete(self, job_id: str, *, lease_token: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.complete_job,
            job_id,
            lease_token=lease_token,
        )

    async def renew_lease(
        self,
        job_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.renew_lease,
            job_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
        )

    async def start_execution(self, job_id: str, *, lease_token: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.start_execution,
            job_id,
            lease_token=lease_token,
        )

    async def mark_uncertain(
        self,
        job_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.mark_uncertain,
            job_id,
            lease_token=lease_token,
            last_error=last_error,
        )

    async def fail(
        self,
        job_id: str,
        *,
        lease_token: str,
        retry_at: int,
        last_error: str,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            store.fail_job,
            job_id,
            lease_token=lease_token,
            retry_at=retry_at,
            last_error=last_error,
        )
