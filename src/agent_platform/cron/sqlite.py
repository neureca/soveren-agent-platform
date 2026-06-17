"""SQLite adapter for cron storage."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from agent_platform.cron import store
from agent_platform.cron.contracts import CronJob


class SQLiteCronStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

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
        return await asyncio.to_thread(
            store.insert_job,
            self.conn,
            tenant_id=tenant_id,
            name=name,
            payload=payload,
            run_at=run_at,
            rrule=rrule,
            timezone=timezone,
            max_attempts=max_attempts,
        )

    async def claim_due(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> list[CronJob]:
        return await asyncio.to_thread(
            store.claim_due_jobs,
            self.conn,
            limit=limit,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )

    async def complete(self, job_id: str) -> None:
        await asyncio.to_thread(store.complete_job, self.conn, job_id)

    async def fail(self, job_id: str, *, retry_at: int, last_error: str) -> None:
        await asyncio.to_thread(
            store.fail_job,
            self.conn,
            job_id,
            retry_at=retry_at,
            last_error=last_error,
        )
