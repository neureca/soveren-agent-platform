"""Cron handler that emits queue events."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.queue.durable import enqueue


@dataclass(slots=True)
class QueueCronHandler:
    conn: sqlite3.Connection
    recipient: str = "agent"
    message_type: str = "CronJobDue"

    async def handle(self, job: CronJob) -> None:
        payload: dict[str, Any] = {
            "cron_job_id": job.id,
            "name": job.name,
            "payload": job.payload,
            "run_at": job.run_at,
        }
        enqueue(
            self.conn,
            tenant_id=job.tenant_id,
            recipient=self.recipient,
            message_type=self.message_type,
            payload=payload,
            idempotency_key=f"cron:{job.id}:{job.run_at}",
            correlation_id=job.id,
        )

