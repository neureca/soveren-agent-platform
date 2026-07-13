"""Cron handler that emits queue events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.queue.contracts import DurableQueue


@dataclass(slots=True)
class QueueCronHandler:
    queue: DurableQueue
    recipient: str = "agent"
    message_type: str = "CronJobDue"

    async def handle(self, job: CronJob) -> None:
        payload: dict[str, Any] = {
            "cron_job_id": job.id,
            "source_id": job.source_id,
            "name": job.name,
            "payload": job.payload,
            "run_at": job.run_at,
        }
        await self.queue.enqueue(
            tenant_id=job.tenant_id,
            recipient=self.recipient,
            message_type=self.message_type,
            payload=payload,
            idempotency_key=f"cron:{job.id}:{job.run_at}",
            correlation_id=job.id,
        )
