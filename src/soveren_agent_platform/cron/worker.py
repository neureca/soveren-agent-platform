"""Cron worker loop."""

from __future__ import annotations

import asyncio
import logging
import socket
from pathlib import Path

from soveren_agent_platform.cron.contracts import (
    CronJob,
    CronStore,
)
from soveren_agent_platform.cron.sqlite import SQLiteCronStore
from soveren_agent_platform.runtime.worker_loop import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    PollingWorkerConfig,
    run_polling_worker,
)

log = logging.getLogger(__name__)


def lease_owner() -> str:
    return f"{socket.gethostname()}/cron"


async def run_cron_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    tenant_id: str | None = None,
    poll_interval_s: float = 30.0,
    batch_size: int = 20,
    lease_seconds: int = 60,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    recipient: str = "agent",
) -> None:
    """Publish due cron jobs to the durable agent event queue."""
    async with await SQLiteCronStore.open(db_path) as store:
        await run_cron_store_worker(
            store,
            stop_event,
            tenant_id=tenant_id,
            poll_interval_s=poll_interval_s,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            max_consecutive_failures=max_consecutive_failures,
            recipient=recipient,
        )


async def run_cron_store_worker(
    store: CronStore,
    stop_event: asyncio.Event,
    *,
    tenant_id: str | None = None,
    poll_interval_s: float = 30.0,
    batch_size: int = 20,
    lease_seconds: int = 60,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    recipient: str = "agent",
) -> None:
    if not isinstance(recipient, str) or not recipient.strip():
        raise ValueError("recipient must be a non-empty string")
    await _run_cron_polling_worker(
        store,
        stop_event,
        recipient=recipient,
        tenant_id=tenant_id,
        poll_interval_s=poll_interval_s,
        batch_size=batch_size,
        lease_seconds=lease_seconds,
        max_consecutive_failures=max_consecutive_failures,
    )


async def _run_cron_polling_worker(
    store: CronStore,
    stop_event: asyncio.Event,
    *,
    recipient: str,
    tenant_id: str | None,
    poll_interval_s: float,
    batch_size: int,
    lease_seconds: int,
    max_consecutive_failures: int,
) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    if tenant_id is not None and not tenant_id.strip():
        raise ValueError("tenant_id must be non-empty when provided")
    owner = lease_owner()

    async def claim() -> list[CronJob]:
        if tenant_id is None:
            return await store.claim_due(
                limit=batch_size,
                lease_owner=owner,
                lease_seconds=lease_seconds,
            )
        return await store.claim_due(
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
            tenant_id=tenant_id,
        )

    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name="cron" if tenant_id is None else f"cron:{tenant_id}",
            idle_initial_s=poll_interval_s,
            idle_max_s=poll_interval_s,
            max_consecutive_failures=max_consecutive_failures,
        ),
        claim=claim,
        process=lambda job: _dispatch_due_event(store, job, recipient=recipient),
        renew_lease=lambda job: store.renew_lease(
            job.id,
            lease_token=job.lease_token,
            lease_seconds=lease_seconds,
        ),
        lease_renew_interval_s=max(0.1, lease_seconds / 3),
    )


async def _dispatch_due_event(
    store: CronStore,
    job: CronJob,
    *,
    recipient: str,
) -> None:
    if not await store.dispatch_due_event(
        job.id,
        lease_token=job.lease_token,
        recipient=recipient,
    ):
        log.error(
            "cron lease lost before event dispatch id=%s name=%s",
            job.id,
            job.name,
        )
