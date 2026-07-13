"""Cron worker loop."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from soveren_agent_platform.cron.contracts import CronHandler, CronJob, CronNotStartedError, CronStore
from soveren_agent_platform.cron.sqlite import SQLiteCronStore
from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker

log = logging.getLogger(__name__)


def lease_owner() -> str:
    return f"{socket.gethostname()}/cron"


async def run_cron_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    handler: CronHandler,
    poll_interval_s: float = 30.0,
    batch_size: int = 20,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
) -> None:
    """Poll due SQLite cron jobs and delegate each due job to `handler`."""
    async with await SQLiteCronStore.open(db_path) as store:
        await run_cron_store_worker(
            store,
            stop_event,
            handler=handler,
            poll_interval_s=poll_interval_s,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            retry_backoff_s=retry_backoff_s,
        )


async def run_cron_store_worker(
    store: CronStore,
    stop_event: asyncio.Event,
    *,
    handler: CronHandler,
    poll_interval_s: float = 30.0,
    batch_size: int = 20,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
) -> None:
    owner = lease_owner()
    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name="cron",
            idle_initial_s=poll_interval_s,
            idle_max_s=poll_interval_s,
        ),
        claim=lambda: store.claim_due(
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
        ),
        process=lambda job: _execute_job(
            store,
            job,
            handler=handler,
            retry_backoff_s=retry_backoff_s,
        ),
        renew_lease=lambda job: store.renew_lease(
            job.id,
            lease_token=job.lease_token,
            lease_seconds=lease_seconds,
        ),
        lease_renew_interval_s=max(0.1, lease_seconds / 3),
    )


async def _execute_job(
    store: CronStore,
    job: CronJob,
    *,
    handler: CronHandler,
    retry_backoff_s: int,
) -> None:
    if not await store.start_execution(job.id, lease_token=job.lease_token):
        log.error("cron lease lost before execution id=%s name=%s", job.id, job.name)
        return
    try:
        await handler.handle(job)
        if not await store.complete(job.id, lease_token=job.lease_token):
            log.error("cron execution completed without owned lease id=%s name=%s", job.id, job.name)
    except CronNotStartedError as exc:
        log.warning("cron execution did not start id=%s name=%s", job.id, job.name)
        await store.fail(
            job.id,
            lease_token=job.lease_token,
            retry_at=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )
    except Exception as exc:
        log.exception("cron execution outcome is uncertain id=%s name=%s", job.id, job.name)
        await store.mark_uncertain(
            job.id,
            lease_token=job.lease_token,
            last_error=f"{type(exc).__name__}: {exc}",
        )
