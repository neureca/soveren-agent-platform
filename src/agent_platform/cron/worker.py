"""Cron worker loop."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from agent_platform.cron.contracts import CronHandler, CronStore
from agent_platform.cron.sqlite import SQLiteCronStore
from agent_platform.runtime.worker_loop import sleep_or_stop
from agent_platform.storage.sqlite import open_sqlite

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
    conn = open_sqlite(db_path)
    try:
        await run_cron_store_worker(
            SQLiteCronStore(conn),
            stop_event,
            handler=handler,
            poll_interval_s=poll_interval_s,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            retry_backoff_s=retry_backoff_s,
        )
    finally:
        conn.close()


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
    log.info("cron worker started owner=%s", owner)
    try:
        while not stop_event.is_set():
            jobs = await store.claim_due(
                limit=batch_size,
                lease_owner=owner,
                lease_seconds=lease_seconds,
            )
            for job in jobs:
                try:
                    await handler.handle(job)
                    await store.complete(job.id)
                except Exception as exc:
                    log.exception("cron handler failed id=%s name=%s", job.id, job.name)
                    await store.fail(
                        job.id,
                        retry_at=int(time.time()) + retry_backoff_s,
                        last_error=f"{type(exc).__name__}: {exc}",
                    )
            await sleep_or_stop(stop_event, poll_interval_s)
    finally:
        log.info("cron worker stopped owner=%s", owner)
