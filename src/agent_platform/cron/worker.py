"""Cron worker loop."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from agent_platform.cron.contracts import CronHandler
from agent_platform.cron.store import claim_due_jobs, complete_job, fail_job
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
    """Poll due cron jobs and delegate each due job to `handler`."""
    conn = open_sqlite(db_path)
    owner = lease_owner()
    log.info("cron worker started owner=%s", owner)
    try:
        while not stop_event.is_set():
            jobs = await asyncio.to_thread(
                claim_due_jobs,
                conn,
                limit=batch_size,
                lease_owner=owner,
                lease_seconds=lease_seconds,
            )
            for job in jobs:
                try:
                    await handler.handle(job)
                    await asyncio.to_thread(complete_job, conn, job.id)
                except Exception as exc:
                    log.exception("cron handler failed id=%s name=%s", job.id, job.name)
                    await asyncio.to_thread(
                        fail_job,
                        conn,
                        job.id,
                        retry_at=int(time.time()) + retry_backoff_s,
                        last_error=f"{type(exc).__name__}: {exc}",
                    )
            await _sleep_or_stop(stop_event, poll_interval_s)
    finally:
        conn.close()
        log.info("cron worker stopped owner=%s", owner)


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

