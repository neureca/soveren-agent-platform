"""Worker for outbound channel messages."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.store import claim_due, mark_retry, mark_sent, row_to_message
from agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)

LEASE_SECONDS = 60
BATCH_SIZE = 5
IDLE_INITIAL_S = 1.0
IDLE_MAX_S = 10.0
RETRY_BACKOFF_S = 30


def lease_owner(channel: str) -> str:
    return f"{socket.gethostname()}/outbound/{channel}"


async def run_outbound_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    registry: OutboundRegistry,
    channel: str,
) -> None:
    conn = open_sqlite(db_path)
    owner = lease_owner(channel)
    idle = IDLE_INITIAL_S
    log.info("outbound worker started channel=%s owner=%s", channel, owner)
    try:
        while not stop_event.is_set():
            try:
                rows = await asyncio.to_thread(
                    claim_due,
                    conn,
                    channel=channel,
                    limit=BATCH_SIZE,
                    lease_owner=owner,
                    lease_seconds=LEASE_SECONDS,
                )
            except Exception:
                log.exception("outbound claim_due failed channel=%s", channel)
                rows = []
            if not rows:
                await _sleep_or_stop(stop_event, idle)
                idle = min(idle * 2, IDLE_MAX_S)
                continue
            idle = IDLE_INITIAL_S
            sender = registry.get(channel)
            for row in rows:
                try:
                    result = await sender.send(row_to_message(row))
                    await asyncio.to_thread(mark_sent, conn, row["id"], result=result.metadata)
                except Exception as exc:
                    log.exception("outbound send failed id=%s channel=%s", row["id"], channel)
                    await asyncio.to_thread(
                        mark_retry,
                        conn,
                        row["id"],
                        run_after=int(time.time()) + RETRY_BACKOFF_S,
                        last_error=f"{type(exc).__name__}: {exc}",
                    )
    finally:
        conn.close()
        log.info("outbound worker stopped channel=%s owner=%s", channel, owner)


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

