"""Worker for outbound channel messages."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path
from typing import Any

from agent_platform.outbound.contracts import OutboundMessage, OutboundQueue
from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.sqlite import SQLiteOutboundQueue
from agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker
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
    try:
        await run_outbound_queue_worker(
            SQLiteOutboundQueue(conn),
            stop_event,
            registry=registry,
            channel=channel,
        )
    finally:
        conn.close()


async def run_outbound_queue_worker(
    queue: OutboundQueue,
    stop_event: asyncio.Event,
    *,
    registry: OutboundRegistry,
    channel: str,
    batch_size: int = BATCH_SIZE,
    lease_seconds: int = LEASE_SECONDS,
    retry_backoff_s: int = RETRY_BACKOFF_S,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
) -> None:
    owner = lease_owner(channel)
    sender = registry.get(channel)
    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"outbound:{channel}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        ),
        claim=lambda: queue.claim_due(
            channel=channel,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
        ),
        process=lambda message: _send_message(
            queue,
            message,
            sender=sender,
            retry_backoff_s=retry_backoff_s,
        ),
    )


async def _send_message(
    queue: OutboundQueue,
    message: OutboundMessage,
    *,
    sender: Any,
    retry_backoff_s: int,
) -> None:
    try:
        result = await sender.send(message)
        await queue.mark_sent(message.id, result=result.metadata)
    except Exception as exc:
        log.exception("outbound send failed id=%s channel=%s", message.id, message.channel)
        await queue.mark_retry(
            message.id,
            run_after=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )
