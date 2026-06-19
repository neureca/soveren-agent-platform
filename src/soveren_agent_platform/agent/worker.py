"""Queue-to-agent worker loop."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from soveren_agent_platform.agent.contracts import AgentEvent, AgentHandler
from soveren_agent_platform.queue.contracts import DurableQueue, QueueEvent
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker
from soveren_agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)


def lease_owner(recipient: str) -> str:
    return f"{socket.gethostname()}/{recipient}"


async def run_agent_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    handler: AgentHandler,
    recipient: str = "agent",
    batch_size: int = 5,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
    idle_initial_s: float = 1.0,
    idle_max_s: float = 10.0,
) -> None:
    """Continuously claim SQLite queue rows for `recipient` and pass them to `handler`."""
    conn = open_sqlite(db_path)
    try:
        await run_agent_queue_worker(
            SQLiteEventQueue(conn),
            stop_event,
            handler=handler,
            recipient=recipient,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            retry_backoff_s=retry_backoff_s,
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        )
    finally:
        conn.close()


async def run_agent_queue_worker(
    queue: DurableQueue,
    stop_event: asyncio.Event,
    *,
    handler: AgentHandler,
    recipient: str = "agent",
    batch_size: int = 5,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
    idle_initial_s: float = 1.0,
    idle_max_s: float = 10.0,
) -> None:
    """Run the agent worker against any queue adapter implementing `DurableQueue`."""
    owner = lease_owner(recipient)
    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"agent:{recipient}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        ),
        claim=lambda: queue.claim_due(
            recipient=recipient,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
        ),
        process=lambda event: _process_event(
            queue,
            event,
            handler=handler,
            retry_backoff_s=retry_backoff_s,
        ),
    )


async def _process_event(
    queue: DurableQueue,
    event: QueueEvent,
    *,
    handler: AgentHandler,
    retry_backoff_s: int,
) -> None:
    agent_event = _agent_event(event)
    try:
        await handler.handle(agent_event)
        await queue.mark_done(event.id)
    except Exception as exc:
        log.exception("agent handler failed id=%s message_type=%s", event.id, event.message_type)
        await queue.mark_retry(
            event.id,
            run_after=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )


def _agent_event(event: QueueEvent) -> AgentEvent:
    return AgentEvent(
        id=event.id,
        tenant_id=event.tenant_id,
        recipient=event.recipient,
        message_type=event.message_type,
        payload=event.payload,
        correlation_id=event.correlation_id,
        causation_id=event.causation_id,
    )
