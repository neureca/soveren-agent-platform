"""Queue-to-agent worker loop."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path

from soveren_agent_platform.agent.contracts import AgentEvent, AgentEventObserver, AgentEventOutcome, AgentHandler
from soveren_agent_platform.queue.contracts import DurableQueue, QueueEvent
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.runtime.failures import event_error_detail, is_non_retryable_event_error
from soveren_agent_platform.runtime.latency import SQLiteTurnLatencyLogger
from soveren_agent_platform.runtime.worker_loop import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    PollingWorkerConfig,
    run_polling_worker,
)

log = logging.getLogger(__name__)


def lease_owner(recipient: str) -> str:
    return f"{socket.gethostname()}/{recipient}"


async def run_agent_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    handler: AgentHandler,
    latency_logger: SQLiteTurnLatencyLogger | None = None,
    tenant_id: str | None = None,
    recipient: str = "agent",
    batch_size: int = 5,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
    idle_initial_s: float = 1.0,
    idle_max_s: float = 10.0,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
) -> None:
    """Continuously claim SQLite queue rows for `recipient` and pass them to `handler`."""
    async with await SQLiteEventQueue.open(db_path) as queue:
        await run_agent_queue_worker(
            queue,
            stop_event,
            handler=handler,
            latency_logger=latency_logger,
            tenant_id=tenant_id,
            recipient=recipient,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            retry_backoff_s=retry_backoff_s,
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
            max_consecutive_failures=max_consecutive_failures,
        )


async def run_agent_queue_worker(
    queue: DurableQueue,
    stop_event: asyncio.Event,
    *,
    handler: AgentHandler,
    latency_logger: SQLiteTurnLatencyLogger | None = None,
    tenant_id: str | None = None,
    recipient: str = "agent",
    batch_size: int = 5,
    lease_seconds: int = 60,
    retry_backoff_s: int = 30,
    idle_initial_s: float = 1.0,
    idle_max_s: float = 10.0,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
) -> None:
    """Run the agent worker against any queue adapter implementing `DurableQueue`."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    if tenant_id is not None and not tenant_id.strip():
        raise ValueError("tenant_id must be non-empty when provided")
    owner = lease_owner(recipient)

    async def claim() -> list[QueueEvent]:
        if tenant_id is None:
            return await queue.claim_due(
                recipient=recipient,
                limit=batch_size,
                lease_owner=owner,
                lease_seconds=lease_seconds,
            )
        return await queue.claim_due(
            recipient=recipient,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
            tenant_id=tenant_id,
        )

    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"agent:{recipient}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
            max_consecutive_failures=max_consecutive_failures,
        ),
        claim=claim,
        process=lambda event: _process_event(
            queue,
            event,
            handler=handler,
            latency_logger=latency_logger,
            retry_backoff_s=retry_backoff_s,
        ),
        renew_lease=lambda event: queue.renew_lease(
            event.id,
            lease_token=event.lease_token,
            lease_seconds=lease_seconds,
        ),
        lease_renew_interval_s=max(0.1, lease_seconds / 3),
    )


async def _process_event(
    queue: DurableQueue,
    event: QueueEvent,
    *,
    handler: AgentHandler,
    latency_logger: SQLiteTurnLatencyLogger | None = None,
    retry_backoff_s: int,
) -> None:
    observer: AbstractContextManager[None]
    source_id = event.payload.get("source_id")
    if latency_logger is None or not isinstance(source_id, str):
        observer = nullcontext()
    else:
        batch_id = event.payload.get("batch_id")
        observer = latency_logger.agent(
            event.tenant_id, source_id, event.id, batch_id if isinstance(batch_id, str) else None,
        )
    with observer:
        transitioned = await _handle_event(queue, event, handler=handler, retry_backoff_s=retry_backoff_s)
    if transitioned and latency_logger is not None and isinstance(source_id, str):
        await latency_logger.finish(event.tenant_id, source_id, event.id)


async def _handle_event(
    queue: DurableQueue, event: QueueEvent, *, handler: AgentHandler, retry_backoff_s: int,
) -> bool:
    agent_event = _agent_event(event)
    try:
        await handler.handle(agent_event)
        changed = await queue.mark_done(event.id, lease_token=event.lease_token)
        if changed:
            await _notify_outcome(handler, agent_event, "done")
        return changed
    except Exception as exc:
        log.error("agent handler failed id=%s message_type=%s error_type=%s",
                  event.id, event.message_type, type(exc).__name__)
        if is_non_retryable_event_error(exc):
            changed = await queue.mark_dead_letter(
                event.id, lease_token=event.lease_token,
                last_error=event_error_detail(exc),
            )
            if changed:
                await _notify_outcome(handler, agent_event, "dead_letter")
            return changed
        status = await queue.mark_retry(
            event.id,
            lease_token=event.lease_token,
            run_after=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )
        if status is not None:
            await _notify_outcome(handler, agent_event, "dead_letter" if status == "dead_letter" else "retrying")
        return status is not None


async def _notify_outcome(handler: AgentHandler, event: AgentEvent, outcome: AgentEventOutcome) -> None:
    if isinstance(handler, AgentEventObserver):
        try:
            async with asyncio.timeout(10):
                await handler.on_event_outcome(event, outcome)
        except Exception:
            # Presentation failures cannot replay an already committed queue effect.
            log.warning("agent outcome observer failed id=%s outcome=%s", event.id, outcome)


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
