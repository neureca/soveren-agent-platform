"""Inbound batching worker."""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path

from agent_platform.batching.contracts import BatchState, BatchStore, InboundMessage
from agent_platform.batching.rules import (
    DEFAULT_MAX_COUNT,
    DEFAULT_MAX_WINDOW_S,
    DEFAULT_QUIET_WINDOW_S,
    decide_batch,
)
from agent_platform.batching.sqlite import SQLiteBatchStore
from agent_platform.batching.store import batch_payload
from agent_platform.queue.contracts import DurableQueue, QueueEvent
from agent_platform.queue.sqlite import SQLiteEventQueue
from agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker
from agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)

LEASE_SECONDS = 30
BATCH_SIZE = 20
IDLE_INITIAL_S = 0.5
IDLE_MAX_S = 5.0
RETRY_BACKOFF_S = 10
FLUSH_PRIORITY = 200


def lease_owner() -> str:
    return f"{socket.gethostname()}/batching"


async def run_batching_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    recipient: str = "batching",
    output_recipient: str = "agent",
    output_message_type: str = "ChatBatchReady",
    quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
    max_window_s: int = DEFAULT_MAX_WINDOW_S,
    max_count: int = DEFAULT_MAX_COUNT,
) -> None:
    conn = open_sqlite(db_path)
    try:
        await run_batching_queue_worker(
            SQLiteEventQueue(conn),
            SQLiteBatchStore(conn),
            stop_event,
            recipient=recipient,
            output_recipient=output_recipient,
            output_message_type=output_message_type,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )
    finally:
        conn.close()


async def run_batching_queue_worker(
    queue: DurableQueue,
    batch_store: BatchStore,
    stop_event: asyncio.Event,
    *,
    recipient: str = "batching",
    output_recipient: str = "agent",
    output_message_type: str = "ChatBatchReady",
    quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
    max_window_s: int = DEFAULT_MAX_WINDOW_S,
    max_count: int = DEFAULT_MAX_COUNT,
    batch_size: int = BATCH_SIZE,
    lease_seconds: int = LEASE_SECONDS,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
) -> None:
    owner = lease_owner()
    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"batching:{recipient}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        ),
        claim=lambda: queue.claim_due(
            recipient=recipient,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
        ),
        process=lambda event: _process(
            queue,
            batch_store,
            event,
            output_recipient=output_recipient,
            output_message_type=output_message_type,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        ),
    )


async def _process(
    queue: DurableQueue,
    batch_store: BatchStore,
    event: QueueEvent,
    *,
    output_recipient: str,
    output_message_type: str,
    quiet_window_s: int,
    max_window_s: int,
    max_count: int,
) -> None:
    try:
        if event.message_type == "InboundMessageReceived":
            batch_id = await batch_store.append_inbound_message(_message_from_event(event))
            if batch_id is not None:
                await _evaluate_and_maybe_flush(
                    queue,
                    batch_store,
                    batch_id,
                    causation_id=event.id,
                    output_recipient=output_recipient,
                    output_message_type=output_message_type,
                    quiet_window_s=quiet_window_s,
                    max_window_s=max_window_s,
                    max_count=max_count,
                )
        elif event.message_type == "FlushInboundBatch":
            await _evaluate_and_maybe_flush(
                queue,
                batch_store,
                str(event.payload["batch_id"]),
                causation_id=event.id,
                output_recipient=output_recipient,
                output_message_type=output_message_type,
                quiet_window_s=quiet_window_s,
                max_window_s=max_window_s,
                max_count=max_count,
            )
        else:
            log.warning("batching got unknown message_type=%s id=%s", event.message_type, event.id)
        await queue.mark_done(event.id)
    except Exception as exc:
        log.exception("batching failed id=%s message_type=%s", event.id, event.message_type)
        await queue.mark_retry(
            event.id,
            run_after=int(time.time()) + RETRY_BACKOFF_S,
            last_error=f"{type(exc).__name__}: {exc}",
        )


async def _evaluate_and_maybe_flush(
    queue: DurableQueue,
    batch_store: BatchStore,
    batch_id: str,
    *,
    causation_id: str,
    output_recipient: str,
    output_message_type: str,
    quiet_window_s: int,
    max_window_s: int,
    max_count: int,
) -> None:
    state = await batch_store.load_state(
        batch_id,
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
        max_count=max_count,
    )
    decision = decide_batch(state)
    await batch_store.store_decision(batch_id, decision, state=state)
    if state is None:
        return
    if decision.action == "flush":
        await _flush_batch(
            queue,
            batch_store,
            state,
            causation_id=causation_id,
            output_recipient=output_recipient,
            output_message_type=output_message_type,
        )
        return
    await _schedule_flush(
        queue,
        state,
        causation_id=causation_id,
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
    )


async def _flush_batch(
    queue: DurableQueue,
    batch_store: BatchStore,
    state: BatchState,
    *,
    causation_id: str,
    output_recipient: str,
    output_message_type: str,
) -> None:
    if await batch_store.mark_routed(state.batch_id):
        await queue.enqueue(
            tenant_id=state.tenant_id,
            recipient=output_recipient,
            message_type=output_message_type,
            payload=batch_payload(state),
            idempotency_key=f"inbound-batch:{state.batch_id}",
            correlation_id=state.batch_id,
            causation_id=causation_id,
        )


async def _schedule_flush(
    queue: DurableQueue,
    state: BatchState,
    *,
    causation_id: str,
    quiet_window_s: int,
    max_window_s: int,
) -> None:
    run_after = _next_flush_deadline(
        first_message_at=state.first_message_at,
        last_message_at=state.last_message_at,
        now=int(time.time()),
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
    )
    await queue.enqueue(
        tenant_id=state.tenant_id,
        recipient="batching",
        message_type="FlushInboundBatch",
        payload={"batch_id": state.batch_id},
        idempotency_key=f"inbound-batch-flush:{state.batch_id}:{run_after}",
        priority=FLUSH_PRIORITY,
        run_after=run_after,
        causation_id=causation_id,
    )


def _next_flush_deadline(
    *,
    first_message_at: int,
    last_message_at: int,
    now: int,
    quiet_window_s: int,
    max_window_s: int,
) -> int:
    quiet_deadline = last_message_at + quiet_window_s
    max_deadline = first_message_at + max_window_s
    if now < quiet_deadline:
        return min(quiet_deadline, max_deadline)
    if now < max_deadline:
        return max_deadline
    return now


def _message_from_event(event: QueueEvent) -> InboundMessage:
    payload = event.payload
    return InboundMessage(
        tenant_id=event.tenant_id,
        channel=str(payload["channel"]),
        source_id=str(payload["source_id"]),
        raw_event_id=str(payload.get("raw_event_id") or event.id),
        source_event_id=str(payload.get("source_event_id") or event.id),
        text=payload.get("text"),
        payload=payload,
        message_at=int(payload.get("message_at") or time.time()),
    )
