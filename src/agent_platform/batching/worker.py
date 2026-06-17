"""Inbound batching worker."""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import sqlite3
import time
from pathlib import Path

from agent_platform.batching.contracts import InboundMessage
from agent_platform.batching.rules import (
    DEFAULT_MAX_COUNT,
    DEFAULT_MAX_WINDOW_S,
    DEFAULT_QUIET_WINDOW_S,
    decide_batch,
)
from agent_platform.batching.store import (
    append_inbound_message,
    batch_payload,
    load_state,
    mark_routed,
    store_decision,
)
from agent_platform.queue.durable import claim_due, enqueue, mark_done, mark_retry
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
    owner = lease_owner()
    idle = IDLE_INITIAL_S
    log.info("batching worker started owner=%s recipient=%s", owner, recipient)
    try:
        while not stop_event.is_set():
            try:
                rows = await asyncio.to_thread(
                    claim_due,
                    conn,
                    recipient=recipient,
                    limit=BATCH_SIZE,
                    lease_owner=owner,
                    lease_seconds=LEASE_SECONDS,
                )
            except Exception:
                log.exception("batching claim_due failed")
                rows = []
            if not rows:
                await _sleep_or_stop(stop_event, idle)
                idle = min(idle * 2, IDLE_MAX_S)
                continue
            idle = IDLE_INITIAL_S
            for row in rows:
                await _process(
                    conn,
                    row,
                    output_recipient=output_recipient,
                    output_message_type=output_message_type,
                    quiet_window_s=quiet_window_s,
                    max_window_s=max_window_s,
                    max_count=max_count,
                )
    finally:
        conn.close()
        log.info("batching worker stopped owner=%s recipient=%s", owner, recipient)


async def _process(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    output_recipient: str,
    output_message_type: str,
    quiet_window_s: int,
    max_window_s: int,
    max_count: int,
) -> None:
    event_id = row["id"]
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        if row["message_type"] == "InboundMessageReceived":
            batch_id = append_inbound_message(conn, _message_from_payload(row, payload))
            if batch_id is not None:
                _evaluate_and_maybe_flush(
                    conn,
                    batch_id,
                    causation_id=event_id,
                    output_recipient=output_recipient,
                    output_message_type=output_message_type,
                    quiet_window_s=quiet_window_s,
                    max_window_s=max_window_s,
                    max_count=max_count,
                )
        elif row["message_type"] == "FlushInboundBatch":
            _evaluate_and_maybe_flush(
                conn,
                str(payload["batch_id"]),
                causation_id=event_id,
                output_recipient=output_recipient,
                output_message_type=output_message_type,
                quiet_window_s=quiet_window_s,
                max_window_s=max_window_s,
                max_count=max_count,
            )
        else:
            log.warning("batching got unknown message_type=%s id=%s", row["message_type"], event_id)
        await asyncio.to_thread(mark_done, conn, event_id)
    except Exception as exc:
        log.exception("batching failed id=%s message_type=%s", event_id, row["message_type"])
        await asyncio.to_thread(
            mark_retry,
            conn,
            event_id,
            run_after=int(time.time()) + RETRY_BACKOFF_S,
            last_error=f"{type(exc).__name__}: {exc}",
        )


def _evaluate_and_maybe_flush(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    causation_id: str,
    output_recipient: str,
    output_message_type: str,
    quiet_window_s: int,
    max_window_s: int,
    max_count: int,
) -> None:
    state = load_state(
        conn,
        batch_id,
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
        max_count=max_count,
    )
    decision = decide_batch(state)
    store_decision(conn, batch_id, decision, state=state)
    if state is None:
        return
    if decision.action == "flush":
        _flush_batch(
            conn,
            state.batch_id,
            causation_id=causation_id,
            output_recipient=output_recipient,
            output_message_type=output_message_type,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )
        return
    _schedule_flush(
        conn,
        state,
        causation_id=causation_id,
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
    )


def _flush_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    causation_id: str,
    output_recipient: str,
    output_message_type: str,
    quiet_window_s: int,
    max_window_s: int,
    max_count: int,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        state = load_state(
            conn,
            batch_id,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )
        decision = decide_batch(state)
        store_decision(conn, batch_id, decision, state=state)
        if state is None:
            conn.execute("COMMIT")
            return
        if decision.action != "flush":
            conn.execute("COMMIT")
            _schedule_flush(
                conn,
                state,
                causation_id=causation_id,
                quiet_window_s=quiet_window_s,
                max_window_s=max_window_s,
            )
            return
        if mark_routed(conn, batch_id):
            enqueue(
                conn,
                tenant_id=state.tenant_id,
                recipient=output_recipient,
                message_type=output_message_type,
                payload=batch_payload(state),
                idempotency_key=f"inbound-batch:{batch_id}",
                correlation_id=batch_id,
                causation_id=causation_id,
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _schedule_flush(
    conn: sqlite3.Connection,
    state: object,
    *,
    causation_id: str,
    quiet_window_s: int,
    max_window_s: int,
) -> None:
    first_message_at = int(state.first_message_at)  # type: ignore[attr-defined]
    last_message_at = int(state.last_message_at)  # type: ignore[attr-defined]
    run_after = _next_flush_deadline(
        first_message_at=first_message_at,
        last_message_at=last_message_at,
        now=int(time.time()),
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
    )
    enqueue(
        conn,
        tenant_id=str(state.tenant_id),  # type: ignore[attr-defined]
        recipient="batching",
        message_type="FlushInboundBatch",
        payload={"batch_id": state.batch_id},  # type: ignore[attr-defined]
        idempotency_key=f"inbound-batch-flush:{state.batch_id}:{run_after}",  # type: ignore[attr-defined]
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


def _message_from_payload(row: sqlite3.Row, payload: dict) -> InboundMessage:
    return InboundMessage(
        tenant_id=row["tenant_id"],
        channel=str(payload["channel"]),
        source_id=str(payload["source_id"]),
        raw_event_id=str(payload.get("raw_event_id") or row["id"]),
        source_event_id=str(payload.get("source_event_id") or row["id"]),
        text=payload.get("text"),
        payload=payload,
        message_at=int(payload.get("message_at") or time.time()),
    )


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

