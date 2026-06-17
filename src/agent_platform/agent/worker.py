"""Queue-to-agent worker loop."""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import sqlite3
import time
from pathlib import Path

from agent_platform.agent.contracts import AgentEvent, AgentHandler
from agent_platform.queue.durable import claim_due, mark_done, mark_retry
from agent_platform.storage.sqlite import open_sqlite

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
    """Continuously claim queue rows for `recipient` and pass them to `handler`."""
    conn = open_sqlite(db_path)
    owner = lease_owner(recipient)
    idle = idle_initial_s
    log.info("agent worker started recipient=%s owner=%s", recipient, owner)
    try:
        while not stop_event.is_set():
            rows = await _claim_or_sleep(
                conn,
                stop_event,
                recipient=recipient,
                batch_size=batch_size,
                lease_owner_value=owner,
                lease_seconds=lease_seconds,
                idle=idle,
            )
            if not rows:
                idle = min(idle * 2, idle_max_s)
                continue
            idle = idle_initial_s
            for row in rows:
                await _process_row(
                    conn,
                    row,
                    handler=handler,
                    retry_backoff_s=retry_backoff_s,
                )
    finally:
        conn.close()
        log.info("agent worker stopped recipient=%s owner=%s", recipient, owner)


async def _claim_or_sleep(
    conn: sqlite3.Connection,
    stop_event: asyncio.Event,
    *,
    recipient: str,
    batch_size: int,
    lease_owner_value: str,
    lease_seconds: int,
    idle: float,
) -> list[sqlite3.Row]:
    try:
        rows = await asyncio.to_thread(
            claim_due,
            conn,
            recipient=recipient,
            limit=batch_size,
            lease_owner=lease_owner_value,
            lease_seconds=lease_seconds,
        )
    except Exception:
        log.exception("agent worker claim_due failed recipient=%s", recipient)
        rows = []
    if not rows:
        await _sleep_or_stop(stop_event, idle)
    return list(rows)


async def _process_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    handler: AgentHandler,
    retry_backoff_s: int,
) -> None:
    event = _event_from_row(row)
    try:
        await handler.handle(event)
        await asyncio.to_thread(mark_done, conn, event.id)
    except Exception as exc:
        log.exception("agent handler failed id=%s message_type=%s", event.id, event.message_type)
        await asyncio.to_thread(
            mark_retry,
            conn,
            event.id,
            run_after=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )


def _event_from_row(row: sqlite3.Row) -> AgentEvent:
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
    except json.JSONDecodeError:
        payload = {"_raw": row["payload_json"]}
    return AgentEvent(
        id=row["id"],
        tenant_id=row["tenant_id"],
        recipient=row["recipient"],
        message_type=row["message_type"],
        payload=payload,
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
    )


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

