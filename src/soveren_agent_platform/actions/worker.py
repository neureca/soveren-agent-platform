"""Worker for generic platform actions."""
from __future__ import annotations

import asyncio
import logging
import socket
import sqlite3
import time
from pathlib import Path

from soveren_agent_platform.actions.contracts import ActionStore
from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.sqlite import SQLiteActionStore
from soveren_agent_platform.queue.contracts import DurableQueue, QueueEvent
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue, row_to_queue_event
from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker
from soveren_agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)

LEASE_SECONDS = 60
BATCH_SIZE = 5
IDLE_INITIAL_S = 1.0
IDLE_MAX_S = 10.0
RETRY_BACKOFF_S = 30


def lease_owner() -> str:
    return f"{socket.gethostname()}/actions"


async def run_actions_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    registry: ActionRegistry,
    recipient: str = "actions",
) -> None:
    conn = open_sqlite(db_path)
    try:
        await run_actions_queue_worker(
            SQLiteActionStore(conn),
            SQLiteEventQueue(conn),
            stop_event,
            registry=registry,
            recipient=recipient,
        )
    finally:
        conn.close()


async def run_actions_queue_worker(
    action_store: ActionStore,
    queue: DurableQueue,
    stop_event: asyncio.Event,
    *,
    registry: ActionRegistry,
    recipient: str = "actions",
    batch_size: int = BATCH_SIZE,
    lease_seconds: int = LEASE_SECONDS,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
) -> None:
    owner = lease_owner()
    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"actions:{recipient}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        ),
        claim=lambda: queue.claim_due(
            recipient=recipient,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
        ),
        process=lambda event: process_action_queue_event(
            action_store,
            event,
            registry=registry,
            queue=queue,
        ),
    )


async def process_action_event(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    registry: ActionRegistry,
) -> None:
    await process_action_queue_event(
        SQLiteActionStore(conn),
        row_to_queue_event(row),
        registry=registry,
        queue=SQLiteEventQueue(conn),
    )


async def process_action_queue_event(
    action_store: ActionStore,
    event: QueueEvent,
    *,
    registry: ActionRegistry,
    queue: DurableQueue,
) -> None:
    event_id = event.id
    try:
        action_id = str(event.payload["action_id"])
    except KeyError as exc:
        await queue.mark_retry(
            event_id,
            run_after=int(time.time()) + RETRY_BACKOFF_S,
            last_error=f"bad action event payload: {exc}",
        )
        return

    action = await action_store.get(action_id)
    if action is None:
        log.warning("action %s not found, dropping event %s", action_id, event_id)
        await queue.mark_done(event_id)
        return
    if action.status in ("executed", "failed", "denied", "cancelled"):
        await queue.mark_done(event_id)
        return
    if action.status not in ("approved", "queued"):
        log.info("action %s status=%s is not executable yet", action_id, action.status)
        await queue.mark_done(event_id)
        return
    if not await action_store.mark_executing(action_id):
        await queue.mark_done(event_id)
        return

    try:
        executor = registry.get(action.kind)
        refreshed = await action_store.get(action_id)
        if refreshed is None:
            raise RuntimeError(f"action disappeared during execution: {action_id}")
        result = await executor.execute(refreshed)
        if result.status == "queued":
            await action_store.mark_queued(action_id, result=result.result)
        elif result.status == "executed":
            await action_store.mark_executed(action_id, result=result.result)
        else:
            raise RuntimeError(f"unsupported action result status: {result.status!r}")
        await queue.mark_done(event_id)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.exception("action execution failed id=%s", action_id)
        await action_store.mark_failed(action_id, error=err)
        await queue.mark_done(event_id)
