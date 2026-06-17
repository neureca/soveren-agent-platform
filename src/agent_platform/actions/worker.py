"""Worker for generic platform actions."""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import sqlite3
import time
from pathlib import Path

from agent_platform.actions.registry import ActionRegistry
from agent_platform.actions.store import (
    get_action,
    mark_executed,
    mark_executing,
    mark_failed,
    mark_queued,
)
from agent_platform.queue.durable import claim_due, mark_done, mark_retry
from agent_platform.storage.sqlite import open_sqlite

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
    owner = lease_owner()
    idle = IDLE_INITIAL_S
    log.info("actions worker started owner=%s recipient=%s", owner, recipient)
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
                log.exception("actions claim_due failed")
                rows = []
            if not rows:
                await _sleep_or_stop(stop_event, idle)
                idle = min(idle * 2, IDLE_MAX_S)
                continue
            idle = IDLE_INITIAL_S
            for row in rows:
                await process_action_event(conn, row, registry=registry)
    finally:
        conn.close()
        log.info("actions worker stopped owner=%s recipient=%s", owner, recipient)


async def process_action_event(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    registry: ActionRegistry,
) -> None:
    event_id = row["id"]
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        action_id = str(payload["action_id"])
    except (json.JSONDecodeError, KeyError) as exc:
        await asyncio.to_thread(
            mark_retry,
            conn,
            event_id,
            run_after=int(time.time()) + RETRY_BACKOFF_S,
            last_error=f"bad action event payload: {exc}",
        )
        return

    action = await asyncio.to_thread(get_action, conn, action_id)
    if action is None:
        log.warning("action %s not found, dropping event %s", action_id, event_id)
        await asyncio.to_thread(mark_done, conn, event_id)
        return
    if action["status"] in ("executed", "failed", "denied", "cancelled"):
        await asyncio.to_thread(mark_done, conn, event_id)
        return
    if action["status"] not in ("approved", "queued"):
        log.info("action %s status=%s is not executable yet", action_id, action["status"])
        await asyncio.to_thread(mark_done, conn, event_id)
        return
    if not await asyncio.to_thread(mark_executing, conn, action_id):
        await asyncio.to_thread(mark_done, conn, event_id)
        return

    try:
        executor = registry.get(action["kind"])
        refreshed = await asyncio.to_thread(get_action, conn, action_id)
        if refreshed is None:
            raise RuntimeError(f"action disappeared during execution: {action_id}")
        result = await executor.execute(conn, refreshed)
        if result.status == "queued":
            await asyncio.to_thread(mark_queued, conn, action_id, result=result.result)
        elif result.status == "executed":
            await asyncio.to_thread(mark_executed, conn, action_id, result=result.result)
        else:
            raise RuntimeError(f"unsupported action result status: {result.status!r}")
        await asyncio.to_thread(mark_done, conn, event_id)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.exception("action execution failed id=%s", action_id)
        await asyncio.to_thread(mark_failed, conn, action_id, error=err)
        await asyncio.to_thread(mark_done, conn, event_id)


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

