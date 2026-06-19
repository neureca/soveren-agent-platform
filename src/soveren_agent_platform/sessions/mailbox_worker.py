"""Worker that drains queued prompts for idle execution sessions."""
from __future__ import annotations

import asyncio
import logging
import socket
import sqlite3
from pathlib import Path

from soveren_agent_platform.runtime.worker_loop import sleep_or_stop
from soveren_agent_platform.sessions.contracts import (
    MailboxItem,
    SessionEventStore,
    SessionMailboxStore,
    SessionSnapshotStore,
    SessionStore,
)
from soveren_agent_platform.sessions.registry import SessionBackendMapping, normalize_session_backends
from soveren_agent_platform.sessions.sqlite import (
    SQLiteSessionEventStore,
    SQLiteSessionMailboxStore,
    SQLiteSessionSnapshotStore,
    SQLiteSessionStore,
)
from soveren_agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)

IDLE_INITIAL_S = 1.0
IDLE_MAX_S = 10.0
BATCH_SIZE = 5
STALE_SENDING_S = 30 * 60


def lease_owner() -> str:
    return f"{socket.gethostname()}/session-mailbox"


async def run_session_mailbox_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
) -> None:
    conn = open_sqlite(db_path)
    try:
        await run_session_mailbox_store_worker(
            SQLiteSessionStore(conn),
            SQLiteSessionMailboxStore(conn),
            stop_event,
            tenant_id=tenant_id,
            session_backends=session_backends,
            stale_sending_s=stale_sending_s,
            event_store=SQLiteSessionEventStore(conn),
            snapshot_store=SQLiteSessionSnapshotStore(conn),
        )
    finally:
        conn.close()


async def run_session_mailbox_store_worker(
    session_store: SessionStore,
    mailbox_store: SessionMailboxStore,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> None:
    idle = idle_initial_s
    log.info(
        "session mailbox worker started owner=%s backends=%s",
        lease_owner(),
        ",".join(sorted(normalize_session_backends(session_backends))) or "off",
    )
    try:
        while not stop_event.is_set():
            try:
                processed = await drain_store_once(
                    session_store,
                    mailbox_store,
                    tenant_id=tenant_id,
                    session_backends=session_backends,
                    stale_sending_s=stale_sending_s,
                    event_store=event_store,
                    snapshot_store=snapshot_store,
                )
            except Exception:
                log.exception("session mailbox drain failed")
                processed = 0
            if processed:
                idle = idle_initial_s
                continue
            await sleep_or_stop(stop_event, idle)
            idle = min(idle * 2, idle_max_s)
    finally:
        log.info("session mailbox worker stopped")


async def drain_once(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> int:
    return await drain_store_once(
        SQLiteSessionStore(conn),
        SQLiteSessionMailboxStore(conn),
        tenant_id=tenant_id,
        session_backends=session_backends,
        stale_sending_s=stale_sending_s,
        event_store=event_store,
        snapshot_store=snapshot_store,
    )


async def drain_store_once(
    session_store: SessionStore,
    mailbox_store: SessionMailboxStore,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> int:
    processed = 0
    processed += await _fail_stale_sending(
        mailbox_store,
        tenant_id=tenant_id,
        stale_sending_s=stale_sending_s,
    )
    session_ids = await mailbox_store.ready_session_ids(tenant_id=tenant_id, limit=BATCH_SIZE)
    for session_id in session_ids:
        item = await mailbox_store.claim_next(session_id)
        if item is None:
            continue
        processed += 1
        await _send_item(
            session_store,
            mailbox_store,
            item,
            session_backends=session_backends,
            event_store=event_store,
            snapshot_store=snapshot_store,
        )
    return processed


async def _fail_stale_sending(
    mailbox_store: SessionMailboxStore,
    *,
    tenant_id: str,
    stale_sending_s: int,
) -> int:
    rows = await mailbox_store.fail_stale_sending(
        tenant_id=tenant_id,
        older_than_s=stale_sending_s,
        reason="session mailbox item was left in sending after worker interruption",
        limit=BATCH_SIZE,
    )
    return len(rows)


async def _send_item(
    session_store: SessionStore,
    mailbox_store: SessionMailboxStore,
    item: MailboxItem,
    *,
    session_backends: SessionBackendMapping,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> None:
    session = await session_store.get(item.session_id)
    if session is None:
        await mailbox_store.mark_failed(item.id, last_error="runtime session not found")
        return

    backend = normalize_session_backends(session_backends).get(session.backend)
    if backend is None:
        await mailbox_store.mark_failed(
            item.id,
            last_error=f"no backend registered for {session.backend!r}",
        )
        return

    await session_store.set_status(
        session.id,
        "busy",
        current_action_id=item.action_id,
    )
    try:
        await backend.send(session.backend_session_id, item.prompt)
        if event_store is not None:
            await event_store.record(
                session_id=session.id,
                direction="input",
                payload_text=item.prompt,
                action_id=item.action_id,
                marker=f"mailbox:{item.id}:input",
            )
        capture = await backend.capture(session.backend_session_id)
    except RuntimeError as exc:
        await mailbox_store.requeue(item.id, last_error=str(exc))
        await session_store.set_status(session.id, "idle", last_error=str(exc))
        return
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.exception("session mailbox item failed id=%s", item.id)
        await mailbox_store.mark_failed(item.id, last_error=err)
        await session_store.set_status(session.id, "failed", last_error=err)
        return

    await mailbox_store.mark_sent(
        item.id,
        result={"output": capture.text, "timed_out": capture.timed_out},
    )
    if event_store is not None and capture.text.strip():
        await event_store.record(
            session_id=session.id,
            direction="output",
            payload_text=capture.text,
            action_id=item.action_id,
            marker=f"mailbox:{item.id}:output",
        )
    if snapshot_store is not None:
        await snapshot_store.refresh(session.id)
    next_status = "busy" if capture.timed_out else "idle"
    await session_store.set_status(
        session.id,
        next_status,
        current_action_id=item.action_id if capture.timed_out else None,
    )
