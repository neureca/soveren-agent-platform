"""Worker that drains queued prompts for idle execution sessions."""
from __future__ import annotations

import asyncio
import logging
import socket
import sqlite3
from pathlib import Path

from agent_platform.sessions import mailbox
from agent_platform.sessions.registry import SessionBackendMapping, normalize_session_backends
from agent_platform.sessions.store import get_session, set_session_status
from agent_platform.storage.sqlite import open_sqlite

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
    idle = IDLE_INITIAL_S
    log.info(
        "session mailbox worker started owner=%s backends=%s",
        lease_owner(),
        ",".join(sorted(normalize_session_backends(session_backends))) or "off",
    )
    try:
        while not stop_event.is_set():
            try:
                processed = await drain_once(
                    conn,
                    tenant_id=tenant_id,
                    session_backends=session_backends,
                    stale_sending_s=stale_sending_s,
                )
            except Exception:
                log.exception("session mailbox drain failed")
                processed = 0
            if processed:
                idle = IDLE_INITIAL_S
                continue
            await _sleep_or_stop(stop_event, idle)
            idle = min(idle * 2, IDLE_MAX_S)
    finally:
        conn.close()
        log.info("session mailbox worker stopped")


async def drain_once(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
) -> int:
    processed = 0
    processed += await _fail_stale_sending(conn, tenant_id=tenant_id, stale_sending_s=stale_sending_s)
    session_ids = await asyncio.to_thread(
        mailbox.ready_session_ids,
        conn,
        tenant_id=tenant_id,
        limit=BATCH_SIZE,
    )
    for session_id in session_ids:
        item = await asyncio.to_thread(mailbox.claim_next, conn, session_id)
        if item is None:
            continue
        processed += 1
        await _send_item(conn, item, session_backends=session_backends)
    return processed


async def _fail_stale_sending(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    stale_sending_s: int,
) -> int:
    rows = await asyncio.to_thread(
        mailbox.fail_stale_sending,
        conn,
        tenant_id=tenant_id,
        older_than_s=stale_sending_s,
        reason="session mailbox item was left in sending after worker interruption",
        limit=BATCH_SIZE,
    )
    return len(rows)


async def _send_item(
    conn: sqlite3.Connection,
    item: sqlite3.Row,
    *,
    session_backends: SessionBackendMapping,
) -> None:
    session = await asyncio.to_thread(get_session, conn, item["session_id"])
    if session is None:
        await asyncio.to_thread(mailbox.mark_failed, conn, item["id"], last_error="runtime session not found")
        return

    backend = normalize_session_backends(session_backends).get(session["backend"])
    if backend is None:
        await asyncio.to_thread(
            mailbox.mark_failed,
            conn,
            item["id"],
            last_error=f"no backend registered for {session['backend']!r}",
        )
        return

    await asyncio.to_thread(
        set_session_status,
        conn,
        session["id"],
        "busy",
        current_action_id=item["action_id"],
    )
    try:
        await backend.send(session["backend_session_id"], item["prompt"])
        capture = await backend.capture(session["backend_session_id"])
    except RuntimeError as exc:
        # Backends use RuntimeError for recoverable busy/race cases in this
        # platform layer. The item remains queued for a later drain.
        await asyncio.to_thread(mailbox.requeue, conn, item["id"], last_error=str(exc))
        await asyncio.to_thread(set_session_status, conn, session["id"], "idle", last_error=str(exc))
        return
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.exception("session mailbox item failed id=%s", item["id"])
        await asyncio.to_thread(mailbox.mark_failed, conn, item["id"], last_error=err)
        await asyncio.to_thread(set_session_status, conn, session["id"], "failed", last_error=err)
        return

    await asyncio.to_thread(
        mailbox.mark_sent,
        conn,
        item["id"],
        result={"output": capture.text, "timed_out": capture.timed_out},
    )
    next_status = "busy" if capture.timed_out else "idle"
    await asyncio.to_thread(
        set_session_status,
        conn,
        session["id"],
        next_status,
        current_action_id=item["action_id"] if capture.timed_out else None,
    )


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
