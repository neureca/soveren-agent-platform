"""Worker that drains queued prompts for idle execution sessions."""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import sqlite3
import time
from pathlib import Path

from soveren_agent_platform.runtime.worker_loop import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ConsecutiveFailureGuard,
    sleep_or_stop,
)
from soveren_agent_platform.sessions.backend import (
    CaptureResult,
    DeliveryAbortBackend,
    DeliveryCaptureBackend,
    SendReceipt,
    TenantBoundaryError,
    ensure_conversation_boundary,
)
from soveren_agent_platform.sessions.contracts import (
    MailboxItem,
    RuntimeSession,
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

log = logging.getLogger(__name__)

IDLE_INITIAL_S = 1.0
IDLE_MAX_S = 10.0
BATCH_SIZE = 5
STALE_SENDING_S = 30 * 60
CAPTURE_RETRY_AFTER_S = 5
CAPTURE_PENDING_TIMEOUT_S = 15 * 60


def lease_owner() -> str:
    return f"{socket.gethostname()}/session-mailbox"


async def run_session_mailbox_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
    capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
) -> None:
    async with await SQLiteSessionStore.open(db_path) as session_store:
        await run_session_mailbox_store_worker(
            session_store,
            SQLiteSessionMailboxStore._from_connection(session_store._conn),
            stop_event,
            tenant_id=tenant_id,
            session_backends=session_backends,
            stale_sending_s=stale_sending_s,
            capture_pending_timeout_s=capture_pending_timeout_s,
            max_consecutive_failures=max_consecutive_failures,
            event_store=SQLiteSessionEventStore._from_connection(session_store._conn),
            snapshot_store=SQLiteSessionSnapshotStore._from_connection(session_store._conn),
        )


async def run_session_mailbox_store_worker(
    session_store: SessionStore,
    mailbox_store: SessionMailboxStore,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    stale_sending_s: int = STALE_SENDING_S,
    capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> None:
    idle = idle_initial_s
    failures = ConsecutiveFailureGuard(max_consecutive_failures)
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
                    capture_pending_timeout_s=capture_pending_timeout_s,
                    event_store=event_store,
                    snapshot_store=snapshot_store,
                )
            except Exception:
                failure_count = failures.record_failure()
                log.exception(
                    "session mailbox drain failed consecutive_failures=%d/%d",
                    failure_count,
                    failures.limit,
                )
                if failures.exhausted:
                    raise
                processed = 0
            else:
                failures.reset()
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
    capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> int:
    return await drain_store_once(
        SQLiteSessionStore._from_connection(conn),
        SQLiteSessionMailboxStore._from_connection(conn),
        tenant_id=tenant_id,
        session_backends=session_backends,
        stale_sending_s=stale_sending_s,
        capture_pending_timeout_s=capture_pending_timeout_s,
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
    capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
) -> int:
    if capture_pending_timeout_s < 1:
        raise ValueError("capture_pending_timeout_s must be positive")
    processed = 0
    processed += await _fail_stale_sending(
        mailbox_store,
        tenant_id=tenant_id,
        stale_sending_s=stale_sending_s,
    )
    ready_sessions = await mailbox_store.ready_sessions(tenant_id=tenant_id, limit=BATCH_SIZE)
    for ready in ready_sessions:
        session = await session_store.get(
            ready.session_id,
            tenant_id=tenant_id,
            source_id=ready.source_id,
        )
        if session is None:
            continue
        item = await mailbox_store.claim_next(
            ready.session_id,
            tenant_id=tenant_id,
            source_id=ready.source_id,
        )
        if item is None:
            continue
        processed += 1
        await _send_item(
            session_store,
            mailbox_store,
            item,
            session=session,
            session_backends=session_backends,
            event_store=event_store,
            snapshot_store=snapshot_store,
            capture_pending_timeout_s=capture_pending_timeout_s,
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
    session: RuntimeSession,
    session_backends: SessionBackendMapping,
    event_store: SessionEventStore | None = None,
    snapshot_store: SessionSnapshotStore | None = None,
    capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
) -> None:
    if (
        session.id != item.session_id
        or session.tenant_id != item.tenant_id
        or session.source_id != item.source_id
    ):
        await mailbox_store.mark_failed(
            item.id,
            tenant_id=item.tenant_id,
            source_id=item.source_id,
            last_error="runtime session does not belong to mailbox conversation",
        )
        return

    backend = normalize_session_backends(session_backends).get(session.backend)
    if backend is None:
        await mailbox_store.fail_delivery(
            item.id,
            session_id=session.id,
            tenant_id=item.tenant_id,
            source_id=item.source_id,
            last_error=f"no backend registered for {session.backend!r}",
        )
        return
    try:
        ensure_conversation_boundary(
            backend,
            session.tenant_id,
            session.source_id,
            resource_name=f"session backend {session.backend!r}",
        )
    except TenantBoundaryError as exc:
        log.error("session backend tenant mismatch session_id=%s backend=%s", session.id, session.backend)
        await mailbox_store.fail_delivery(
            item.id,
            session_id=session.id,
            tenant_id=item.tenant_id,
            source_id=item.source_id,
            last_error=str(exc),
        )
        return

    await session_store.set_status(
        session.id,
        "busy",
        tenant_id=item.tenant_id,
        source_id=item.source_id,
        current_action_id=item.action_id,
    )
    newly_accepted = item.accepted_at is None
    accepted_at = item.accepted_at
    receipt = _receipt_from_payload(item.backend_receipt)
    if newly_accepted:
        try:
            receipt = await backend.send(session.backend_session_id, item.prompt)
            receipt_payload = _send_receipt_payload(receipt)
        except Exception as exc:
            err = f"delivery outcome is uncertain; automatic resend disabled: {type(exc).__name__}: {exc}"
            log.exception("session mailbox delivery became uncertain id=%s", item.id)
            await mailbox_store.fail_delivery(
                item.id,
                session_id=session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
                last_error=err,
            )
            return
        await mailbox_store.mark_accepted(
            item.id,
            tenant_id=item.tenant_id,
            source_id=item.source_id,
            backend_receipt=receipt_payload,
        )
        accepted_at = int(time.time())

    if newly_accepted and event_store is not None:
        try:
            await event_store.record(
                session_id=session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
                direction="input",
                payload_text=item.prompt,
                action_id=item.action_id,
                marker=f"mailbox:{item.id}:input",
            )
        except Exception:
            log.exception("session mailbox input event recording failed id=%s", item.id)

    try:
        if receipt is not None and isinstance(backend, DeliveryCaptureBackend):
            capture_value = await backend.capture_delivery(session.backend_session_id, receipt)
        else:
            capture_value = await backend.capture(session.backend_session_id)
        capture = _capture_result(capture_value)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.exception("session mailbox capture failed id=%s", item.id)
        await mailbox_store.defer_accepted(
            item.id,
            session_id=session.id,
            tenant_id=item.tenant_id,
            source_id=item.source_id,
            current_action_id=item.action_id,
            last_error=err,
            retry_after_s=CAPTURE_RETRY_AFTER_S,
        )
        return

    if capture.timed_out:
        pending_error = "backend capture timed out; accepted delivery remains pending"
        if accepted_at is not None and int(time.time()) - accepted_at >= capture_pending_timeout_s:
            deadline_error = "backend capture deadline exceeded for accepted delivery"
            if receipt is not None and isinstance(backend, DeliveryAbortBackend):
                try:
                    await backend.abort_delivery(session.backend_session_id, receipt)
                except Exception as exc:
                    log.exception("session mailbox deadline abort failed id=%s", item.id)
                    deadline_error = (
                        f"{deadline_error}; backend abort failed: {_bounded_exception_text(exc)}"
                    )
            await mailbox_store.fail_delivery(
                item.id,
                session_id=session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
                last_error=deadline_error,
            )
        else:
            await mailbox_store.defer_pending(
                item.id,
                session_id=session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
                current_action_id=item.action_id,
                last_error=pending_error,
                retry_after_s=CAPTURE_RETRY_AFTER_S,
            )
        return

    await mailbox_store.complete_delivery(
        item.id,
        session_id=session.id,
        tenant_id=item.tenant_id,
        source_id=item.source_id,
        result={"output": capture.text, "timed_out": False},
        session_status="idle",
    )
    if event_store is not None and capture.text.strip():
        try:
            await event_store.record(
                session_id=session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
                direction="output",
                payload_text=capture.text,
                action_id=item.action_id,
                marker=f"mailbox:{item.id}:output",
            )
        except Exception:
            log.exception("session mailbox output event recording failed id=%s", item.id)
    if snapshot_store is not None:
        try:
            await snapshot_store.refresh(
                session.id,
                tenant_id=item.tenant_id,
                source_id=item.source_id,
            )
        except Exception:
            log.exception("session mailbox snapshot refresh failed session_id=%s", session.id)


def _bounded_exception_text(exc: Exception, *, limit: int = 500) -> str:
    detail = f"{type(exc).__name__}: {exc}"
    if len(detail) <= limit:
        return detail
    return detail[: limit - 3] + "..."


def _send_receipt_payload(receipt: object) -> dict[str, object] | None:
    if receipt is None:
        return None
    if not isinstance(receipt, SendReceipt):
        raise TypeError("session backend send() must return SendReceipt or None")
    if receipt.backend_operation_id is not None and not isinstance(receipt.backend_operation_id, str):
        raise TypeError("SendReceipt.backend_operation_id must be a string or None")
    metadata = receipt.metadata or {}
    if not isinstance(metadata, dict):
        raise TypeError("SendReceipt.metadata must be a dictionary or None")
    try:
        json.dumps(metadata, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise TypeError("SendReceipt.metadata must be JSON serializable") from exc
    return {
        "backend_operation_id": receipt.backend_operation_id,
        "metadata": metadata,
    }


def _capture_result(value: object) -> CaptureResult:
    if not isinstance(value, CaptureResult):
        raise TypeError("session backend capture() must return CaptureResult")
    if not isinstance(value.text, str):
        raise TypeError("CaptureResult.text must be a string")
    if type(value.timed_out) is not bool:
        raise TypeError("CaptureResult.timed_out must be a boolean")
    return value


def _receipt_from_payload(payload: dict[str, object] | None) -> SendReceipt | None:
    if not payload:
        return None
    operation_id = payload.get("backend_operation_id")
    metadata = payload.get("metadata")
    return SendReceipt(
        backend_operation_id=operation_id if isinstance(operation_id, str) else None,
        metadata=metadata if isinstance(metadata, dict) else None,
    )
