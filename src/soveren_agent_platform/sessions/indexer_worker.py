"""Worker that refreshes generalized session context from backend inspectors."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from soveren_agent_platform.runtime.worker_loop import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ConsecutiveFailureGuard,
    sleep_or_stop,
)
from soveren_agent_platform.sessions.backend import TenantBoundaryError, ensure_conversation_boundary
from soveren_agent_platform.sessions.contracts import (
    SessionIndexStore,
    SessionInspection,
    SessionStore,
)
from soveren_agent_platform.sessions.inspector_registry import SessionInspectorMapping, normalize_session_inspectors
from soveren_agent_platform.sessions.sqlite import (
    SQLiteSessionIndexStore,
    SQLiteSessionStore,
)

log = logging.getLogger(__name__)

POLL_INTERVAL_S = 30.0
BATCH_SIZE = 20


async def run_session_indexer_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    poll_interval_s: float = POLL_INTERVAL_S,
    batch_size: int = BATCH_SIZE,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
) -> None:
    async with await SQLiteSessionStore.open(db_path) as session_store:
        await run_session_indexer_store_worker(
            session_store,
            SQLiteSessionIndexStore._from_connection(session_store._conn),
            stop_event,
            tenant_id=tenant_id,
            session_inspectors=session_inspectors,
            poll_interval_s=poll_interval_s,
            batch_size=batch_size,
            max_consecutive_failures=max_consecutive_failures,
        )


async def run_session_indexer_store_worker(
    session_store: SessionStore,
    index_store: SessionIndexStore,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    poll_interval_s: float = POLL_INTERVAL_S,
    batch_size: int = BATCH_SIZE,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    log.info(
        "session indexer worker started inspectors=%s",
        ",".join(sorted(normalize_session_inspectors(session_inspectors))) or "off",
    )
    failures = ConsecutiveFailureGuard(max_consecutive_failures)
    after_session_id: str | None = None
    try:
        while not stop_event.is_set():
            try:
                _, after_session_id = await _index_store_page(
                    session_store,
                    index_store,
                    tenant_id=tenant_id,
                    session_inspectors=session_inspectors,
                    batch_size=batch_size,
                    after_session_id=after_session_id,
                )
            except Exception:
                failure_count = failures.record_failure()
                log.exception(
                    "session indexer refresh failed consecutive_failures=%d/%d",
                    failure_count,
                    failures.limit,
                )
                if failures.exhausted:
                    raise
            else:
                failures.reset()
            await sleep_or_stop(stop_event, poll_interval_s)
    finally:
        log.info("session indexer worker stopped")


async def index_store_once(
    session_store: SessionStore,
    index_store: SessionIndexStore,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    batch_size: int = BATCH_SIZE,
) -> int:
    refreshed, _ = await _index_store_page(
        session_store,
        index_store,
        tenant_id=tenant_id,
        session_inspectors=session_inspectors,
        batch_size=batch_size,
        after_session_id=None,
    )
    return refreshed


async def _index_store_page(
    session_store: SessionStore,
    index_store: SessionIndexStore,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    batch_size: int,
    after_session_id: str | None,
) -> tuple[int, str | None]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    inspectors = normalize_session_inspectors(session_inspectors)
    sessions = await session_store.list_active(
        tenant_id=tenant_id,
        limit=batch_size + 1,
        after_session_id=after_session_id,
    )
    if not sessions and after_session_id is not None:
        sessions = await session_store.list_active(
            tenant_id=tenant_id,
            limit=batch_size + 1,
        )
    page = sessions[:batch_size]
    next_session_id = page[-1].id if len(sessions) > batch_size else None
    refreshed = 0
    for session in page:
        inspector = inspectors.get(session.backend)
        if inspector is None:
            continue
        try:
            ensure_conversation_boundary(
                inspector,
                session.tenant_id,
                session.source_id,
                resource_name=f"session inspector {session.backend!r}",
            )
            inspection = await inspector.inspect(session)
        except TenantBoundaryError:
            log.error(
                "session inspector conversation mismatch session_id=%s backend=%s",
                session.id,
                session.backend,
            )
            continue
        except Exception:
            log.exception("session inspection failed session_id=%s backend=%s", session.id, session.backend)
            continue
        if inspection is None:
            continue
        try:
            if not isinstance(inspection, SessionInspection):
                raise TypeError("session inspector must return SessionInspection or None")
            inspection.validate_for_session(session.id)
        except Exception:
            log.exception(
                "session inspection rejected session_id=%s backend=%s",
                session.id,
                session.backend,
            )
            continue
        if not inspection.payload_text.strip():
            continue
        try:
            update = await index_store.index_inspection(
                session_id=session.id,
                tenant_id=session.tenant_id,
                source_id=session.source_id,
                inspection=inspection,
            )
        except Exception:
            log.exception(
                "session index persistence failed session_id=%s backend=%s",
                session.id,
                session.backend,
            )
            continue
        if update.snapshot_id is not None:
            refreshed += 1
    return refreshed, next_session_id
