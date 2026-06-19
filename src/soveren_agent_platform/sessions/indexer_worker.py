"""Worker that refreshes generalized session context from backend inspectors."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from soveren_agent_platform.runtime.worker_loop import sleep_or_stop
from soveren_agent_platform.sessions.contracts import (
    RuntimeSessionEvent,
    SessionEventStore,
    SessionInspection,
    SessionSnapshotStore,
    SessionStore,
)
from soveren_agent_platform.sessions.inspector_registry import SessionInspectorMapping, normalize_session_inspectors
from soveren_agent_platform.sessions.sqlite import (
    SQLiteSessionEventStore,
    SQLiteSessionSnapshotStore,
    SQLiteSessionStore,
)
from soveren_agent_platform.storage.sqlite import open_sqlite

log = logging.getLogger(__name__)

POLL_INTERVAL_S = 30.0
BATCH_SIZE = 20
RECENT_EVENT_LIMIT = 5


async def run_session_indexer_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    poll_interval_s: float = POLL_INTERVAL_S,
    batch_size: int = BATCH_SIZE,
) -> None:
    conn = open_sqlite(db_path)
    try:
        await run_session_indexer_store_worker(
            SQLiteSessionStore(conn),
            SQLiteSessionEventStore(conn),
            SQLiteSessionSnapshotStore(conn),
            stop_event,
            tenant_id=tenant_id,
            session_inspectors=session_inspectors,
            poll_interval_s=poll_interval_s,
            batch_size=batch_size,
        )
    finally:
        conn.close()


async def run_session_indexer_store_worker(
    session_store: SessionStore,
    event_store: SessionEventStore,
    snapshot_store: SessionSnapshotStore,
    stop_event: asyncio.Event,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    poll_interval_s: float = POLL_INTERVAL_S,
    batch_size: int = BATCH_SIZE,
) -> None:
    log.info(
        "session indexer worker started inspectors=%s",
        ",".join(sorted(normalize_session_inspectors(session_inspectors))) or "off",
    )
    try:
        while not stop_event.is_set():
            try:
                await index_store_once(
                    session_store,
                    event_store,
                    snapshot_store,
                    tenant_id=tenant_id,
                    session_inspectors=session_inspectors,
                    batch_size=batch_size,
                )
            except Exception:
                log.exception("session indexer refresh failed")
            await sleep_or_stop(stop_event, poll_interval_s)
    finally:
        log.info("session indexer worker stopped")


async def index_store_once(
    session_store: SessionStore,
    event_store: SessionEventStore,
    snapshot_store: SessionSnapshotStore,
    *,
    tenant_id: str,
    session_inspectors: SessionInspectorMapping,
    batch_size: int = BATCH_SIZE,
) -> int:
    inspectors = normalize_session_inspectors(session_inspectors)
    sessions = await session_store.list_active(tenant_id=tenant_id, limit=batch_size)
    refreshed = 0
    for session in sessions:
        inspector = inspectors.get(session.backend)
        if inspector is None:
            continue
        try:
            inspection = await inspector.inspect(session)
        except Exception:
            log.exception("session inspection failed session_id=%s backend=%s", session.id, session.backend)
            continue
        if inspection is None or not inspection.payload_text.strip():
            continue
        if await _already_recorded(event_store, session.id, inspection):
            continue
        await event_store.record(
            session_id=session.id,
            direction=inspection.direction,
            payload_text=inspection.payload_text,
            marker=inspection.marker,
        )
        await snapshot_store.refresh(session.id)
        refreshed += 1
    return refreshed


async def _already_recorded(
    event_store: SessionEventStore,
    session_id: str,
    inspection: SessionInspection,
) -> bool:
    if not inspection.marker:
        return False
    recent = await event_store.recent(session_id, limit=RECENT_EVENT_LIMIT)
    return any(_same_marker(event, inspection.marker) for event in recent)


def _same_marker(event: RuntimeSessionEvent, marker: str) -> bool:
    return event.marker == marker
