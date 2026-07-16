"""Atomic persistence boundary for inspected session context."""

from __future__ import annotations

import sqlite3

from soveren_agent_platform.sessions.contracts import SessionIndexUpdate, SessionInspection
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.snapshots import refresh_snapshot


def index_session_inspection(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    inspection: SessionInspection,
) -> SessionIndexUpdate:
    """Record a new inspection and refresh its searchable snapshot atomically."""
    inspection.validate_for_session(session_id)
    conn.execute("BEGIN IMMEDIATE")
    try:
        marker_event = _latest_marker_event(
            conn,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            marker=inspection.marker,
        )
        if marker_event is not None:
            if _latest_snapshot_covers_event(
                conn,
                session_id=session_id,
                event=marker_event,
            ):
                conn.execute("COMMIT")
                return SessionIndexUpdate(recorded=False, snapshot_id=None)
            snapshot_id = refresh_snapshot(
                conn,
                session_id,
                tenant_id=tenant_id,
                source_id=source_id,
            )
            conn.execute("COMMIT")
            return SessionIndexUpdate(recorded=False, snapshot_id=snapshot_id)
        record_session_event(
            conn,
            session_id=session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            direction=inspection.direction,
            payload_text=inspection.payload_text,
            marker=inspection.marker,
        )
        snapshot_id = refresh_snapshot(
            conn,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
        conn.execute("COMMIT")
        return SessionIndexUpdate(recorded=True, snapshot_id=snapshot_id)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _latest_marker_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    marker: str | None,
) -> sqlite3.Row | None:
    if not marker:
        return None
    return conn.execute(
        "SELECT event.id, event.created_at, event.rowid AS event_rowid"
        " FROM runtime_session_events event"
        " JOIN runtime_sessions session ON session.id = event.session_id"
        " WHERE event.session_id = ? AND session.tenant_id = ? AND session.source_id = ?"
        "   AND event.marker = ?"
        " ORDER BY event.created_at DESC, event.rowid DESC LIMIT 1",
        (session_id, tenant_id, source_id, marker),
    ).fetchone()


def _latest_snapshot_covers_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    event: sqlite3.Row,
) -> bool:
    source = conn.execute(
        "SELECT source_event.created_at AS source_created_at,"
        "       source_event.rowid AS source_event_rowid"
        " FROM runtime_session_context_snapshots snapshot"
        " LEFT JOIN runtime_session_events source_event"
        "   ON source_event.id = snapshot.source_event_id"
        "  AND source_event.session_id = snapshot.session_id"
        " WHERE snapshot.session_id = ?"
        " ORDER BY snapshot.created_at DESC, snapshot.rowid DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    if source is None or source["source_created_at"] is None:
        return False
    return (source["source_created_at"], source["source_event_rowid"]) >= (
        event["created_at"],
        event["event_rowid"],
    )
