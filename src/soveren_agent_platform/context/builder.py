"""Read-only builder for the platform context sent to planner turns."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context.contracts import PlannerContext
from soveren_agent_platform.sessions.routing import SessionRouteResult
from soveren_agent_platform.storage.adapter import SQLiteAdapter, SQLiteConnectionHandle
from soveren_agent_platform.storage.sqlite import run_sqlite


@dataclass(frozen=True, slots=True)
class ContextLimits:
    max_batch_messages: int = 20
    max_sessions: int = 8
    max_mailbox: int = 12
    max_actions: int = 12
    max_outbound: int = 12
    max_cron: int = 12
    max_text_chars: int = 1200


class SQLitePlannerContextBuilder(SQLiteAdapter):
    """Assemble app-neutral planner context from the platform storage tables."""

    def __init__(self, handle: SQLiteConnectionHandle, *, limits: ContextLimits | None = None) -> None:
        super().__init__(handle)
        self.limits = limits or ContextLimits()

    async def build(self, *, event: AgentEvent, route_result: SessionRouteResult) -> PlannerContext:
        return await run_sqlite(
            self._conn,
            _build_context,
            event=event,
            route_result=route_result,
            limits=self.limits,
        )


async def build_planner_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    route_result: SessionRouteResult,
    limits: ContextLimits | None = None,
) -> PlannerContext:
    return await SQLitePlannerContextBuilder._from_connection(conn, limits=limits).build(
        event=event,
        route_result=route_result,
    )


def _build_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    route_result: SessionRouteResult,
    limits: ContextLimits,
) -> PlannerContext:
    source_id = _source_id(event)
    routed_sessions: list[dict[str, Any]] = [asdict(snapshot) for snapshot in route_result.snapshots]
    sessions = _session_context(
        conn,
        event=event,
        source_id=source_id,
        routed_sessions=routed_sessions,
        limits=limits,
    )
    allowed_session_ids = {str(item["session_id"]) for item in sessions}
    route_hint = asdict(route_result.hint)
    if route_hint.get("session_id") not in allowed_session_ids:
        route_hint["session_id"] = None
        if route_hint.get("action") == "route_existing":
            route_hint["action"] = "no_match"
            route_hint["confidence"] = 0.0
    session_routing: dict[str, Any] = {
        "route_hint": route_hint,
        "sessions": sessions,
    }
    return PlannerContext(
        trigger=_trigger_context(event, source_id=source_id),
        session_routing=session_routing,
        batch=_batch_context(conn, event, source_id=source_id, limits=limits),
        sessions=sessions,
        mailbox=_mailbox_context(conn, event=event, source_id=source_id, limits=limits),
        actions=_action_context(conn, event=event, source_id=source_id, limits=limits),
        outbound=_outbound_context(conn, event=event, source_id=source_id, limits=limits),
        cron=_cron_context(conn, event=event, source_id=source_id, limits=limits),
    )


def _trigger_context(event: AgentEvent, *, source_id: str) -> dict[str, Any]:
    return {
        "event_id": event.id,
        "tenant_id": event.tenant_id,
        "recipient": event.recipient,
        "message_type": event.message_type,
        "correlation_id": event.correlation_id,
        "source_id": source_id,
        "channel": event.payload.get("channel"),
        "user_id": event.payload.get("user_id"),
    }


def _batch_context(
    conn: sqlite3.Connection,
    event: AgentEvent,
    *,
    source_id: str,
    limits: ContextLimits,
) -> dict[str, Any] | None:
    batch_id = event.payload.get("batch_id")
    messages = event.payload.get("batch_messages")
    if isinstance(messages, list) and messages:
        return {
            "batch_id": batch_id,
            "message_count": int(event.payload.get("batch_message_count") or len(messages)),
            "text": _clip(str(event.payload.get("text") or ""), limits.max_text_chars),
            "messages": [_message_context(message, limits=limits) for message in messages[: limits.max_batch_messages]],
        }
    if not isinstance(batch_id, str):
        text = str(event.payload.get("text") or "")
        return {
            "batch_id": None,
            "message_count": 1 if text else 0,
            "text": _clip(text, limits.max_text_chars),
            "messages": [],
        }
    rows = conn.execute(
        "SELECT m.payload_json, m.message_at FROM inbound_batch_messages m"
        " JOIN inbound_batches b ON b.id = m.batch_id AND b.tenant_id = m.tenant_id"
        " WHERE m.batch_id = ? AND m.tenant_id = ? AND b.tenant_id = ?"
        "   AND m.source_id = ? AND b.source_id = ?"
        " ORDER BY m.message_at ASC, m.created_at ASC, m.rowid ASC"
        " LIMIT ?",
        (
            batch_id,
            event.tenant_id,
            event.tenant_id,
            source_id,
            source_id,
            limits.max_batch_messages,
        ),
    ).fetchall()
    if not rows:
        return None
    parsed = [_json(row["payload_json"]) for row in rows]
    return {
        "batch_id": batch_id,
        "message_count": len(parsed),
        "text": _clip("\n".join(str(item.get("text") or "") for item in parsed), limits.max_text_chars),
        "messages": [_message_context(item, limits=limits) for item in parsed],
    }


def _message_context(message: Any, *, limits: ContextLimits) -> dict[str, Any]:
    payload = message if isinstance(message, dict) else {}
    return {
        "text": _clip(str(payload.get("text") or ""), limits.max_text_chars),
        "message_at": payload.get("message_at"),
        "raw_event_id": payload.get("raw_event_id"),
        "source_event_id": payload.get("source_event_id"),
        "from_user_id": payload.get("from_user_id") or payload.get("user_id"),
        "from_username": payload.get("from_username"),
    }


def _session_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    source_id: str,
    routed_sessions: list[dict[str, Any]],
    limits: ContextLimits,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM runtime_sessions"
        " WHERE tenant_id = ? AND source_id = ? AND status IN ('starting','idle','busy','closing','failed')"
        " ORDER BY last_used_at DESC, updated_at DESC LIMIT ?",
        (event.tenant_id, source_id, limits.max_sessions),
    ).fetchall()
    routed_by_id = {
        str(item["session_id"]): dict(item)
        for item in routed_sessions
        if item.get("session_id") is not None
    }
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        session_id = row["id"]
        base = routed_by_id.get(session_id, {})
        by_id[session_id] = {
            **base,
            "session_id": session_id,
            "kind": row["kind"],
            "backend": row["backend"],
            "status": row["status"],
            "title": row["title"],
            "cwd": row["cwd"],
            "last_used_at": row["last_used_at"],
            "current_action_id": row["current_action_id"],
            "last_error": row["last_error"],
            "mailbox": _mailbox_counts(conn, session_id),
        }
    return list(by_id.values())[: limits.max_sessions]


def _mailbox_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    source_id: str,
    limits: ContextLimits,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT m.id, m.session_id, m.source_id, m.source_event_id, m.action_id,"
        "       m.prompt, m.status, m.last_error, m.created_at, m.updated_at,"
        "       s.kind, s.backend, s.title"
        " FROM session_mailbox m"
        " JOIN runtime_sessions s ON s.id = m.session_id"
        " WHERE m.tenant_id = ? AND m.source_id = ?"
        "   AND m.status IN ('queued','sending','failed')"
        " ORDER BY m.created_at ASC, m.rowid ASC LIMIT ?",
        (event.tenant_id, source_id, limits.max_mailbox),
    ).fetchall()
    return [
        {
            "id": row["id"],
            "session_id": row["session_id"],
            "session_kind": row["kind"],
            "session_backend": row["backend"],
            "session_title": row["title"],
            "source_event_id": row["source_event_id"],
            "action_id": row["action_id"],
            "status": row["status"],
            "prompt": _clip(row["prompt"], limits.max_text_chars),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _action_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    source_id: str,
    limits: ContextLimits,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, kind, payload_json, status, approval_policy, source_id,"
        "       source_event_id, approved_by, approved_at, executed_at,"
        "       last_error, created_at, updated_at"
        " FROM actions"
        " WHERE tenant_id = ?"
        "   AND source_id = ?"
        "   AND status IN ('pending','approved','queued','executing','failed','uncertain')"
        " ORDER BY updated_at DESC, rowid DESC LIMIT ?",
        (event.tenant_id, source_id, limits.max_actions),
    ).fetchall()
    return [
        {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "approval_policy": row["approval_policy"],
            "source_id": row["source_id"],
            "source_event_id": row["source_event_id"],
            "payload": _compact_payload(_json(row["payload_json"]), limits=limits),
            "approved_by": row["approved_by"],
            "approved_at": row["approved_at"],
            "executed_at": row["executed_at"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _outbound_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    source_id: str,
    limits: ContextLimits,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, source_id, channel, destination_id, text, status, attempts, max_attempts,"
        "       correlation_id, last_error, run_after, created_at, updated_at"
        " FROM outbound_messages"
        " WHERE tenant_id = ?"
        "   AND source_id = ?"
        "   AND status IN ('queued','leased','sending','retrying','uncertain','dead_letter')"
        " ORDER BY run_after ASC, created_at ASC LIMIT ?",
        (event.tenant_id, source_id, limits.max_outbound),
    ).fetchall()
    return [
        {
            "id": row["id"],
            "channel": row["channel"],
            "destination_id": row["destination_id"],
            "status": row["status"],
            "text": _clip(row["text"], limits.max_text_chars),
            "attempts": row["attempts"],
            "max_attempts": row["max_attempts"],
            "correlation_id": row["correlation_id"],
            "last_error": row["last_error"],
            "run_after": row["run_after"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _cron_context(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    source_id: str,
    limits: ContextLimits,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, source_id, name, payload_json, status, run_at, rrule, timezone,"
        "       attempts, max_attempts, last_error, created_at, updated_at"
        " FROM cron_jobs"
        " WHERE tenant_id = ? AND source_id = ?"
        "   AND status IN ('pending','leased','running','uncertain','dead_letter')"
        " ORDER BY run_at ASC, created_at ASC LIMIT ?",
        (event.tenant_id, source_id, limits.max_cron),
    ).fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "status": row["status"],
            "run_at": row["run_at"],
            "rrule": row["rrule"],
            "timezone": row["timezone"],
            "attempts": row["attempts"],
            "max_attempts": row["max_attempts"],
            "payload": _compact_payload(_json(row["payload_json"]), limits=limits),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _mailbox_counts(conn: sqlite3.Connection, session_id: str) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM session_mailbox"
        " WHERE session_id = ? AND status IN ('queued','sending','failed')"
        " GROUP BY status",
        (session_id,),
    ).fetchall()
    return {row["status"]: int(row["count"]) for row in rows}


def _compact_payload(payload: Any, *, limits: ContextLimits) -> Any:
    if isinstance(payload, dict):
        return {
            str(key): _compact_payload(value, limits=limits)
            for key, value in payload.items()
            if key not in {"auth_header", "token", "password", "secret"}
        }
    if isinstance(payload, list):
        return [_compact_payload(item, limits=limits) for item in payload[:20]]
    if isinstance(payload, str):
        return _clip(payload, limits.max_text_chars)
    return payload


def _source_id(event: AgentEvent) -> str:
    source_id = str(event.payload.get("source_id") or "").strip()
    if not source_id:
        raise ValueError("planner event must contain a non-empty source_id")
    return source_id


def _json(raw: str | None) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _clip(value: str | None, max_chars: int) -> str:
    text = value or ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return "." * max_chars
    return text[: max(0, max_chars - 3)] + "..."
