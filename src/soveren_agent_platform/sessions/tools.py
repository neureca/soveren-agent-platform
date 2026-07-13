"""Platform-level dynamic tools for generalized runtime sessions."""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from soveren_agent_platform.model_boundary import ModelRedactionPolicy, redact_value_for_model
from soveren_agent_platform.sessions.backend import ensure_conversation_boundary
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)
from soveren_agent_platform.sessions.contracts import SessionInspection
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.inspector_registry import SessionInspectorMapping, normalize_session_inspectors
from soveren_agent_platform.sessions.snapshots import latest_snapshot, refresh_snapshot, snapshot_keywords
from soveren_agent_platform.sessions.sqlite import row_to_session
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite

SESSION_TOOL_NAMESPACE = "platform.sessions"
_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_./-]{3,}")


class SQLiteSessionDirectoryTools(SQLiteAdapter):
    """Register tenant-scoped session discovery tools backed by platform storage."""

    def register(
        self,
        registry: DynamicToolRegistry,
        *,
        tenant_id: str,
        source_id: str,
        session_inspectors: SessionInspectorMapping | None = None,
        model_redaction_policy: ModelRedactionPolicy | None = None,
    ) -> None:
        register_session_directory_tools(
            registry,
            self._conn,
            tenant_id=tenant_id,
            source_id=source_id,
            session_inspectors=session_inspectors,
            model_redaction_policy=model_redaction_policy,
        )


def register_session_directory_tools(
    registry: DynamicToolRegistry,
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    session_inspectors: SessionInspectorMapping | None = None,
    model_redaction_policy: ModelRedactionPolicy | None = None,
) -> None:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    registry.bind_conversation(tenant_id=tenant_id, source_id=source_id)
    registry.register(
        DynamicToolSpec(
            name="list_runtime_sessions",
            namespace=SESSION_TOOL_NAMESPACE,
            description="List generalized runtime sessions known to the platform.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20},
                },
            },
        ),
        lambda call: _list_runtime_sessions_tool(
            conn,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
            model_redaction_policy=model_redaction_policy,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="search_session_snapshots",
            namespace=SESSION_TOOL_NAMESPACE,
            description="Search generalized session snapshots by text query.",
            input_schema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20},
                },
            },
        ),
        lambda call: _search_session_snapshots_tool(
            conn,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
            model_redaction_policy=model_redaction_policy,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="get_session_context",
            namespace=SESSION_TOOL_NAMESPACE,
            description="Read one generalized session with latest snapshot, recent events, and mailbox state.",
            input_schema={
                "type": "object",
                "required": ["session_id"],
                "properties": {"session_id": {"type": "string"}},
            },
        ),
        lambda call: _get_session_context_tool(
            conn,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
            model_redaction_policy=model_redaction_policy,
        ),
    )
    if session_inspectors is not None:
        registry.register(
            DynamicToolSpec(
                name="refresh_session_candidate",
                namespace=SESSION_TOOL_NAMESPACE,
                description="Refresh one generalized session through its backend inspector.",
                input_schema={
                    "type": "object",
                    "required": ["session_id"],
                    "properties": {"session_id": {"type": "string"}},
                },
            ),
            lambda call: _refresh_session_candidate(
                conn,
                tenant_id=tenant_id,
                source_id=source_id,
                session_id=str(_args(call).get("session_id") or ""),
                session_inspectors=session_inspectors,
            ),
        )


async def _list_runtime_sessions_tool(
    conn: sqlite3.Connection,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> DynamicToolResult:
    payload = await run_sqlite(
        conn,
        _list_runtime_sessions,
        tenant_id=tenant_id,
        source_id=source_id,
        limit=_limit_arg(call, default=8),
    )
    return DynamicToolResult.json(_model_payload(payload, policy=model_redaction_policy))


async def _search_session_snapshots_tool(
    conn: sqlite3.Connection,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> DynamicToolResult:
    payload = await run_sqlite(
        conn,
        _search_session_snapshots,
        tenant_id=tenant_id,
        source_id=source_id,
        query=str(_args(call).get("query") or ""),
        limit=_limit_arg(call, default=8),
    )
    return DynamicToolResult.json(_model_payload(payload, policy=model_redaction_policy))


async def _get_session_context_tool(
    conn: sqlite3.Connection,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> DynamicToolResult:
    payload = await run_sqlite(
        conn,
        _get_session_context,
        tenant_id=tenant_id,
        source_id=source_id,
        session_id=str(_args(call).get("session_id") or ""),
    )
    return DynamicToolResult.json(_model_payload(payload, policy=model_redaction_policy))


def _list_runtime_sessions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    limit: int,
) -> dict[str, Any]:
    rows = _session_rows(conn, tenant_id=tenant_id, source_id=source_id, limit=limit)
    return {"sessions": [_session_payload(conn, row, include_snapshot=True) for row in rows]}


def _search_session_snapshots(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    query: str,
    limit: int,
) -> dict[str, Any]:
    query_tokens = set(_tokens(query))
    rows = _session_rows(conn, tenant_id=tenant_id, source_id=source_id, limit=100)
    scored: list[tuple[int, sqlite3.Row]] = []
    for row in rows:
        snapshot = latest_snapshot(conn, row["id"])
        haystack = " ".join(
            [
                row["id"],
                row["backend_session_id"],
                row["title"] or "",
                row["cwd"] or "",
                snapshot["summary"] if snapshot is not None else "",
                " ".join(snapshot_keywords(snapshot)),
            ]
        )
        score = len(query_tokens & set(_tokens(haystack)))
        if score:
            scored.append((score, row))
    scored.sort(key=lambda item: item[0], reverse=True)
    return {
        "query": query,
        "sessions": [
            {**_session_payload(conn, row, include_snapshot=True), "score": score} for score, row in scored[:limit]
        ],
    }


def _get_session_context(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    session_id: str,
) -> dict[str, Any]:
    row = _session_row(conn, tenant_id=tenant_id, source_id=source_id, session_id=session_id)
    if row is None:
        return {"session": None}
    events = conn.execute(
        "SELECT id, direction, payload_text, action_id, marker, created_at"
        " FROM runtime_session_events"
        " WHERE session_id = ?"
        " ORDER BY created_at DESC, rowid DESC LIMIT 10",
        (session_id,),
    ).fetchall()
    return {
        "session": _session_payload(conn, row, include_snapshot=True),
        "events": [
            {
                "direction": event["direction"],
                "payload_text": event["payload_text"],
                "created_at": event["created_at"],
            }
            for event in events
        ],
        "mailbox": _mailbox_counts(conn, session_id),
    }


async def _refresh_session_candidate(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    session_id: str,
    session_inspectors: SessionInspectorMapping,
) -> DynamicToolResult:
    row = await run_sqlite(
        conn,
        _session_row,
        tenant_id=tenant_id,
        source_id=source_id,
        session_id=session_id,
    )
    if row is None:
        return DynamicToolResult.json({"refreshed": False, "reason": "session not found"}, success=False)
    session = row_to_session(row)
    inspector = normalize_session_inspectors(session_inspectors).get(session.backend)
    if inspector is None:
        return DynamicToolResult.json({"refreshed": False, "reason": "session inspector not registered"}, success=False)
    ensure_conversation_boundary(
        inspector,
        session.tenant_id,
        session.source_id,
        resource_name=f"session inspector {session.backend!r}",
    )
    inspection = await inspector.inspect(session)
    if inspection is None or not inspection.payload_text.strip():
        return DynamicToolResult.json({"refreshed": False, "reason": "empty inspection"})
    stored, snapshot_id = await run_sqlite(
        conn,
        _store_inspection,
        session_id=session.id,
        inspection=inspection,
    )
    if not stored:
        return DynamicToolResult.json({"refreshed": False, "reason": "already current", "session_id": session.id})
    return DynamicToolResult.json({"refreshed": True, "session_id": session.id, "snapshot_id": snapshot_id})


def _store_inspection(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    inspection: SessionInspection,
) -> tuple[bool, str | None]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        if _has_marker(conn, session_id, inspection):
            conn.execute("COMMIT")
            return False, None
        record_session_event(
            conn,
            session_id=session_id,
            direction=inspection.direction,
            payload_text=inspection.payload_text,
            marker=inspection.marker,
        )
        snapshot_id = refresh_snapshot(conn, session_id)
        conn.execute("COMMIT")
        return True, snapshot_id
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _session_rows(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    limit: int,
) -> list[sqlite3.Row]:
    params: list[Any] = [tenant_id, source_id, limit]
    return list(
        conn.execute(
            "SELECT * FROM runtime_sessions"
            " WHERE tenant_id = ? AND status != 'closed'"
            " AND source_id = ?"
            " ORDER BY last_used_at DESC, updated_at DESC LIMIT ?",
            params,
        )
    )


def _session_row(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    session_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runtime_sessions WHERE tenant_id = ? AND id = ? AND source_id = ?",
        (tenant_id, session_id, source_id),
    ).fetchone()


def _session_payload(conn: sqlite3.Connection, row: sqlite3.Row, *, include_snapshot: bool) -> dict[str, Any]:
    payload = {
        "session_id": row["id"],
        "kind": row["kind"],
        "backend": row["backend"],
        "status": row["status"],
        "title": row["title"],
        "cwd": row["cwd"],
        "mailbox": _mailbox_counts(conn, row["id"]),
    }
    if include_snapshot:
        payload["snapshot"] = _snapshot_payload(latest_snapshot(conn, row["id"]))
    return payload


def _snapshot_payload(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "summary": row["summary"],
        "keywords": _json_list(row["keywords_json"]),
        "files": _json_list(row["files_json"]),
        "cwd": row["cwd"],
        "branch": row["branch"],
        "topic_key": row["topic_key"],
        "last_user_intent": row["last_user_intent"],
        "last_agent_state": row["last_agent_state"],
        "confidence": row["confidence"],
        "created_at": row["created_at"],
    }


def _mailbox_counts(conn: sqlite3.Connection, session_id: str) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM session_mailbox WHERE session_id = ? GROUP BY status",
        (session_id,),
    ).fetchall()
    return {str(row["status"]): int(row["count"]) for row in rows}


def _has_marker(conn: sqlite3.Connection, session_id: str, inspection: SessionInspection) -> bool:
    if not inspection.marker:
        return False
    row = conn.execute(
        "SELECT 1 FROM runtime_session_events WHERE session_id = ? AND marker = ? LIMIT 1",
        (session_id, inspection.marker),
    ).fetchone()
    return row is not None


def _args(call: DynamicToolCall) -> dict[str, Any]:
    return call.arguments if isinstance(call.arguments, dict) else {}


def _model_payload(value: dict[str, Any], *, policy: ModelRedactionPolicy | None) -> dict[str, Any]:
    redacted = redact_value_for_model(value, policy=policy)
    return redacted if isinstance(redacted, dict) else {}


def _limit_arg(call: DynamicToolCall, *, default: int) -> int:
    value = _args(call).get("limit")
    if not isinstance(value, int):
        return default
    return max(1, min(value, 20))


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, (str, int, float))]


def _tokens(text: str) -> list[str]:
    return [token.strip("./-_").lower() for token in _TOKEN_RE.findall(text) if len(token.strip("./-_")) >= 3]
