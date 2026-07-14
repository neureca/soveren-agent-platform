"""SQLite store for generic actions."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.idempotency import require_idempotent_replay, stored_json_matches


def _now() -> int:
    return int(time.time())


def insert_action(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    kind: str,
    payload: dict[str, Any],
    run_id: str | None = None,
    approval_policy: str = "manual",
    source_event_id: str | None = None,
    idempotency_key: str | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    """Create an action. Return `(action_id, created)`."""
    _validate_conversation(tenant_id, source_id)
    if idempotency_key is not None:
        existing = conn.execute(
            "SELECT * FROM actions WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is not None:
            _require_action_replay(
                existing,
                key=idempotency_key,
                kind=kind,
                payload=payload,
                run_id=run_id,
                approval_policy=approval_policy,
                source_event_id=source_event_id,
            )
            return existing["id"], False
    now = now if now is not None else _now()
    action_id = "act_" + uuid.uuid4().hex
    status = "approved" if approval_policy == "auto" else "pending"
    try:
        conn.execute(
            "INSERT INTO actions"
            " (id, tenant_id, run_id, kind, payload_json, status, approval_policy,"
            "  source_id, source_event_id, idempotency_key, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                action_id,
                tenant_id,
                run_id,
                kind,
                json.dumps(payload, ensure_ascii=False),
                status,
                approval_policy,
                source_id,
                source_event_id,
                idempotency_key,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        if idempotency_key is None:
            raise
        existing = conn.execute(
            "SELECT * FROM actions WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is None:
            raise
        _require_action_replay(
            existing,
            key=idempotency_key,
            kind=kind,
            payload=payload,
            run_id=run_id,
            approval_policy=approval_policy,
            source_event_id=source_event_id,
        )
        return existing["id"], False
    return action_id, True


def _require_action_replay(
    row: sqlite3.Row,
    *,
    key: str,
    kind: str,
    payload: dict[str, Any],
    run_id: str | None,
    approval_policy: str,
    source_event_id: str | None,
) -> None:
    require_idempotent_replay(
        row["kind"] == kind
        and stored_json_matches(row["payload_json"], payload)
        and row["run_id"] == run_id
        and row["approval_policy"] == approval_policy
        and row["source_event_id"] == source_event_id,
        resource="action",
        key=key,
        existing_id=row["id"],
    )


def get_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM actions WHERE id = ? AND tenant_id = ? AND source_id = ?",
        (action_id, tenant_id, source_id),
    ).fetchone()


def approve_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    approver_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'approved', approved_by = ?, approved_at = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'pending'",
        (approver_id, now, now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def deny_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    approver_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'denied', approved_by = ?, approved_at = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'pending'",
        (approver_id, now, now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def mark_executing(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions SET status = 'executing', updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status IN ('approved','queued')",
        (now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def mark_queued(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions SET status = 'queued', result_json = ?, last_error = NULL, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'executing'",
        (json.dumps(result or {}, ensure_ascii=False), now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def mark_executed(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    result: dict[str, Any],
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'executed', executed_at = ?, result_json = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status IN ('executing','queued')",
        (now, json.dumps(result, ensure_ascii=False), now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def mark_failed(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'failed', last_error = ?, result_json = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ?"
        " AND status IN ('approved','executing','queued')",
        (
            error[:500],
            json.dumps({"error": error}, ensure_ascii=False),
            now,
            action_id,
            tenant_id,
            source_id,
        ),
    )
    return cur.rowcount > 0


def mark_retryable(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'queued', last_error = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'executing'",
        (error[:500], now, action_id, tenant_id, source_id),
    )
    return cur.rowcount > 0


def mark_uncertain(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'uncertain', last_error = ?, result_json = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'executing'",
        (
            error[:500],
            json.dumps({"execution": "uncertain", "error": error}, ensure_ascii=False),
            now,
            action_id,
            tenant_id,
            source_id,
        ),
    )
    return cur.rowcount > 0


def _validate_conversation(tenant_id: str, source_id: str) -> None:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
