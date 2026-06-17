"""SQLite store for generic actions."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any


def _now() -> int:
    return int(time.time())


def insert_action(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    kind: str,
    payload: dict[str, Any],
    run_id: str | None = None,
    approval_policy: str = "manual",
    source_id: str | None = None,
    source_event_id: str | None = None,
    idempotency_key: str | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    """Create an action. Return `(action_id, created)`."""
    if idempotency_key is not None:
        existing = conn.execute(
            "SELECT id FROM actions WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            return existing["id"], False
    now = now if now is not None else _now()
    action_id = "act_" + uuid.uuid4().hex
    status = "approved" if approval_policy == "auto" else "pending"
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
    return action_id, True


def get_action(conn: sqlite3.Connection, action_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM actions WHERE id = ?", (action_id,)).fetchone()


def approve_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    approver_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'approved', approved_by = ?, approved_at = ?, updated_at = ?"
        " WHERE id = ? AND status = 'pending'",
        (approver_id, now, now, action_id),
    )
    return cur.rowcount > 0


def deny_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    approver_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions"
        " SET status = 'denied', approved_by = ?, approved_at = ?, updated_at = ?"
        " WHERE id = ? AND status = 'pending'",
        (approver_id, now, now, action_id),
    )
    return cur.rowcount > 0


def mark_executing(conn: sqlite3.Connection, action_id: str, *, now: int | None = None) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE actions SET status = 'executing', updated_at = ?"
        " WHERE id = ? AND status IN ('approved','queued')",
        (now, action_id),
    )
    return cur.rowcount > 0


def mark_queued(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE actions SET status = 'queued', result_json = ?, updated_at = ?"
        " WHERE id = ? AND status IN ('approved','queued','executing')",
        (json.dumps(result or {}, ensure_ascii=False), now, action_id),
    )


def mark_executed(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    result: dict[str, Any],
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE actions"
        " SET status = 'executed', executed_at = ?, result_json = ?, updated_at = ?"
        " WHERE id = ?",
        (now, json.dumps(result, ensure_ascii=False), now, action_id),
    )


def mark_failed(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    error: str,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE actions"
        " SET status = 'failed', last_error = ?, result_json = ?, updated_at = ?"
        " WHERE id = ?",
        (error[:500], json.dumps({"error": error}, ensure_ascii=False), now, action_id),
    )

