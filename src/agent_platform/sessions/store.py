"""SQLite store for runtime sessions."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any


def insert_session(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    kind: str,
    backend: str,
    backend_session_id: str,
    owner_id: str | None = None,
    title: str = "",
    cwd: str = "",
    status: str = "idle",
    metadata: dict[str, Any] | None = None,
    now: int | None = None,
) -> str:
    now = now if now is not None else int(time.time())
    session_id = "rs_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO runtime_sessions"
        " (id, tenant_id, source_id, owner_id, kind, backend, backend_session_id,"
        "  title, cwd, status, last_used_at, metadata_json, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            session_id,
            tenant_id,
            source_id,
            owner_id,
            kind,
            backend,
            backend_session_id,
            title,
            cwd,
            status,
            now,
            json.dumps(metadata or {}, ensure_ascii=False),
            now,
            now,
        ),
    )
    return session_id


def get_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()


def list_active_sessions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    limit: int,
) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM runtime_sessions"
        " WHERE tenant_id = ? AND status != 'closed'"
        " ORDER BY last_used_at DESC, updated_at DESC LIMIT ?",
        (tenant_id, limit),
    ))


def set_session_status(
    conn: sqlite3.Connection,
    session_id: str,
    status: str,
    *,
    current_action_id: str | None = None,
    last_error: str | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else int(time.time())
    conn.execute(
        "UPDATE runtime_sessions SET"
        " status = ?, current_action_id = ?, last_error = ?, updated_at = ?, last_used_at = ?"
        " WHERE id = ?",
        (status, current_action_id, last_error, now, now, session_id),
    )
