"""SQLite helpers for app-neutral memory records."""
from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.idempotency import require_idempotent_replay, stored_json_matches
from soveren_agent_platform.memory.contracts import MemoryRecord

MAX_SEARCH_CANDIDATES = 200
_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_./-]{3,}")


def remember(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    scope: str,
    subject_id: str,
    text: str,
    kind: str = "note",
    metadata: dict[str, Any] | None = None,
    confidence: float = 1.0,
    source_event_id: str | None = None,
    created_by: str | None = None,
    idempotency_key: str | None = None,
    expires_at: int | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    if not tenant_id.strip():
        raise ValueError("tenant_id is required")
    if not source_id.strip():
        raise ValueError("source_id is required")
    if not scope.strip():
        raise ValueError("scope is required")
    if not subject_id.strip():
        raise ValueError("subject_id is required")
    if not text.strip():
        raise ValueError("text is required")
    now = now if now is not None else int(time.time())
    memory_id = "mem_" + uuid.uuid4().hex
    values = (
        memory_id,
        tenant_id,
        scope,
        subject_id,
        kind,
        text,
        json.dumps(metadata or {}, ensure_ascii=False),
        confidence,
        source_id,
        source_event_id,
        created_by,
        idempotency_key,
        expires_at,
        now,
        now,
    )
    insert_sql = (
        "INSERT INTO memory_records ("
        "  id, tenant_id, scope, subject_id, kind, text, metadata_json,"
        "  confidence, source_id, source_event_id, created_by, idempotency_key,"
        "  expires_at, created_at, updated_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    if idempotency_key is None:
        conn.execute(insert_sql, values)
        return memory_id, True

    inserted = conn.execute(
        f"{insert_sql} ON CONFLICT DO NOTHING RETURNING id",
        values,
    ).fetchone()
    if inserted is not None:
        return str(inserted["id"]), True
    existing = conn.execute(
        "SELECT * FROM memory_records"
        " WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
        (tenant_id, source_id, idempotency_key),
    ).fetchone()
    if existing is None:
        raise RuntimeError("memory insert conflict did not resolve to the idempotency key")
    require_idempotent_replay(
        existing["scope"] == scope
        and existing["subject_id"] == subject_id
        and existing["kind"] == kind
        and existing["text"] == text
        and stored_json_matches(existing["metadata_json"], metadata or {})
        and existing["confidence"] == confidence
        and existing["source_event_id"] == source_event_id
        and existing["created_by"] == created_by
        and existing["expires_at"] == expires_at,
        resource="memory record",
        key=idempotency_key,
        existing_id=existing["id"],
    )
    return str(existing["id"]), False


def get_memory(
    conn: sqlite3.Connection,
    memory_id: str,
    *,
    tenant_id: str,
    source_id: str,
    now: int | None = None,
) -> MemoryRecord | None:
    now = now if now is not None else int(time.time())
    row = conn.execute(
        "SELECT * FROM memory_records"
        " WHERE tenant_id = ? AND source_id = ? AND id = ? AND deleted_at IS NULL"
        " AND (expires_at IS NULL OR expires_at > ?)",
        (tenant_id, source_id, memory_id, now),
    ).fetchone()
    return row_to_memory(row) if row is not None else None


def search_memory(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    query: str = "",
    scope: str | None = None,
    subject_id: str | None = None,
    kind: str | None = None,
    limit: int = 10,
    now: int | None = None,
) -> list[MemoryRecord]:
    now = now if now is not None else int(time.time())
    safe_limit = max(1, min(limit, 50))
    params: list[Any] = [tenant_id, source_id, now]
    filters = [
        "tenant_id = ?",
        "source_id = ?",
        "deleted_at IS NULL",
        "(expires_at IS NULL OR expires_at > ?)",
    ]
    if scope is not None:
        filters.append("scope = ?")
        params.append(scope)
    if subject_id is not None:
        filters.append("subject_id = ?")
        params.append(subject_id)
    if kind is not None:
        filters.append("kind = ?")
        params.append(kind)
    rows = conn.execute(
        "SELECT * FROM memory_records"
        f" WHERE {' AND '.join(filters)}"
        " ORDER BY updated_at DESC, rowid DESC"
        " LIMIT ?",
        (*params, MAX_SEARCH_CANDIDATES),
    ).fetchall()
    query_tokens = set(_tokens(query))
    if not query_tokens:
        return [row_to_memory(row) for row in rows[:safe_limit]]
    scored: list[tuple[int, sqlite3.Row]] = []
    for row in rows:
        haystack = " ".join([
            row["kind"] or "",
            row["text"] or "",
            row["metadata_json"] or "",
        ])
        score = len(query_tokens & set(_tokens(haystack)))
        if score:
            scored.append((score, row))
    scored.sort(key=lambda item: (-item[0], -(item[1]["updated_at"] or 0)))
    return [row_to_memory(row) for _, row in scored[:safe_limit]]


def forget_memory(
    conn: sqlite3.Connection,
    memory_id: str,
    *,
    tenant_id: str,
    source_id: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else int(time.time())
    cursor = conn.execute(
        "UPDATE memory_records SET deleted_at = ?, updated_at = ?"
        " WHERE tenant_id = ? AND source_id = ? AND id = ? AND deleted_at IS NULL",
        (now, now, tenant_id, source_id, memory_id),
    )
    return cursor.rowcount > 0


def row_to_memory(row: sqlite3.Row) -> MemoryRecord:
    return MemoryRecord(
        id=row["id"],
        tenant_id=row["tenant_id"],
        scope=row["scope"],
        subject_id=row["subject_id"],
        kind=row["kind"],
        text=row["text"],
        metadata=_json_dict(row["metadata_json"]),
        confidence=float(row["confidence"]),
        source_id=row["source_id"],
        source_event_id=row["source_event_id"],
        created_by=row["created_by"],
        idempotency_key=row["idempotency_key"],
        expires_at=row["expires_at"],
        deleted_at=row["deleted_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _json_dict(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _tokens(text: str) -> list[str]:
    return [item.strip("./-_").lower() for item in _TOKEN_RE.findall(text) if item.strip("./-_")]
