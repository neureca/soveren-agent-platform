"""SQLite operations for conversation history."""

from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.conversation_history.contracts import (
    ConversationMessage,
    ConversationSearchHit,
    MessageDirection,
)
from soveren_agent_platform.idempotency import require_idempotent_replay, stored_json_matches

MAX_SEARCH_QUERY_CHARS = 1000
MAX_SEARCH_TOKENS = 50
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def record_message(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    channel: str,
    direction: MessageDirection,
    text: str,
    source_message_id: str,
    occurred_at: int,
    author_id: str | None = None,
    author_username: str | None = None,
    author_display_name: str | None = None,
    source_event_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    _validate_message(
        tenant_id=tenant_id,
        source_id=source_id,
        channel=channel,
        direction=direction,
        source_message_id=source_message_id,
        occurred_at=occurred_at,
    )
    author_username = _normalize_author_username(author_username)
    author_display_name = _normalize_author_display_name(author_display_name)
    now = now if now is not None else int(time.time())
    message_id = "history_" + uuid.uuid4().hex
    metadata = metadata or {}
    try:
        conn.execute(
            "INSERT INTO conversation_messages ("
            " id, tenant_id, source_id, channel, direction, author_id, author_username,"
            " author_display_name, text,"
            " source_message_id, source_event_id, metadata_json, occurred_at, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                message_id,
                tenant_id,
                source_id,
                channel,
                direction,
                author_id,
                author_username,
                author_display_name,
                text,
                source_message_id,
                source_event_id,
                json.dumps(metadata, ensure_ascii=False),
                occurred_at,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        existing = conn.execute(
            "SELECT * FROM conversation_messages"
            " WHERE tenant_id = ? AND source_id = ? AND direction = ? AND source_message_id = ?",
            (tenant_id, source_id, direction, source_message_id),
        ).fetchone()
        if existing is None:
            raise
        require_idempotent_replay(
            existing["channel"] == channel
            and existing["author_id"] == author_id
            and existing["author_username"] == author_username
            and existing["author_display_name"] == author_display_name
            and existing["text"] == text
            and existing["source_event_id"] == source_event_id
            and stored_json_matches(existing["metadata_json"], metadata)
            and existing["occurred_at"] == occurred_at,
            resource="conversation message",
            key=source_message_id,
            existing_id=existing["id"],
        )
        return str(existing["id"]), False
    return message_id, True


def get_message(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    tenant_id: str,
    source_id: str,
) -> ConversationMessage | None:
    _validate_scope(tenant_id, source_id)
    row = conn.execute(
        "SELECT * FROM conversation_messages WHERE id = ? AND tenant_id = ? AND source_id = ?",
        (message_id, tenant_id, source_id),
    ).fetchone()
    return row_to_message(row) if row is not None else None


def recent_messages(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    limit: int = 20,
    before_message_id: str | None = None,
) -> list[ConversationMessage]:
    _validate_scope(tenant_id, source_id)
    safe_limit = _bounded(limit, name="limit", maximum=100)
    before_filter = ""
    before_params: tuple[Any, ...] = ()
    if before_message_id is not None:
        cursor = conn.execute(
            "SELECT occurred_at, rowid FROM conversation_messages"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (before_message_id, tenant_id, source_id),
        ).fetchone()
        if cursor is None:
            raise ValueError("before_message_id does not belong to this conversation")
        before_filter = " AND (occurred_at < ? OR (occurred_at = ? AND rowid < ?))"
        before_params = (cursor["occurred_at"], cursor["occurred_at"], cursor["rowid"])
    rows = conn.execute(
        "SELECT * FROM conversation_messages"
        " WHERE tenant_id = ? AND source_id = ?"
        + before_filter
        + " ORDER BY occurred_at DESC, rowid DESC LIMIT ?",
        (tenant_id, source_id, *before_params, safe_limit),
    ).fetchall()
    return [row_to_message(row) for row in reversed(rows)]


def search_messages(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    query: str,
    limit: int = 10,
    context_before: int = 3,
    context_after: int = 3,
    since: int | None = None,
    until: int | None = None,
) -> list[ConversationSearchHit]:
    _validate_scope(tenant_id, source_id)
    if not isinstance(query, str):
        raise ValueError("query must be a string")
    if len(query) > MAX_SEARCH_QUERY_CHARS:
        raise ValueError(f"query must not exceed {MAX_SEARCH_QUERY_CHARS} characters")
    tokens = sorted(set(_TOKEN_RE.findall(query.casefold())))
    if not tokens:
        return []
    if len(tokens) > MAX_SEARCH_TOKENS:
        raise ValueError(f"query must not contain more than {MAX_SEARCH_TOKENS} unique tokens")
    safe_limit = _bounded(limit, name="limit", maximum=50)
    safe_before = _bounded(context_before, name="context_before", maximum=10, minimum=0)
    safe_after = _bounded(context_after, name="context_after", maximum=10, minimum=0)
    if since is not None and until is not None and since > until:
        raise ValueError("since must not be greater than until")
    filters = ["m.tenant_id = ?", "m.source_id = ?"]
    params: list[Any] = [tenant_id, source_id]
    if since is not None:
        filters.append("m.occurred_at >= ?")
        params.append(since)
    if until is not None:
        filters.append("m.occurred_at <= ?")
        params.append(until)
    match_query = " OR ".join(f'"{token}"*' for token in tokens)
    rows = conn.execute(
        "SELECT m.*, m.rowid AS history_rowid"
        " FROM conversation_messages_fts"
        " JOIN conversation_messages AS m ON m.rowid = conversation_messages_fts.rowid"
        " WHERE conversation_messages_fts MATCH ? AND "
        + " AND ".join(filters)
        + " ORDER BY bm25(conversation_messages_fts), m.occurred_at DESC, m.rowid DESC LIMIT ?",
        (match_query, *params, safe_limit),
    ).fetchall()
    return [
        ConversationSearchHit(
            match=row_to_message(row),
            context=_message_context(
                conn,
                row,
                tenant_id=tenant_id,
                source_id=source_id,
                before=safe_before,
                after=safe_after,
            ),
        )
        for row in rows
    ]


def prune_history_before(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    before: int,
    limit: int = 1000,
) -> int:
    _validate_scope(tenant_id, source_id)
    safe_limit = _bounded(limit, name="limit", maximum=10_000)
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id FROM conversation_messages"
            " WHERE tenant_id = ? AND source_id = ? AND occurred_at < ?"
            " ORDER BY occurred_at ASC, rowid ASC LIMIT ?",
            (tenant_id, source_id, before, safe_limit),
        ).fetchall()
        ids = [str(row["id"]) for row in rows]
        if ids:
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"DELETE FROM conversation_messages WHERE tenant_id = ? AND source_id = ?"
                f" AND id IN ({placeholders})",
                (tenant_id, source_id, *ids),
            )
        conn.execute("COMMIT")
        return len(ids)
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def row_to_message(row: sqlite3.Row) -> ConversationMessage:
    return ConversationMessage(
        id=str(row["id"]),
        tenant_id=str(row["tenant_id"]),
        source_id=str(row["source_id"]),
        channel=str(row["channel"]),
        direction=row["direction"],
        author_id=row["author_id"],
        author_username=row["author_username"],
        author_display_name=row["author_display_name"],
        text=str(row["text"]),
        source_message_id=str(row["source_message_id"]),
        source_event_id=row["source_event_id"],
        metadata=json.loads(row["metadata_json"]),
        occurred_at=int(row["occurred_at"]),
        created_at=int(row["created_at"]),
    )


def _message_context(
    conn: sqlite3.Connection,
    match: sqlite3.Row,
    *,
    tenant_id: str,
    source_id: str,
    before: int,
    after: int,
) -> tuple[ConversationMessage, ...]:
    before_rows = conn.execute(
        "SELECT * FROM conversation_messages"
        " WHERE tenant_id = ? AND source_id = ?"
        " AND (occurred_at < ? OR (occurred_at = ? AND rowid < ?))"
        " ORDER BY occurred_at DESC, rowid DESC LIMIT ?",
        (
            tenant_id,
            source_id,
            match["occurred_at"],
            match["occurred_at"],
            match["history_rowid"],
            before,
        ),
    ).fetchall()
    after_rows = conn.execute(
        "SELECT * FROM conversation_messages"
        " WHERE tenant_id = ? AND source_id = ?"
        " AND (occurred_at > ? OR (occurred_at = ? AND rowid > ?))"
        " ORDER BY occurred_at ASC, rowid ASC LIMIT ?",
        (
            tenant_id,
            source_id,
            match["occurred_at"],
            match["occurred_at"],
            match["history_rowid"],
            after,
        ),
    ).fetchall()
    context = [*reversed(before_rows), match, *after_rows]
    return tuple(row_to_message(row) for row in context)


def _validate_message(
    *,
    tenant_id: str,
    source_id: str,
    channel: str,
    direction: str,
    source_message_id: str,
    occurred_at: int,
) -> None:
    _validate_scope(tenant_id, source_id)
    if not channel.strip() or not source_message_id.strip():
        raise ValueError("channel and source_message_id must be non-empty")
    if direction not in {"inbound", "outbound"}:
        raise ValueError(f"unsupported message direction: {direction}")
    if isinstance(occurred_at, bool) or not isinstance(occurred_at, int) or occurred_at < 0:
        raise ValueError("occurred_at must be a non-negative integer")


def _validate_scope(tenant_id: str, source_id: str) -> None:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")


def _normalize_author_display_name(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("author_display_name must be a string when provided")
    normalized = value.strip()
    return normalized or None


def _normalize_author_username(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("author_username must be a string when provided")
    normalized = value.strip().lstrip("@").strip()
    return normalized or None


def _bounded(value: int, *, name: str, maximum: int, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value
