"""SQLite store for outbound messages."""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from typing import Any

from agent_platform.outbound.contracts import OutboundMessage

log = logging.getLogger(__name__)


def _now() -> int:
    return int(time.time())


def enqueue_outbound(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    channel: str,
    destination_id: str,
    text: str,
    idempotency_key: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    run_after: int | None = None,
    max_attempts: int = 5,
    correlation_id: str | None = None,
    now: int | None = None,
) -> str | None:
    now = now if now is not None else _now()
    run_after = run_after if run_after is not None else now
    message_id = "out_" + uuid.uuid4().hex
    try:
        conn.execute(
            "INSERT INTO outbound_messages"
            " (id, tenant_id, channel, destination_id, text, payload_json, status,"
            "  priority, run_after, attempts, max_attempts, idempotency_key,"
            "  correlation_id, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?)",
            (
                message_id,
                tenant_id,
                channel,
                destination_id,
                text,
                json.dumps(payload or {}, ensure_ascii=False),
                priority,
                run_after,
                max_attempts,
                idempotency_key,
                correlation_id,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError as exc:
        if "idempotency_key" in str(exc):
            return None
        raise
    return message_id


def claim_due(
    conn: sqlite3.Connection,
    *,
    channel: str,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    now: int | None = None,
) -> list[sqlite3.Row]:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id FROM outbound_messages"
            " WHERE channel = ?"
            "   AND run_after <= ?"
            "   AND (status = 'queued'"
            "        OR status = 'retrying'"
            "        OR (status = 'leased' AND lease_until <= ?))"
            " ORDER BY priority ASC, created_at ASC"
            " LIMIT ?",
            (channel, now, now, limit),
        ).fetchall()
        ids = [row["id"] for row in rows]
        if not ids:
            conn.execute("COMMIT")
            return []
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'leased', lease_owner = ?, lease_until = ?,"
            " attempts = attempts + 1, updated_at = ?"
            f" WHERE id IN ({placeholders})",
            (lease_owner, now + lease_seconds, now, *ids),
        )
        claimed = conn.execute(
            f"SELECT * FROM outbound_messages WHERE id IN ({placeholders})"
            " ORDER BY priority ASC, created_at ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return list(claimed)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def mark_sent(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE outbound_messages SET"
        " status = 'sent', payload_json = ?, lease_owner = NULL, lease_until = NULL,"
        " sent_at = ?, updated_at = ?"
        " WHERE id = ?",
        (json.dumps(result or {}, ensure_ascii=False), now, now, message_id),
    )


def mark_retry(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    run_after: int,
    last_error: str,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    row = conn.execute(
        "SELECT attempts, max_attempts FROM outbound_messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    if row is None:
        return
    new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "retrying"
    conn.execute(
        "UPDATE outbound_messages SET"
        " status = ?, run_after = ?, last_error = ?, lease_owner = NULL,"
        " lease_until = NULL, updated_at = ?"
        " WHERE id = ?",
        (new_status, run_after, last_error, now, message_id),
    )
    if new_status == "dead_letter":
        log.error("outbound message %s dead_letter: %s", message_id, last_error)


def row_to_message(row: sqlite3.Row) -> OutboundMessage:
    return OutboundMessage(
        id=row["id"],
        tenant_id=row["tenant_id"],
        channel=row["channel"],
        destination_id=row["destination_id"],
        text=row["text"],
        payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
        correlation_id=row["correlation_id"],
    )

