"""SQLite store for outbound messages."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.idempotency import (
    idempotency_fingerprint,
    require_idempotent_replay,
    stored_json_matches,
)
from soveren_agent_platform.outbound.contracts import OutboundMessage

log = logging.getLogger(__name__)


def _now() -> int:
    return int(time.time())


def enqueue_outbound(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
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
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    now = now if now is not None else _now()
    requested_run_after = run_after
    fingerprint = idempotency_fingerprint(
        {
            "channel": channel,
            "destination_id": destination_id,
            "text": text,
            "payload": payload or {},
            "priority": priority,
            "run_after": requested_run_after,
            "max_attempts": max_attempts,
            "correlation_id": correlation_id,
        }
    )
    run_after = run_after if run_after is not None else now
    message_id = "out_" + uuid.uuid4().hex
    try:
        conn.execute(
            "INSERT INTO outbound_messages"
            " (id, tenant_id, source_id, channel, destination_id, text, payload_json, status,"
            "  priority, run_after, attempts, max_attempts, idempotency_key,"
            "  idempotency_fingerprint, correlation_id, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?)",
            (
                message_id,
                tenant_id,
                source_id,
                channel,
                destination_id,
                text,
                json.dumps(payload or {}, ensure_ascii=False),
                priority,
                run_after,
                max_attempts,
                idempotency_key,
                fingerprint,
                correlation_id,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        existing = conn.execute(
            "SELECT * FROM outbound_messages"
            " WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is None:
            raise
        stored_fingerprint = existing["idempotency_fingerprint"]
        matches = (
            stored_fingerprint == fingerprint
            if stored_fingerprint is not None
            else existing["channel"] == channel
            and existing["destination_id"] == destination_id
            and existing["text"] == text
            and stored_json_matches(existing["payload_json"], payload or {})
            and existing["priority"] == priority
            and existing["max_attempts"] == max_attempts
            and existing["correlation_id"] == correlation_id
            and (requested_run_after is None or existing["run_after"] == requested_run_after)
        )
        require_idempotent_replay(
            matches,
            resource="outbound message",
            key=idempotency_key,
            existing_id=existing["id"],
        )
        return None
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
    lease_token = uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'uncertain',"
            " last_error = 'outbound send outcome is uncertain after lease expiry',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE status = 'sending' AND lease_until <= ?",
            (now, now),
        )
        rows = conn.execute(
            "SELECT id FROM outbound_messages"
            " WHERE channel = ?"
            "   AND run_after <= ?"
            "   AND (status = 'queued'"
            "        OR status = 'retrying'"
            "        OR (status = 'leased' AND lease_until <= ?))"
            " ORDER BY priority ASC, created_at ASC, rowid ASC"
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
            " status = 'leased', lease_owner = ?, lease_until = ?, lease_token = ?,"
            " attempts = attempts + 1, updated_at = ?"
            f" WHERE id IN ({placeholders})",
            (lease_owner, now + lease_seconds, lease_token, now, *ids),
        )
        claimed = conn.execute(
            f"SELECT * FROM outbound_messages WHERE id IN ({placeholders})"
            " ORDER BY priority ASC, created_at ASC, rowid ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return list(claimed)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def renew_lease(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    lease_seconds: int,
    now: int | None = None,
) -> bool:
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    now = now if now is not None else _now()
    return bool(
        conn.execute(
            "UPDATE outbound_messages SET lease_until = ?, updated_at = ?"
            " WHERE id = ? AND status IN ('leased','sending') AND lease_token = ?"
            "   AND lease_until > ?",
            (now + lease_seconds, now, message_id, lease_token, now),
        ).rowcount
    )


def mark_sending(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    return bool(
        conn.execute(
            "UPDATE outbound_messages SET status = 'sending', updated_at = ?"
            " WHERE id = ? AND status = 'leased' AND lease_token = ? AND lease_until > ?",
            (now, message_id, lease_token, now),
        ).rowcount
    )


def mark_sent(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE outbound_messages SET"
        " status = 'sent', result_json = ?, lease_owner = NULL, lease_until = NULL,"
        " lease_token = NULL, sent_at = ?, updated_at = ?"
        " WHERE id = ? AND status = 'sending' AND lease_token = ?",
        (json.dumps(result or {}, ensure_ascii=False), now, now, message_id, lease_token),
    )
    return cur.rowcount == 1


def mark_uncertain(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    last_error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    return bool(
        conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'uncertain', last_error = ?, lease_owner = NULL,"
            " lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status = 'sending' AND lease_token = ?",
            (last_error, now, message_id, lease_token),
        ).rowcount
    )


def mark_retry(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    run_after: int,
    last_error: str,
    now: int | None = None,
) -> str | None:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT attempts, max_attempts FROM outbound_messages"
            " WHERE id = ? AND status IN ('leased','sending') AND lease_token = ?",
            (message_id, lease_token),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "retrying"
        updated = conn.execute(
            "UPDATE outbound_messages SET"
            " status = ?, run_after = ?, last_error = ?, lease_owner = NULL,"
            " lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status IN ('leased','sending') AND lease_token = ?",
            (new_status, run_after, last_error, now, message_id, lease_token),
        ).rowcount
        if updated != 1:
            conn.execute("COMMIT")
            return None
        conn.execute("COMMIT")
        if new_status == "dead_letter":
            log.error("outbound message %s dead_letter: %s", message_id, last_error)
        return new_status
    except Exception:
        conn.execute("ROLLBACK")
        raise


def row_to_message(row: sqlite3.Row) -> OutboundMessage:
    return OutboundMessage(
        id=row["id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        channel=row["channel"],
        destination_id=row["destination_id"],
        text=row["text"],
        lease_token=row["lease_token"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
        correlation_id=row["correlation_id"],
    )
