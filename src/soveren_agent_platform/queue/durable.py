"""Durable queue on top of the platform `event_queue` SQLite table."""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from typing import Any

log = logging.getLogger(__name__)


def _now() -> int:
    return int(time.time())


def enqueue(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    recipient: str,
    message_type: str,
    payload: dict[str, Any],
    idempotency_key: str,
    priority: int = 100,
    run_after: int | None = None,
    max_attempts: int = 5,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    now: int | None = None,
) -> str | None:
    """Insert one queued event. Return event id, or `None` on idempotency collision."""
    now = now if now is not None else _now()
    event_id = "evt_" + uuid.uuid4().hex
    run_after = run_after if run_after is not None else now
    try:
        conn.execute(
            "INSERT INTO event_queue ("
            "  id, tenant_id, recipient, message_type, payload_json, status,"
            "  priority, run_after, attempts, max_attempts, idempotency_key,"
            "  correlation_id, causation_id, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                tenant_id,
                recipient,
                message_type,
                json.dumps(payload, ensure_ascii=False),
                priority,
                run_after,
                max_attempts,
                idempotency_key,
                correlation_id,
                causation_id,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError as exc:
        if "idempotency_key" in str(exc):
            return None
        raise
    return event_id


def claim_due(
    conn: sqlite3.Connection,
    *,
    recipient: str,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    now: int | None = None,
) -> list[sqlite3.Row]:
    """Atomically lease due events for `recipient`, including expired leases."""
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        candidate_rows = conn.execute(
            "SELECT id FROM event_queue"
            " WHERE recipient = ?"
            "   AND run_after <= ?"
            "   AND (status = 'queued'"
            "        OR status = 'retrying'"
            "        OR (status = 'leased' AND lease_until <= ?))"
            " ORDER BY priority ASC, created_at ASC"
            " LIMIT ?",
            (recipient, now, now, limit),
        ).fetchall()
        ids = [r["id"] for r in candidate_rows]
        if not ids:
            conn.execute("COMMIT")
            return []
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            "UPDATE event_queue SET"
            "  status = 'leased',"
            "  lease_owner = ?,"
            "  lease_until = ?,"
            "  attempts = attempts + 1,"
            "  updated_at = ?"
            f" WHERE id IN ({placeholders})",
            (lease_owner, now + lease_seconds, now, *ids),
        )
        claimed = conn.execute(
            f"SELECT * FROM event_queue WHERE id IN ({placeholders})"
            " ORDER BY priority ASC, created_at ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return list(claimed)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def mark_done(conn: sqlite3.Connection, event_id: str, *, now: int | None = None) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE event_queue SET"
        "  status = 'done',"
        "  lease_owner = NULL,"
        "  lease_until = NULL,"
        "  updated_at = ?"
        " WHERE id = ?",
        (now, event_id),
    )


def mark_retry(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    run_after: int,
    last_error: str,
    now: int | None = None,
) -> None:
    """Route an event to `retrying` or `dead_letter` by attempts/max_attempts."""
    now = now if now is not None else _now()
    row = conn.execute(
        "SELECT attempts, max_attempts FROM event_queue WHERE id = ?",
        (event_id,),
    ).fetchone()
    if row is None:
        return
    new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "retrying"
    conn.execute(
        "UPDATE event_queue SET"
        "  status = ?,"
        "  run_after = ?,"
        "  last_error = ?,"
        "  lease_owner = NULL,"
        "  lease_until = NULL,"
        "  updated_at = ?"
        " WHERE id = ?",
        (new_status, run_after, last_error, now, event_id),
    )
    if new_status == "dead_letter":
        log.error(
            "event %s moved to dead_letter after %d attempts: %s",
            event_id,
            row["attempts"],
            last_error,
        )

