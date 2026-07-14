"""Durable queue on top of the platform `event_queue` SQLite table."""

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
    requested_run_after = run_after
    fingerprint = idempotency_fingerprint(
        {
            "recipient": recipient,
            "message_type": message_type,
            "payload": payload,
            "priority": priority,
            "run_after": requested_run_after,
            "max_attempts": max_attempts,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
        }
    )
    run_after = run_after if run_after is not None else now
    try:
        conn.execute(
            "INSERT INTO event_queue ("
            "  id, tenant_id, recipient, message_type, payload_json, status,"
            "  priority, run_after, attempts, max_attempts, idempotency_key,"
            "  idempotency_fingerprint, correlation_id, causation_id, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
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
                fingerprint,
                correlation_id,
                causation_id,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        existing = conn.execute(
            "SELECT * FROM event_queue WHERE tenant_id = ? AND idempotency_key = ?",
            (tenant_id, idempotency_key),
        ).fetchone()
        if existing is None:
            raise
        stored_fingerprint = existing["idempotency_fingerprint"]
        # Legacy rows cannot recover the original schedule after a retry mutates run_after.
        matches = (
            stored_fingerprint == fingerprint
            if stored_fingerprint is not None
            else existing["recipient"] == recipient
            and existing["message_type"] == message_type
            and stored_json_matches(existing["payload_json"], payload)
            and existing["priority"] == priority
            and existing["max_attempts"] == max_attempts
            and existing["correlation_id"] == correlation_id
            and existing["causation_id"] == causation_id
        )
        require_idempotent_replay(
            matches,
            resource="event",
            key=idempotency_key,
            existing_id=existing["id"],
        )
        return None
    return event_id


def claim_due(
    conn: sqlite3.Connection,
    *,
    recipient: str,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    recover_exhausted: bool = False,
    now: int | None = None,
) -> list[sqlite3.Row]:
    """Atomically lease due events, optionally including exhausted recovery work."""
    now = now if now is not None else _now()
    lease_token = uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE event_queue SET"
            "  status = 'dead_letter',"
            "  last_error = 'event lease expired after the maximum number of attempts',"
            "  lease_owner = NULL,"
            "  lease_until = NULL,"
            "  lease_token = NULL,"
            "  updated_at = ?"
            " WHERE recipient = ? AND status = 'leased' AND lease_until <= ?"
            "   AND attempts >= max_attempts AND ? = 0",
            (now, recipient, now, recover_exhausted),
        )
        candidate_rows = conn.execute(
            "SELECT id FROM event_queue"
            " WHERE recipient = ?"
            "   AND run_after <= ?"
            "   AND (status = 'queued'"
            "        OR status = 'retrying'"
            "        OR (status = 'leased' AND lease_until <= ?"
            "            AND (attempts < max_attempts OR ? = 1)))"
            " ORDER BY priority ASC, created_at ASC, rowid ASC"
            " LIMIT ?",
            (recipient, now, now, recover_exhausted, limit),
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
            "  lease_token = ?,"
            "  attempts = attempts + 1,"
            "  updated_at = ?"
            f" WHERE id IN ({placeholders})",
            (lease_owner, now + lease_seconds, lease_token, now, *ids),
        )
        claimed = conn.execute(
            f"SELECT * FROM event_queue WHERE id IN ({placeholders}) ORDER BY priority ASC, created_at ASC, rowid ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return list(claimed)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def mark_done(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    lease_token: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    cur = conn.execute(
        "UPDATE event_queue SET"
        "  status = 'done',"
        "  lease_owner = NULL,"
        "  lease_until = NULL,"
        "  lease_token = NULL,"
        "  updated_at = ?"
        " WHERE id = ? AND status = 'leased' AND lease_token = ?",
        (now, event_id, lease_token),
    )
    return cur.rowcount == 1


def renew_lease(
    conn: sqlite3.Connection,
    event_id: str,
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
            "UPDATE event_queue SET lease_until = ?, updated_at = ?"
            " WHERE id = ? AND status = 'leased' AND lease_token = ?"
            "   AND lease_until > ?",
            (now + lease_seconds, now, event_id, lease_token, now),
        ).rowcount
    )


def mark_retry(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    lease_token: str,
    run_after: int,
    last_error: str,
    now: int | None = None,
) -> str | None:
    """Route an event to `retrying` or `dead_letter` by attempts/max_attempts."""
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT attempts, max_attempts FROM event_queue WHERE id = ? AND status = 'leased' AND lease_token = ?",
            (event_id, lease_token),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "retrying"
        updated = conn.execute(
            "UPDATE event_queue SET"
            "  status = ?,"
            "  run_after = ?,"
            "  last_error = ?,"
            "  lease_owner = NULL,"
            "  lease_until = NULL,"
            "  lease_token = NULL,"
            "  updated_at = ?"
            " WHERE id = ? AND status = 'leased' AND lease_token = ?",
            (new_status, run_after, last_error, now, event_id, lease_token),
        ).rowcount
        if updated != 1:
            conn.execute("COMMIT")
            return None
        conn.execute("COMMIT")
        if new_status == "dead_letter":
            log.error(
                "event %s moved to dead_letter after %d attempts: %s",
                event_id,
                row["attempts"],
                last_error,
            )
        return new_status
    except Exception:
        conn.execute("ROLLBACK")
        raise
