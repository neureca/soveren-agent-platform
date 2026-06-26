"""Durable per-session mailbox for prompts waiting on busy execution sessions."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any


def _now() -> int:
    return int(time.time())


def enqueue_prompt(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    prompt: str,
    action_id: str | None = None,
    source_event_id: str | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    """Append a prompt to a session mailbox.

    Returns `(mailbox_id, created)`. `action_id` is an optional idempotency key.
    """
    now = now if now is not None else _now()
    mailbox_id = "sm_" + uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        if action_id is not None:
            existing = conn.execute(
                "SELECT id FROM session_mailbox WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                return existing["id"], False

        session = conn.execute(
            "SELECT status FROM runtime_sessions WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (session_id, tenant_id, source_id),
        ).fetchone()
        if session is None:
            raise RuntimeError("runtime session not found")
        if session["status"] not in ("idle", "busy"):
            raise RuntimeError(f"runtime session status {session['status']!r} does not accept mailbox prompts")

        conn.execute(
            "INSERT INTO session_mailbox"
            " (id, session_id, tenant_id, source_id, source_event_id, action_id,"
            "  prompt, status, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
            (
                mailbox_id,
                session_id,
                tenant_id,
                source_id,
                source_event_id,
                action_id,
                prompt,
                now,
                now,
            ),
        )
        conn.execute("COMMIT")
        return mailbox_id, True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def has_pending(conn: sqlite3.Connection, session_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM session_mailbox WHERE session_id = ? AND status IN ('queued','sending') LIMIT 1",
        (session_id,),
    ).fetchone()
    return row is not None


def ready_session_ids(conn: sqlite3.Connection, *, tenant_id: str, limit: int) -> list[str]:
    rows = conn.execute(
        "SELECT m.session_id, MIN(m.created_at) AS oldest, MIN(m.rowid) AS oldest_row"
        " FROM session_mailbox m"
        " JOIN runtime_sessions s ON s.id = m.session_id"
        " WHERE m.tenant_id = ?"
        "   AND m.status = 'queued'"
        "   AND s.status = 'idle'"
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM session_mailbox blocked"
        "     WHERE blocked.session_id = m.session_id"
        "       AND blocked.status = 'sending'"
        "   )"
        " GROUP BY m.session_id"
        " ORDER BY oldest ASC, oldest_row ASC"
        " LIMIT ?",
        (tenant_id, limit),
    ).fetchall()
    return [row["session_id"] for row in rows]


def claim_next(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        session = conn.execute(
            "SELECT id FROM runtime_sessions WHERE id = ? AND status = 'idle'",
            (session_id,),
        ).fetchone()
        if session is None:
            conn.execute("COMMIT")
            return None
        item = conn.execute(
            "SELECT * FROM session_mailbox"
            " WHERE session_id = ? AND status = 'queued'"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM session_mailbox blocked"
            "     WHERE blocked.session_id = ?"
            "       AND blocked.status = 'sending'"
            "   )"
            " ORDER BY created_at ASC, rowid ASC"
            " LIMIT 1",
            (session_id, session_id),
        ).fetchone()
        if item is None:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE session_mailbox"
            " SET status = 'sending', updated_at = ?, last_error = NULL"
            " WHERE id = ? AND status = 'queued'",
            (now, item["id"]),
        )
        claimed = conn.execute(
            "SELECT * FROM session_mailbox WHERE id = ?",
            (item["id"],),
        ).fetchone()
        conn.execute("COMMIT")
        return claimed
    except Exception:
        conn.execute("ROLLBACK")
        raise


def mark_sent(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE session_mailbox"
        " SET status = 'sent', result_json = ?, updated_at = ?, sent_at = ?"
        " WHERE id = ? AND status = 'sending'",
        (json.dumps(result or {}, ensure_ascii=False), now, now, mailbox_id),
    )


def requeue(conn: sqlite3.Connection, mailbox_id: str, *, last_error: str) -> None:
    conn.execute(
        "UPDATE session_mailbox"
        " SET status = 'queued', last_error = ?, updated_at = ?"
        " WHERE id = ? AND status = 'sending'",
        (last_error[:500], _now(), mailbox_id),
    )


def mark_failed(conn: sqlite3.Connection, mailbox_id: str, *, last_error: str) -> None:
    conn.execute(
        "UPDATE session_mailbox SET status = 'failed', last_error = ?, updated_at = ? WHERE id = ?",
        (last_error[:500], _now(), mailbox_id),
    )


def fail_stale_sending(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    older_than_s: int,
    reason: str,
    limit: int,
) -> list[sqlite3.Row]:
    cutoff = _now() - older_than_s
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT * FROM session_mailbox"
            " WHERE tenant_id = ? AND status = 'sending' AND updated_at <= ?"
            " ORDER BY updated_at ASC, rowid ASC"
            " LIMIT ?",
            (tenant_id, cutoff, limit),
        ).fetchall()
        if rows:
            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                "UPDATE session_mailbox"
                " SET status = 'failed', last_error = ?, updated_at = ?"
                f" WHERE id IN ({placeholders}) AND status = 'sending'",
                (reason[:500], now, *ids),
            )
        conn.execute("COMMIT")
        return list(rows)
    except Exception:
        conn.execute("ROLLBACK")
        raise
