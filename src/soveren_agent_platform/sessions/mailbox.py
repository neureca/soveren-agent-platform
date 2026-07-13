"""Durable per-session mailbox for prompts waiting on busy execution sessions."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.idempotency import require_idempotent_replay


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
    idempotency_key: str | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    """Append a prompt to a session mailbox.

    Returns `(mailbox_id, created)`. Keys are scoped to the conversation.
    """
    now = now if now is not None else _now()
    mailbox_id = "sm_" + uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing_rows: list[sqlite3.Row] = []
        if action_id is not None:
            existing = conn.execute(
                "SELECT * FROM session_mailbox"
                " WHERE tenant_id = ? AND source_id = ? AND action_id = ?",
                (tenant_id, source_id, action_id),
            ).fetchone()
            if existing is not None:
                existing_rows.append(existing)
        if idempotency_key is not None:
            existing = conn.execute(
                "SELECT * FROM session_mailbox"
                " WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
                (tenant_id, source_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                existing_rows.append(existing)
        if existing_rows:
            existing = existing_rows[0]
            require_idempotent_replay(
                all(row["id"] == existing["id"] for row in existing_rows)
                and existing["session_id"] == session_id
                and existing["prompt"] == prompt
                and existing["action_id"] == action_id
                and existing["source_event_id"] == source_event_id,
                resource="session mailbox item",
                key=idempotency_key or f"action:{action_id}",
                existing_id=existing["id"],
            )
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
            "  prompt, status, idempotency_key, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)",
            (
                mailbox_id,
                session_id,
                tenant_id,
                source_id,
                source_event_id,
                action_id,
                prompt,
                idempotency_key,
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


def ready_sessions(conn: sqlite3.Connection, *, tenant_id: str, limit: int) -> list[sqlite3.Row]:
    now = _now()
    rows = conn.execute(
        "SELECT m.session_id, m.source_id, MIN(m.created_at) AS oldest, MIN(m.rowid) AS oldest_row,"
        " MIN(CASE WHEN m.status = 'sending' AND m.accepted_at IS NOT NULL THEN 0 ELSE 1 END) AS priority"
        " FROM session_mailbox m"
        " JOIN runtime_sessions s"
        "   ON s.id = m.session_id AND s.tenant_id = m.tenant_id AND s.source_id = m.source_id"
        " WHERE m.tenant_id = ?"
        "   AND ("
        "     (m.status = 'sending' AND m.accepted_at IS NOT NULL AND m.run_after <= ?)"
        "     OR ("
        "       m.status = 'queued' AND m.run_after <= ? AND s.status = 'idle'"
        "       AND NOT EXISTS ("
        "         SELECT 1 FROM session_mailbox blocked"
        "         WHERE blocked.session_id = m.session_id"
        "           AND blocked.tenant_id = m.tenant_id AND blocked.source_id = m.source_id"
        "           AND blocked.status = 'sending'"
        "       )"
        "     )"
        "   )"
        " GROUP BY m.session_id, m.source_id"
        " ORDER BY priority ASC, oldest ASC, oldest_row ASC"
        " LIMIT ?",
        (tenant_id, now, now, limit),
    ).fetchall()
    return list(rows)


def claim_next(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    tenant_id: str,
    source_id: str,
) -> sqlite3.Row | None:
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        accepted = conn.execute(
            "SELECT * FROM session_mailbox"
            " WHERE session_id = ? AND tenant_id = ? AND source_id = ?"
            "   AND status = 'sending' AND accepted_at IS NOT NULL"
            "   AND run_after <= ?"
            " ORDER BY accepted_at ASC, rowid ASC LIMIT 1",
            (session_id, tenant_id, source_id, now),
        ).fetchone()
        if accepted is not None:
            conn.execute("COMMIT")
            return accepted

        session = conn.execute(
            "SELECT id FROM runtime_sessions"
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'idle'",
            (session_id, tenant_id, source_id),
        ).fetchone()
        if session is None:
            conn.execute("COMMIT")
            return None
        item = conn.execute(
            "SELECT * FROM session_mailbox"
            " WHERE session_id = ? AND tenant_id = ? AND source_id = ? AND status = 'queued'"
            "   AND run_after <= ?"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM session_mailbox blocked"
            "     WHERE blocked.session_id = ? AND blocked.tenant_id = ? AND blocked.source_id = ?"
            "       AND blocked.status = 'sending'"
            "   )"
            " ORDER BY created_at ASC, rowid ASC"
            " LIMIT 1",
            (session_id, tenant_id, source_id, now, session_id, tenant_id, source_id),
        ).fetchone()
        if item is None:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE session_mailbox"
            " SET status = 'sending', attempts = attempts + 1, updated_at = ?, last_error = NULL"
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'queued'",
            (now, item["id"], tenant_id, source_id),
        )
        claimed = conn.execute(
            "SELECT * FROM session_mailbox WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (item["id"], tenant_id, source_id),
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
    tenant_id: str,
    source_id: str,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE session_mailbox"
        " SET status = 'sent', result_json = ?, updated_at = ?, sent_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ?"
        "   AND status = 'sending' AND accepted_at IS NOT NULL",
        (json.dumps(result or {}, ensure_ascii=False), now, now, mailbox_id, tenant_id, source_id),
    )


def mark_accepted(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    tenant_id: str,
    source_id: str,
    backend_receipt: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute(
        "UPDATE session_mailbox"
        " SET accepted_at = ?, backend_receipt_json = ?, run_after = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ?"
        "   AND status = 'sending' AND accepted_at IS NULL",
        (now, json.dumps(backend_receipt or {}, ensure_ascii=False), now, now, mailbox_id, tenant_id, source_id),
    )


def defer_accepted(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    current_action_id: str | None,
    last_error: str,
    retry_after_s: int,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT attempts, max_attempts FROM session_mailbox"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?"
            "   AND status = 'sending' AND accepted_at IS NOT NULL",
            (mailbox_id, tenant_id, source_id),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return True
        attempts = int(row["attempts"]) + 1
        terminal = attempts >= int(row["max_attempts"])
        if terminal:
            conn.execute(
                "UPDATE session_mailbox"
                " SET status = 'failed', attempts = ?, last_error = ?, updated_at = ?"
                " WHERE id = ? AND tenant_id = ? AND source_id = ?"
                "   AND status = 'sending' AND accepted_at IS NOT NULL",
                (attempts, last_error[:500], now, mailbox_id, tenant_id, source_id),
            )
        else:
            conn.execute(
                "UPDATE session_mailbox"
                " SET attempts = ?, run_after = ?, last_error = ?, updated_at = ?"
                " WHERE id = ? AND tenant_id = ? AND source_id = ?"
                "   AND status = 'sending' AND accepted_at IS NOT NULL",
                (attempts, now + max(1, retry_after_s), last_error[:500], now, mailbox_id, tenant_id, source_id),
            )
        session_updated = conn.execute(
            "UPDATE runtime_sessions"
            " SET status = ?, current_action_id = ?, last_error = ?, updated_at = ?, last_used_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (
                "failed" if terminal else "busy",
                None if terminal else current_action_id,
                last_error[:500],
                now,
                now,
                session_id,
                tenant_id,
                source_id,
            ),
        ).rowcount
        if session_updated != 1:
            raise RuntimeError("runtime session disappeared while deferring accepted delivery")
        conn.execute("COMMIT")
        return terminal
    except Exception:
        conn.execute("ROLLBACK")
        raise


def defer_pending(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    current_action_id: str | None,
    last_error: str,
    retry_after_s: int,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        updated = conn.execute(
            "UPDATE session_mailbox"
            " SET run_after = ?, last_error = ?, updated_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?"
            "   AND status = 'sending' AND accepted_at IS NOT NULL",
            (now + max(1, retry_after_s), last_error[:500], now, mailbox_id, tenant_id, source_id),
        ).rowcount
        if updated != 1:
            raise RuntimeError("accepted mailbox delivery is no longer pending")
        session_updated = conn.execute(
            "UPDATE runtime_sessions"
            " SET status = 'busy', current_action_id = ?, last_error = ?, updated_at = ?, last_used_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (current_action_id, last_error[:500], now, now, session_id, tenant_id, source_id),
        ).rowcount
        if session_updated != 1:
            raise RuntimeError("runtime session disappeared while deferring pending delivery")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def complete_delivery(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    result: dict[str, Any],
    session_status: str,
    current_action_id: str | None = None,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        updated = conn.execute(
            "UPDATE session_mailbox"
            " SET status = 'sent', result_json = ?, updated_at = ?, sent_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?"
            "   AND status = 'sending' AND accepted_at IS NOT NULL",
            (json.dumps(result, ensure_ascii=False), now, now, mailbox_id, tenant_id, source_id),
        ).rowcount
        if updated != 1:
            raise RuntimeError("accepted mailbox delivery is no longer completable")
        session_updated = conn.execute(
            "UPDATE runtime_sessions"
            " SET status = ?, current_action_id = ?, last_error = NULL, updated_at = ?, last_used_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (session_status, current_action_id, now, now, session_id, tenant_id, source_id),
        ).rowcount
        if session_updated != 1:
            raise RuntimeError("runtime session disappeared while completing delivery")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def fail_delivery(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    session_id: str,
    tenant_id: str,
    source_id: str,
    last_error: str,
    now: int | None = None,
) -> None:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        mailbox_updated = conn.execute(
            "UPDATE session_mailbox SET status = 'failed', last_error = ?, updated_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'sending'",
            (last_error[:500], now, mailbox_id, tenant_id, source_id),
        ).rowcount
        if mailbox_updated != 1:
            raise RuntimeError("mailbox delivery is no longer fail-able")
        session_updated = conn.execute(
            "UPDATE runtime_sessions"
            " SET status = 'failed', current_action_id = NULL, last_error = ?, updated_at = ?, last_used_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (last_error[:500], now, now, session_id, tenant_id, source_id),
        ).rowcount
        if session_updated != 1:
            raise RuntimeError("runtime session disappeared while failing delivery")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def requeue(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    tenant_id: str,
    source_id: str,
    last_error: str,
) -> None:
    conn.execute(
        "UPDATE session_mailbox"
        " SET status = 'queued', last_error = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ?"
        "   AND status = 'sending' AND accepted_at IS NULL",
        (last_error[:500], _now(), mailbox_id, tenant_id, source_id),
    )


def mark_failed(
    conn: sqlite3.Connection,
    mailbox_id: str,
    *,
    tenant_id: str,
    source_id: str,
    last_error: str,
) -> None:
    conn.execute(
        "UPDATE session_mailbox SET status = 'failed', last_error = ?, updated_at = ?"
        " WHERE id = ? AND tenant_id = ? AND source_id = ?",
        (last_error[:500], _now(), mailbox_id, tenant_id, source_id),
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
            "SELECT m.* FROM session_mailbox m"
            " JOIN runtime_sessions s"
            "   ON s.id = m.session_id AND s.tenant_id = m.tenant_id AND s.source_id = m.source_id"
            " WHERE m.tenant_id = ? AND m.status = 'sending'"
            "   AND m.accepted_at IS NULL AND m.updated_at <= ?"
            " ORDER BY m.updated_at ASC, m.rowid ASC"
            " LIMIT ?",
            (tenant_id, cutoff, limit),
        ).fetchall()
        if rows:
            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                "UPDATE session_mailbox"
                " SET status = 'failed', last_error = ?, result_json = ?, updated_at = ?"
                f" WHERE tenant_id = ? AND id IN ({placeholders})"
                "   AND status = 'sending' AND accepted_at IS NULL",
                (
                    reason[:500],
                    json.dumps({"delivery": "uncertain"}),
                    now,
                    tenant_id,
                    *ids,
                ),
            )
            session_ids = sorted({str(row["session_id"]) for row in rows})
            session_placeholders = ",".join("?" * len(session_ids))
            conn.execute(
                "UPDATE runtime_sessions"
                " SET status = 'failed', current_action_id = NULL, last_error = ?,"
                " updated_at = ?, last_used_at = ?"
                f" WHERE tenant_id = ? AND id IN ({session_placeholders}) AND status IN ('idle','busy')",
                (reason[:500], now, now, tenant_id, *session_ids),
            )
        conn.execute("COMMIT")
        return list(rows)
    except Exception:
        conn.execute("ROLLBACK")
        raise
