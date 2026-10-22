"""SQLite store for outbound messages."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from collections.abc import Sequence
from typing import Any

from soveren_agent_platform.conversation_history.store import record_message
from soveren_agent_platform.dialogue.contracts import DialogueConflictError
from soveren_agent_platform.idempotency import (
    idempotency_fingerprint,
    require_idempotent_replay,
    stored_json_matches,
)
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.outbound.contracts import (
    OutboundEnqueueResult,
    OutboundMessage,
    OutboundRequest,
    OutboundTaskReference,
)

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
    ordering_key: str | None = None,
    ordering_position: int | None = None,
    task_reference: OutboundTaskReference | None = None,
    reply_to_message_id: int | None = None,
    message_thread_id: int | None = None,
    now: int | None = None,
) -> str | None:
    result = enqueue_outbound_with_result(
        conn,
        tenant_id=tenant_id,
        source_id=source_id,
        channel=channel,
        destination_id=destination_id,
        text=text,
        idempotency_key=idempotency_key,
        payload=payload,
        priority=priority,
        run_after=run_after,
        max_attempts=max_attempts,
        correlation_id=correlation_id,
        ordering_key=ordering_key,
        ordering_position=ordering_position,
        task_reference=task_reference,
        reply_to_message_id=reply_to_message_id,
        message_thread_id=message_thread_id,
        now=now,
    )
    return result.message_id if result.created else None


def enqueue_outbound_with_result(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    channel: str,
    destination_id: str,
    text: str,
    idempotency_key: str,
    payload: JsonObject | None = None,
    priority: int = 100,
    run_after: int | None = None,
    max_attempts: int = 5,
    correlation_id: str | None = None,
    ordering_key: str | None = None,
    ordering_position: int | None = None,
    task_reference: OutboundTaskReference | None = None,
    reply_to_message_id: int | None = None,
    message_thread_id: int | None = None,
    now: int | None = None,
) -> OutboundEnqueueResult:
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        result = _enqueue_outbound_with_result_locked(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            channel=channel,
            destination_id=destination_id,
            text=text,
            idempotency_key=idempotency_key,
            payload=payload,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            ordering_key=ordering_key,
            ordering_position=ordering_position,
            task_reference=task_reference,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
            now=now,
        )
        if owns_transaction:
            conn.execute("COMMIT")
        return result
    except BaseException:
        if owns_transaction:
            conn.execute("ROLLBACK")
        raise


def _enqueue_outbound_with_result_locked(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    channel: str,
    destination_id: str,
    text: str,
    idempotency_key: str,
    payload: JsonObject | None = None,
    priority: int = 100,
    run_after: int | None = None,
    max_attempts: int = 5,
    correlation_id: str | None = None,
    ordering_key: str | None = None,
    ordering_position: int | None = None,
    task_reference: OutboundTaskReference | None = None,
    reply_to_message_id: int | None = None,
    message_thread_id: int | None = None,
    now: int | None = None,
) -> OutboundEnqueueResult:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    if (ordering_key is None) != (ordering_position is None):
        raise ValueError("ordering_key and ordering_position must be provided together")
    if ordering_key is not None and (not ordering_key.strip() or len(ordering_key) > 256):
        raise ValueError("ordering_key must be non-empty and at most 256 characters")
    if ordering_position is not None and (
        isinstance(ordering_position, bool) or not isinstance(ordering_position, int) or ordering_position < 1
    ):
        raise ValueError("ordering_position must be a positive integer")
    _validate_telegram_reply_fields(reply_to_message_id, message_thread_id)
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
            "ordering_key": ordering_key,
            "ordering_position": ordering_position,
            "task_id": task_reference.task_id if task_reference is not None else None,
            "task_revision": task_reference.task_revision if task_reference is not None else None,
            "routing_question_id": task_reference.routing_question_id if task_reference is not None else None,
            "reply_to_message_id": reply_to_message_id,
            "message_thread_id": message_thread_id,
        }
    )
    run_after = run_after if run_after is not None else now
    message_id = "out_" + uuid.uuid4().hex
    existing = conn.execute(
        "SELECT * FROM outbound_messages WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
        (tenant_id, source_id, idempotency_key),
    ).fetchone()
    if existing is not None:
        _require_outbound_replay(
            existing,
            channel=channel,
            destination_id=destination_id,
            text=text,
            payload=payload,
            priority=priority,
            requested_run_after=requested_run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            ordering_key=ordering_key,
            ordering_position=ordering_position,
            task_reference=task_reference,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
            idempotency_key=idempotency_key,
        )
        return OutboundEnqueueResult(message_id=str(existing["id"]), created=False)
    if task_reference is not None:
        _validate_task_reference(conn, tenant_id, source_id, task_reference)
    try:
        conn.execute(
            "INSERT INTO outbound_messages"
            " (id, tenant_id, source_id, channel, destination_id, text, payload_json, status,"
            "  priority, run_after, attempts, max_attempts, idempotency_key,"
            "  idempotency_fingerprint, correlation_id, ordering_key, ordering_position,"
            "  task_id, task_revision, routing_question_id, reply_to_message_id, message_thread_id,"
            "  created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                ordering_key,
                ordering_position,
                task_reference.task_id if task_reference is not None else None,
                task_reference.task_revision if task_reference is not None else None,
                task_reference.routing_question_id if task_reference is not None else None,
                reply_to_message_id,
                message_thread_id,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        existing = conn.execute(
            "SELECT * FROM outbound_messages WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is None:
            raise
        _require_outbound_replay(
            existing,
            channel=channel,
            destination_id=destination_id,
            text=text,
            payload=payload,
            priority=priority,
            requested_run_after=requested_run_after,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
            ordering_key=ordering_key,
            ordering_position=ordering_position,
            task_reference=task_reference,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
            idempotency_key=idempotency_key,
        )
        return OutboundEnqueueResult(message_id=str(existing["id"]), created=False)
    return OutboundEnqueueResult(message_id=message_id, created=True)


def enqueue_outbound_many(
    conn: sqlite3.Connection,
    requests: Sequence[OutboundRequest],
    *,
    now: int | None = None,
) -> tuple[str | None, ...]:
    if not requests:
        return ()
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        message_ids = tuple(
            enqueue_outbound(
                conn,
                tenant_id=request.tenant_id,
                source_id=request.source_id,
                channel=request.channel,
                destination_id=request.destination_id,
                text=request.text,
                idempotency_key=request.idempotency_key,
                payload=request.payload,
                priority=request.priority,
                run_after=request.run_after,
                max_attempts=request.max_attempts,
                correlation_id=request.correlation_id,
                ordering_key=request.ordering_key,
                ordering_position=request.ordering_position,
                task_reference=request.task_reference,
                reply_to_message_id=request.reply_to_message_id,
                message_thread_id=request.message_thread_id,
                now=now,
            )
            for request in requests
        )
        conn.execute("COMMIT")
        return message_ids
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def claim_due(
    conn: sqlite3.Connection,
    *,
    channel: str,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    tenant_id: str | None = None,
    now: int | None = None,
) -> list[sqlite3.Row]:
    if limit < 1:
        raise ValueError("limit must be positive")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    if tenant_id is not None and not tenant_id.strip():
        raise ValueError("tenant_id must be non-empty when provided")
    now = now if now is not None else _now()
    lease_token = uuid.uuid4().hex
    tenant_clause = " AND tenant_id = ?" if tenant_id is not None else ""
    tenant_params = (tenant_id,) if tenant_id is not None else ()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'uncertain',"
            " last_error = 'outbound send outcome is uncertain after lease expiry',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE channel = ? AND status = 'sending' AND lease_until <= ?" + tenant_clause,
            (now, channel, now, *tenant_params),
        )
        conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'dead_letter',"
            " last_error = 'outbound lease expired after the maximum number of attempts',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE channel = ? AND status = 'leased' AND lease_until <= ?"
            " AND attempts >= max_attempts" + tenant_clause,
            (now, channel, now, *tenant_params),
        )
        conn.execute(
            "UPDATE outbound_messages AS successor SET"
            " status = 'cancelled',"
            " last_error = 'ordered predecessor did not complete successfully',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE successor.channel = ?"
            + (" AND successor.tenant_id = ?" if tenant_id is not None else "")
            + " AND successor.status IN ('queued','retrying')"
            " AND successor.ordering_key IS NOT NULL"
            " AND EXISTS ("
            "   SELECT 1 FROM outbound_messages AS predecessor"
            "   WHERE predecessor.tenant_id = successor.tenant_id"
            "     AND predecessor.source_id = successor.source_id"
            "     AND predecessor.channel = successor.channel"
            "     AND predecessor.ordering_key = successor.ordering_key"
            "     AND predecessor.ordering_position < successor.ordering_position"
            "     AND predecessor.status IN ('dead_letter','cancelled')"
            " )",
            (now, channel, *tenant_params),
        )
        rows = conn.execute(
            "SELECT candidate.id FROM outbound_messages AS candidate"
            " WHERE candidate.channel = ?"
            + (" AND candidate.tenant_id = ?" if tenant_id is not None else "")
            + "   AND candidate.run_after <= ?"
            "   AND (candidate.status = 'queued'"
            "        OR candidate.status = 'retrying'"
            "        OR (candidate.status = 'leased'"
            "            AND candidate.lease_until <= ?"
            "            AND candidate.attempts < candidate.max_attempts))"
            "   AND (candidate.ordering_key IS NULL"
            "        OR candidate.ordering_position = 1"
            "        OR EXISTS ("
            "          SELECT 1 FROM outbound_messages AS predecessor"
            "          WHERE predecessor.tenant_id = candidate.tenant_id"
            "            AND predecessor.source_id = candidate.source_id"
            "            AND predecessor.channel = candidate.channel"
            "            AND predecessor.ordering_key = candidate.ordering_key"
            "            AND predecessor.ordering_position = candidate.ordering_position - 1"
            "            AND predecessor.status = 'sent'"
            "        ))"
            " ORDER BY candidate.priority ASC, candidate.created_at ASC, candidate.rowid ASC"
            " LIMIT ?",
            (channel, *tenant_params, now, now, limit),
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
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM outbound_messages WHERE id = ? AND status = 'leased'"
            " AND lease_token = ? AND lease_until > ?",
            (message_id, lease_token, now),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return False
        if not _task_admission_is_current(conn, row):
            conn.execute(
                "UPDATE outbound_messages SET status = 'cancelled',"
                " last_error = 'dialogue task or routing question became stale before send admission',"
                " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
                " WHERE id = ? AND status = 'leased' AND lease_token = ?",
                (now, message_id, lease_token),
            )
            conn.execute("COMMIT")
            return False
        changed = conn.execute(
            "UPDATE outbound_messages SET status = 'sending', updated_at = ?"
            " WHERE id = ? AND status = 'leased' AND lease_token = ? AND lease_until > ?",
            (now, message_id, lease_token, now),
        ).rowcount
        conn.execute("COMMIT")
        return changed == 1
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def mark_sent(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    lease_token: str,
    result: dict[str, Any] | None = None,
    now: int | None = None,
) -> bool:
    now = now if now is not None else _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE outbound_messages SET"
            " status = 'sent', result_json = ?, lease_owner = NULL, lease_until = NULL,"
            " lease_token = NULL, sent_at = ?, updated_at = ?"
            " WHERE id = ? AND status = 'sending' AND lease_token = ?",
            (json.dumps(result or {}, ensure_ascii=False), now, now, message_id, lease_token),
        )
        if cur.rowcount == 1:
            row = conn.execute(
                "SELECT * FROM outbound_messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"sent outbound message disappeared: {message_id}")
            record_message(
                conn,
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                channel=row["channel"],
                direction="outbound",
                text=row["text"],
                source_message_id=message_id,
                occurred_at=now,
                now=row["created_at"],
            )
            _project_confirmed_dialogue_message(conn, row, result or {}, now=now)
        conn.execute("COMMIT")
        return cur.rowcount == 1
    except BaseException:
        conn.execute("ROLLBACK")
        raise


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


def mark_dead_letter(
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
            " status = 'dead_letter', last_error = ?, lease_owner = NULL,"
            " lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status IN ('leased','sending') AND lease_token = ?",
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


def _validate_telegram_reply_fields(reply_to_message_id: int | None, message_thread_id: int | None) -> None:
    for name, value in (
        ("reply_to_message_id", reply_to_message_id),
        ("message_thread_id", message_thread_id),
    ):
        if value is not None and (type(value) is not int or value < 1):
            raise ValueError(f"{name} must be a positive integer when provided")


def _require_outbound_replay(
    existing: sqlite3.Row,
    *,
    channel: str,
    destination_id: str,
    text: str,
    payload: JsonObject | None,
    priority: int,
    requested_run_after: int | None,
    max_attempts: int,
    correlation_id: str | None,
    ordering_key: str | None,
    ordering_position: int | None,
    task_reference: OutboundTaskReference | None,
    reply_to_message_id: int | None,
    message_thread_id: int | None,
    idempotency_key: str,
) -> None:
    expected_fingerprint = idempotency_fingerprint(
        {
            "channel": channel,
            "destination_id": destination_id,
            "text": text,
            "payload": payload or {},
            "priority": priority,
            "run_after": requested_run_after,
            "max_attempts": max_attempts,
            "correlation_id": correlation_id,
            "ordering_key": ordering_key,
            "ordering_position": ordering_position,
            "task_id": task_reference.task_id if task_reference is not None else None,
            "task_revision": task_reference.task_revision if task_reference is not None else None,
            "routing_question_id": task_reference.routing_question_id if task_reference is not None else None,
            "reply_to_message_id": reply_to_message_id,
            "message_thread_id": message_thread_id,
        }
    )
    stored_fingerprint = existing["idempotency_fingerprint"]
    # Legacy rows do not preserve the original schedule after retries mutate run_after.
    matches = (
        stored_fingerprint == expected_fingerprint
        if stored_fingerprint is not None
        else existing["channel"] == channel
        and existing["destination_id"] == destination_id
        and existing["text"] == text
        and stored_json_matches(existing["payload_json"], payload or {})
        and existing["priority"] == priority
        and existing["max_attempts"] == max_attempts
        and existing["correlation_id"] == correlation_id
        and existing["ordering_key"] == ordering_key
        and existing["ordering_position"] == ordering_position
        and existing["task_id"] == (task_reference.task_id if task_reference is not None else None)
        and existing["task_revision"] == (task_reference.task_revision if task_reference is not None else None)
        and existing["routing_question_id"]
        == (task_reference.routing_question_id if task_reference is not None else None)
        and existing["reply_to_message_id"] == reply_to_message_id
        and existing["message_thread_id"] == message_thread_id
    )
    require_idempotent_replay(
        matches,
        resource="outbound message",
        key=idempotency_key,
        existing_id=str(existing["id"]),
    )


def _validate_task_reference(
    conn: sqlite3.Connection,
    tenant_id: str,
    source_id: str,
    reference: OutboundTaskReference,
) -> None:
    if reference.task_id is not None:
        row = conn.execute(
            "SELECT status, revision FROM dialogue_tasks WHERE tenant_id = ? AND source_id = ? AND id = ?",
            (tenant_id, source_id, reference.task_id),
        ).fetchone()
        if row is None or row["status"] != "active" or int(row["revision"]) != reference.task_revision:
            raise DialogueConflictError("outbound task reference is stale or outside its conversation")
        return
    assert reference.routing_question_id is not None
    row = conn.execute(
        "SELECT status FROM dialogue_questions WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (tenant_id, source_id, reference.routing_question_id),
    ).fetchone()
    if row is None or row["status"] != "pending":
        raise DialogueConflictError("outbound routing question is stale or outside its conversation")


def _task_admission_is_current(conn: sqlite3.Connection, row: sqlite3.Row) -> bool:
    tenant_id, source_id = row["tenant_id"], row["source_id"]
    if row["task_id"] is not None:
        current = conn.execute(
            "SELECT status, revision FROM dialogue_tasks WHERE tenant_id = ? AND source_id = ? AND id = ?",
            (tenant_id, source_id, row["task_id"]),
        ).fetchone()
        return bool(
            current is not None and current["status"] == "active" and int(current["revision"]) == row["task_revision"]
        )
    if row["routing_question_id"] is not None:
        current = conn.execute(
            "SELECT status FROM dialogue_questions WHERE tenant_id = ? AND source_id = ? AND id = ?",
            (tenant_id, source_id, row["routing_question_id"]),
        ).fetchone()
        return bool(current is not None and current["status"] == "pending")
    return True


def _project_confirmed_dialogue_message(
    conn: sqlite3.Connection,
    outbound: sqlite3.Row,
    result: dict[str, Any],
    *,
    now: int,
) -> None:
    task_id = outbound["task_id"]
    question_id = outbound["routing_question_id"]
    if task_id is None and question_id is None:
        return
    external_id = result.get("external_message_id")
    if external_id is None and outbound["channel"] == "telegram":
        external_id = result.get("message_id")
    if type(external_id) is int and external_id > 0:
        external_id = str(external_id)
    if not isinstance(external_id, str) or not external_id.strip() or len(external_id) > 256:
        external_id = None
    if task_id is not None:
        conn.execute(
            "INSERT INTO dialogue_messages"
            " (message_id, tenant_id, source_id, task_id, role, actor_id, text, occurred_at,"
            " input_id, source_message_id)"
            " VALUES (?, ?, ?, ?, 'assistant', NULL, ?, ?, NULL, ?)",
            (
                "outbound_" + str(outbound["id"]),
                outbound["tenant_id"],
                outbound["source_id"],
                task_id,
                outbound["text"],
                now,
                external_id,
            ),
        )
        conn.execute(
            "UPDATE dialogue_tasks SET updated_at = ? WHERE tenant_id = ? AND source_id = ? AND id = ?",
            (now, outbound["tenant_id"], outbound["source_id"], task_id),
        )
    if external_id is None:
        return
    conn.execute(
        "INSERT INTO dialogue_confirmed_references"
        " (tenant_id, source_id, channel, external_message_id, outbound_message_id, task_id, question_id, confirmed_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            outbound["tenant_id"],
            outbound["source_id"],
            outbound["channel"],
            external_id,
            outbound["id"],
            task_id,
            question_id,
            now,
        ),
    )


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
        ordering_key=row["ordering_key"],
        ordering_position=row["ordering_position"],
        task_reference=(
            OutboundTaskReference(
                task_id=row["task_id"],
                task_revision=row["task_revision"],
                routing_question_id=row["routing_question_id"],
            )
            if row["task_id"] is not None or row["routing_question_id"] is not None
            else None
        ),
        reply_to_message_id=row["reply_to_message_id"],
        message_thread_id=row["message_thread_id"],
    )
