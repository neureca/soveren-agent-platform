"""SQLite store for inbound batching."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.batching.contracts import BatchDecision, BatchState, InboundMessage
from soveren_agent_platform.batching.rules import (
    DEFAULT_MAX_COUNT,
    DEFAULT_MAX_WINDOW_S,
    DEFAULT_QUIET_WINDOW_S,
    extract_features,
)
from soveren_agent_platform.queue.durable import enqueue


def append_inbound_message(conn: sqlite3.Connection, message: InboundMessage) -> str | None:
    now = int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT m.batch_id, b.status"
            " FROM inbound_batch_messages m"
            " JOIN inbound_batches b ON b.id = m.batch_id"
            " WHERE m.raw_event_id = ?",
            (message.raw_event_id,),
        ).fetchone()
        if existing is not None:
            conn.execute("COMMIT")
            return existing["batch_id"] if existing["status"] == "collecting" else None

        batch = open_batch(
            conn,
            tenant_id=message.tenant_id,
            channel=message.channel,
            source_id=message.source_id,
        )
        if batch is None:
            batch_id = "ib_" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO inbound_batches"
                " (id, tenant_id, channel, source_id, status, first_message_at,"
                "  last_message_at, message_count, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, 'collecting', ?, ?, 0, ?, ?)",
                (
                    batch_id,
                    message.tenant_id,
                    message.channel,
                    message.source_id,
                    message.message_at,
                    message.message_at,
                    now,
                    now,
                ),
            )
        else:
            batch_id = batch["id"]

        payload = {
            **message.payload,
            "channel": message.channel,
            "source_id": message.source_id,
            "raw_event_id": message.raw_event_id,
            "source_event_id": message.source_event_id,
            "text": message.text,
            "message_at": message.message_at,
        }
        conn.execute(
            "INSERT INTO inbound_batch_messages"
            " (id, batch_id, tenant_id, channel, source_id, raw_event_id,"
            "  source_event_id, payload_json, message_at, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "ibm_" + uuid.uuid4().hex,
                batch_id,
                message.tenant_id,
                message.channel,
                message.source_id,
                message.raw_event_id,
                message.source_event_id,
                json.dumps(payload, ensure_ascii=False),
                message.message_at,
                now,
            ),
        )
        conn.execute(
            "UPDATE inbound_batches"
            " SET last_message_at = MAX(last_message_at, ?),"
            "     message_count = (SELECT COUNT(*) FROM inbound_batch_messages WHERE batch_id = ?),"
            "     updated_at = ?"
            " WHERE id = ?",
            (message.message_at, batch_id, now, batch_id),
        )
        conn.execute("COMMIT")
        return batch_id
    except Exception:
        conn.execute("ROLLBACK")
        raise


def open_batch(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    channel: str,
    source_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM inbound_batches"
        " WHERE tenant_id = ? AND channel = ? AND source_id = ? AND status = 'collecting'"
        " ORDER BY updated_at DESC LIMIT 1",
        (tenant_id, channel, source_id),
    ).fetchone()


def load_state(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
    max_window_s: int = DEFAULT_MAX_WINDOW_S,
    max_count: int = DEFAULT_MAX_COUNT,
    now: int | None = None,
) -> BatchState | None:
    batch = conn.execute(
        "SELECT * FROM inbound_batches WHERE id = ? AND status = 'collecting'",
        (batch_id,),
    ).fetchone()
    if batch is None:
        return None
    rows = conn.execute(
        "SELECT * FROM inbound_batch_messages"
        " WHERE batch_id = ? ORDER BY message_at ASC, created_at ASC",
        (batch_id,),
    ).fetchall()
    messages = [json.loads(row["payload_json"]) for row in rows]
    features = [
        extract_features(message, prev=messages[idx - 1] if idx else None)
        for idx, message in enumerate(messages)
    ]
    return BatchState(
        batch_id=batch_id,
        tenant_id=batch["tenant_id"],
        channel=batch["channel"],
        source_id=batch["source_id"],
        messages=messages,
        features=features,
        now=now if now is not None else int(time.time()),
        first_message_at=int(batch["first_message_at"]),
        last_message_at=int(batch["last_message_at"]),
        message_count=int(batch["message_count"]),
        quiet_window_s=quiet_window_s,
        max_window_s=max_window_s,
        max_count=max_count,
    )


def store_decision(
    conn: sqlite3.Connection,
    batch_id: str,
    decision: BatchDecision,
    *,
    state: BatchState | None = None,
) -> None:
    telemetry = {
        "action": decision.action,
        "wait_score": decision.wait_score,
        "flush_score": decision.flush_score,
        "matched_rules": decision.matched_rules,
        "reasons": decision.reasons,
    }
    if state is not None:
        telemetry.update({
            "message_count": state.message_count,
            "age_s": state.now - state.first_message_at,
            "quiet_age_s": state.now - state.last_message_at,
            "quiet_window_s": state.quiet_window_s,
            "max_window_s": state.max_window_s,
            "max_count": state.max_count,
        })
    conn.execute(
        "UPDATE inbound_batches SET decision_json = ?, updated_at = ? WHERE id = ?",
        (json.dumps(telemetry, ensure_ascii=False), int(time.time()), batch_id),
    )


def mark_routed(conn: sqlite3.Connection, batch_id: str) -> bool:
    return bool(
        conn.execute(
            "UPDATE inbound_batches SET status = 'routed', updated_at = ?"
            " WHERE id = ? AND status = 'collecting'",
            (int(time.time()), batch_id),
        ).rowcount
    )


def route_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    tenant_id: str,
    recipient: str,
    message_type: str,
    payload: dict[str, Any],
    idempotency_key: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> bool:
    conn.execute("BEGIN IMMEDIATE")
    try:
        routed = mark_routed(conn, batch_id)
        if routed:
            enqueue(
                conn,
                tenant_id=tenant_id,
                recipient=recipient,
                message_type=message_type,
                payload=payload,
                idempotency_key=idempotency_key,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
        conn.execute("COMMIT")
        return routed
    except Exception:
        conn.execute("ROLLBACK")
        raise


def batch_payload(state: BatchState) -> dict:
    first = state.messages[0]
    last = state.messages[-1]
    parts: list[str] = []
    for message in state.messages:
        text = str(message.get("text") or "").strip()
        if not text:
            continue
        author = message.get("from_first_name") or message.get("from_username") or "user"
        parts.append(f"{author}: {text}")
    return {
        **last,
        "channel": state.channel,
        "source_id": state.source_id,
        "text": "\n".join(parts),
        "batch_id": state.batch_id,
        "batch_message_count": len(state.messages),
        "batch_messages": state.messages,
        "batch_raw_event_ids": [msg.get("raw_event_id") for msg in state.messages],
        "first_message": first,
    }
