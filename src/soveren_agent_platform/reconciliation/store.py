"""Atomic SQLite transitions for uncertain-effect reconciliation."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Literal

from soveren_agent_platform.conversation_history.store import record_message
from soveren_agent_platform.cron.schedule import next_run_at
from soveren_agent_platform.idempotency import require_idempotent_replay
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.reconciliation.contracts import (
    ActionResolution,
    CronResolution,
    OutboundResolution,
    ReconciliationResult,
)

EffectType = Literal["action", "outbound", "cron"]


def resolve_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    tenant_id: str,
    source_id: str,
    resolution: ActionResolution,
    request_key: str,
    actor_id: str,
    evidence: dict[str, Any],
    now: int | None = None,
) -> ReconciliationResult:
    if resolution not in {"executed", "failed", "not_executed"}:
        raise ValueError(f"unsupported action resolution: {resolution}")
    now = now if now is not None else int(time.time())
    evidence_json = _validate_request(tenant_id, source_id, request_key, actor_id, evidence)
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = _existing(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="action",
            effect_id=action_id,
            request_key=request_key,
            resolution=resolution,
            actor_id=actor_id,
            evidence_json=evidence_json,
        )
        if existing is not None:
            conn.execute("COMMIT")
            return existing
        row = conn.execute(
            "SELECT status, source_event_id FROM actions WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (action_id, tenant_id, source_id),
        ).fetchone()
        _require_uncertain(row, "action", action_id)
        if resolution == "executed":
            status = "executed"
            conn.execute(
                "UPDATE actions SET status = 'executed', executed_at = ?, result_json = ?,"
                " last_error = NULL, updated_at = ?"
                " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'uncertain'",
                (now, evidence_json, now, action_id, tenant_id, source_id),
            )
        elif resolution == "failed":
            status = "failed"
            conn.execute(
                "UPDATE actions SET status = 'failed', result_json = ?, last_error = ?, updated_at = ?"
                " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'uncertain'",
                (evidence_json, _error_text(evidence, resolution), now, action_id, tenant_id, source_id),
            )
        else:
            status = "approved"
            conn.execute(
                "UPDATE actions SET status = 'approved', result_json = NULL, last_error = NULL, updated_at = ?"
                " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'uncertain'",
                (now, action_id, tenant_id, source_id),
            )
            event_id = enqueue(
                conn,
                tenant_id=tenant_id,
                recipient="actions",
                message_type="ExecuteAction",
                payload={"action_id": action_id, "source_id": source_id},
                idempotency_key=f"reconcile-action:{action_id}:{request_key}",
                correlation_id=action_id,
                causation_id=row["source_event_id"],
                now=now,
            )
            if event_id is None:
                raise RuntimeError("reconciliation execution event already exists without an audit record")
        _record(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="action",
            effect_id=action_id,
            request_key=request_key,
            resolution=resolution,
            result_status=status,
            actor_id=actor_id,
            evidence_json=evidence_json,
            now=now,
        )
        conn.execute("COMMIT")
        return ReconciliationResult(effect_id=action_id, status=status, applied=True)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def resolve_outbound(
    conn: sqlite3.Connection,
    message_id: str,
    *,
    tenant_id: str,
    source_id: str,
    resolution: OutboundResolution,
    request_key: str,
    actor_id: str,
    evidence: dict[str, Any],
    effect_at: int | None = None,
    retry_at: int | None = None,
    now: int | None = None,
) -> ReconciliationResult:
    if resolution not in {"sent", "failed", "not_sent"}:
        raise ValueError(f"unsupported outbound resolution: {resolution}")
    now = now if now is not None else int(time.time())
    effect_at = effect_at if effect_at is not None else now
    retry_at = retry_at if retry_at is not None else now
    evidence_json = _validate_request(tenant_id, source_id, request_key, actor_id, evidence)
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = _existing(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="outbound",
            effect_id=message_id,
            request_key=request_key,
            resolution=resolution,
            actor_id=actor_id,
            evidence_json=evidence_json,
        )
        if existing is not None:
            conn.execute("COMMIT")
            return existing
        row = conn.execute(
            "SELECT * FROM outbound_messages WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (message_id, tenant_id, source_id),
        ).fetchone()
        _require_uncertain(row, "outbound message", message_id)
        if resolution == "sent":
            status = "sent"
            sent_at: int | None = effect_at
            result_json: str | None = evidence_json
            last_error: str | None = None
        elif resolution == "failed":
            status = "dead_letter"
            sent_at = None
            result_json = evidence_json
            last_error = _error_text(evidence, resolution)
        else:
            status = "queued"
            sent_at = None
            result_json = None
            last_error = None
        conn.execute(
            "UPDATE outbound_messages SET status = ?, sent_at = ?, result_json = ?, last_error = ?,"
            " run_after = CASE WHEN ? = 'queued' THEN ? ELSE run_after END,"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'uncertain'",
            (
                status,
                sent_at,
                result_json,
                last_error,
                status,
                retry_at,
                now,
                message_id,
                tenant_id,
                source_id,
            ),
        )
        if resolution == "sent":
            record_message(
                conn,
                tenant_id=tenant_id,
                source_id=source_id,
                channel=row["channel"],
                direction="outbound",
                text=row["text"],
                source_message_id=message_id,
                occurred_at=effect_at,
                now=row["created_at"],
            )
        _record(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="outbound",
            effect_id=message_id,
            request_key=request_key,
            resolution=resolution,
            result_status=status,
            actor_id=actor_id,
            evidence_json=evidence_json,
            now=now,
        )
        conn.execute("COMMIT")
        return ReconciliationResult(effect_id=message_id, status=status, applied=True)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def resolve_cron(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    tenant_id: str,
    source_id: str,
    resolution: CronResolution,
    request_key: str,
    actor_id: str,
    evidence: dict[str, Any],
    effect_at: int | None = None,
    retry_at: int | None = None,
    now: int | None = None,
) -> ReconciliationResult:
    if resolution not in {"fired", "failed", "not_fired"}:
        raise ValueError(f"unsupported cron resolution: {resolution}")
    now = now if now is not None else int(time.time())
    effect_at = effect_at if effect_at is not None else now
    retry_at = retry_at if retry_at is not None else now
    evidence_json = _validate_request(tenant_id, source_id, request_key, actor_id, evidence)
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = _existing(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="cron",
            effect_id=job_id,
            request_key=request_key,
            resolution=resolution,
            actor_id=actor_id,
            evidence_json=evidence_json,
        )
        if existing is not None:
            conn.execute("COMMIT")
            return existing
        row = conn.execute(
            "SELECT status, schedule_anchor_at, run_at, rrule, timezone, attempts FROM cron_jobs"
            " WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (job_id, tenant_id, source_id),
        ).fetchone()
        _require_uncertain(row, "cron job", job_id)
        if resolution == "fired":
            following_run_at = next_run_at(
                row["schedule_anchor_at"],
                row["rrule"],
                row["timezone"],
                effect_at,
            )
            status = "fired" if following_run_at is None else "pending"
            run_at = row["run_at"] if following_run_at is None else following_run_at
            next_retry_at = None
            attempts = row["attempts"] if following_run_at is None else 0
            last_error = None
        elif resolution == "failed":
            status = "dead_letter"
            run_at = row["run_at"]
            next_retry_at = None
            attempts = row["attempts"]
            last_error = _error_text(evidence, resolution)
        else:
            status = "pending"
            run_at = row["run_at"]
            next_retry_at = retry_at
            attempts = 0
            last_error = None
        conn.execute(
            "UPDATE cron_jobs SET status = ?, run_at = ?, retry_at = ?, attempts = ?, last_error = ?,"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = 'uncertain'",
            (status, run_at, next_retry_at, attempts, last_error, now, job_id, tenant_id, source_id),
        )
        _record(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            effect_type="cron",
            effect_id=job_id,
            request_key=request_key,
            resolution=resolution,
            result_status=status,
            actor_id=actor_id,
            evidence_json=evidence_json,
            now=now,
        )
        conn.execute("COMMIT")
        return ReconciliationResult(effect_id=job_id, status=status, applied=True)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _validate_request(
    tenant_id: str,
    source_id: str,
    request_key: str,
    actor_id: str,
    evidence: dict[str, Any],
) -> str:
    if not tenant_id or not source_id or not request_key or not actor_id:
        raise ValueError("tenant_id, source_id, request_key, and actor_id must be non-empty")
    if not evidence:
        raise ValueError("reconciliation evidence must be non-empty")
    return json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _require_uncertain(row: sqlite3.Row | None, effect_name: str, effect_id: str) -> None:
    if row is None:
        raise KeyError(f"{effect_name} not found: {effect_id}")
    if row["status"] != "uncertain":
        raise ValueError(f"{effect_name} {effect_id} is not uncertain: {row['status']}")


def _existing(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    effect_type: EffectType,
    effect_id: str,
    request_key: str,
    resolution: str,
    actor_id: str,
    evidence_json: str,
) -> ReconciliationResult | None:
    row = conn.execute(
        "SELECT * FROM effect_reconciliations"
        " WHERE tenant_id = ? AND source_id = ? AND effect_type = ? AND request_key = ?",
        (tenant_id, source_id, effect_type, request_key),
    ).fetchone()
    if row is None:
        return None
    expected = (effect_id, resolution, actor_id, evidence_json)
    actual = (row["effect_id"], row["resolution"], row["actor_id"], row["evidence_json"])
    require_idempotent_replay(
        actual == expected,
        resource=f"{effect_type} reconciliation",
        key=request_key,
        existing_id=row["id"],
    )
    return ReconciliationResult(effect_id=effect_id, status=row["result_status"], applied=False)


def _record(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    effect_type: EffectType,
    effect_id: str,
    request_key: str,
    resolution: str,
    result_status: str,
    actor_id: str,
    evidence_json: str,
    now: int,
) -> None:
    conn.execute(
        "INSERT INTO effect_reconciliations"
        " (id, tenant_id, source_id, effect_type, effect_id, request_key, resolution, result_status,"
        " actor_id, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "rec_" + uuid.uuid4().hex,
            tenant_id,
            source_id,
            effect_type,
            effect_id,
            request_key,
            resolution,
            result_status,
            actor_id,
            evidence_json,
            now,
        ),
    )


def _error_text(evidence: dict[str, Any], resolution: str) -> str:
    return str(evidence.get("error") or evidence.get("reason") or f"reconciled as {resolution}")[:500]
