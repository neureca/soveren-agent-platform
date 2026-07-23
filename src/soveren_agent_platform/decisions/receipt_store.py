"""Lease-fenced SQLite persistence for accepted planner decisions."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid

from soveren_agent_platform.decisions.contracts import DecisionDispatchClaim
from soveren_agent_platform.idempotency import (
    idempotency_fingerprint,
    require_idempotent_replay,
)
from soveren_agent_platform.json_types import JsonObject, require_json_object


def claim_decision_dispatch(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    trigger_event_id: str,
    input_fingerprint: str,
    stale_after_s: int,
    now: int | None = None,
) -> DecisionDispatchClaim:
    _require_identity(tenant_id, source_id, trigger_event_id)
    if not input_fingerprint.strip():
        raise ValueError("decision dispatch input_fingerprint must be non-empty")
    if stale_after_s < 1:
        raise ValueError("decision dispatch stale_after_s must be positive")
    now = int(time.time()) if now is None else now
    lease_token = uuid.uuid4().hex
    lease_until = now + stale_after_s

    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM decision_dispatches"
            " WHERE tenant_id = ? AND source_id = ? AND trigger_event_id = ?",
            (tenant_id, source_id, trigger_event_id),
        ).fetchone()
        if row is None:
            receipt_id = "dd_" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO decision_dispatches"
                " (id, tenant_id, source_id, trigger_event_id, input_fingerprint, status,"
                "  lease_token, lease_until, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, 'planning', ?, ?, ?, ?)",
                (
                    receipt_id,
                    tenant_id,
                    source_id,
                    trigger_event_id,
                    input_fingerprint,
                    lease_token,
                    lease_until,
                    now,
                    now,
                ),
            )
            conn.execute("COMMIT")
            return DecisionDispatchClaim(
                id=receipt_id,
                status="planning",
                acquired=True,
                lease_token=lease_token,
                run_id=None,
                model=None,
                prompt_version=None,
                decision=None,
                planner_result=None,
                dispatch_context=None,
                dispatch_result=None,
            )

        require_idempotent_replay(
            row["input_fingerprint"] == input_fingerprint,
            resource="decision dispatch",
            key=trigger_event_id,
            existing_id=row["id"],
        )
        if row["status"] == "completed":
            claim = _claim_from_row(row, acquired=False, lease_token=None)
            conn.execute("COMMIT")
            return claim
        if row["lease_token"] is not None and int(row["lease_until"] or 0) > now:
            claim = _claim_from_row(row, acquired=False, lease_token=None)
            conn.execute("COMMIT")
            return claim

        updated = conn.execute(
            "UPDATE decision_dispatches SET lease_token = ?, lease_until = ?, updated_at = ?"
            " WHERE id = ? AND status IN ('planning', 'dispatching')"
            " AND (lease_token IS NULL OR lease_until IS NULL OR lease_until <= ?)",
            (lease_token, lease_until, now, row["id"], now),
        ).rowcount
        if not updated:
            current = conn.execute(
                "SELECT * FROM decision_dispatches WHERE id = ?",
                (row["id"],),
            ).fetchone()
            if current is None:
                raise RuntimeError(f"decision dispatch disappeared while claiming: {row['id']}")
            claim = _claim_from_row(current, acquired=False, lease_token=None)
            conn.execute("COMMIT")
            return claim
        current = conn.execute(
            "SELECT * FROM decision_dispatches WHERE id = ?",
            (row["id"],),
        ).fetchone()
        if current is None:
            raise RuntimeError(f"decision dispatch disappeared after claim: {row['id']}")
        claim = _claim_from_row(current, acquired=True, lease_token=lease_token)
        conn.execute("COMMIT")
        return claim
    except Exception:
        conn.execute("ROLLBACK")
        raise


def accept_decision_dispatch(
    conn: sqlite3.Connection,
    receipt_id: str,
    *,
    lease_token: str,
    run_id: str,
    model: str,
    prompt_version: str,
    decision: JsonObject,
    planner_result: JsonObject,
    dispatch_context: JsonObject,
    now: int | None = None,
) -> bool:
    if not receipt_id.strip() or not lease_token.strip() or not run_id.strip():
        raise ValueError("decision dispatch receipt_id, lease_token, and run_id must be non-empty")
    if not model.strip() or not prompt_version.strip():
        raise ValueError("decision dispatch model and prompt_version must be non-empty")
    if not isinstance(decision, dict) or not decision:
        raise ValueError("accepted decision must be a non-empty JSON object")
    if not isinstance(planner_result, dict) or not isinstance(dispatch_context, dict):
        raise TypeError("planner_result and dispatch_context must be JSON objects")
    now = int(time.time()) if now is None else now
    return bool(
        conn.execute(
            "UPDATE decision_dispatches"
            " SET status = 'dispatching', run_id = ?, model = ?, prompt_version = ?,"
            " decision_json = ?, decision_fingerprint = ?, planner_result_json = ?,"
            " dispatch_context_json = ?, accepted_at = ?, updated_at = ?"
            " WHERE id = ? AND status = 'planning' AND lease_token = ?",
            (
                run_id,
                model,
                prompt_version,
                _dump_json(decision),
                idempotency_fingerprint(decision),
                _dump_json(planner_result),
                _dump_json(dispatch_context),
                now,
                now,
                receipt_id,
                lease_token,
            ),
        ).rowcount
    )


def complete_decision_dispatch(
    conn: sqlite3.Connection,
    receipt_id: str,
    *,
    lease_token: str,
    dispatch_result: JsonObject,
    now: int | None = None,
) -> bool:
    if not receipt_id.strip() or not lease_token.strip():
        raise ValueError("decision dispatch receipt_id and lease_token must be non-empty")
    if not isinstance(dispatch_result, dict):
        raise TypeError("dispatch_result must be a JSON object")
    target = dispatch_result.get("target")
    created = dispatch_result.get("created")
    metadata = dispatch_result.get("metadata")
    if not isinstance(target, str) or not target.strip():
        raise ValueError("dispatch_result target must be a non-empty string")
    if not isinstance(created, bool) or not isinstance(metadata, dict):
        raise ValueError("dispatch_result created and metadata fields are invalid")
    now = int(time.time()) if now is None else now
    effect_id = dispatch_result.get("id")
    return bool(
        conn.execute(
            "UPDATE decision_dispatches"
            " SET status = 'completed', dispatch_target = ?, effect_id = ?,"
            " dispatch_result_json = ?, lease_token = NULL, lease_until = NULL,"
            " completed_at = ?, updated_at = ?"
            " WHERE id = ? AND status = 'dispatching' AND lease_token = ?",
            (
                str(target) if target is not None else None,
                str(effect_id) if effect_id is not None else None,
                _dump_json(dispatch_result),
                now,
                now,
                receipt_id,
                lease_token,
            ),
        ).rowcount
    )


def release_decision_dispatch(
    conn: sqlite3.Connection,
    receipt_id: str,
    *,
    lease_token: str,
    now: int | None = None,
) -> bool:
    if not receipt_id.strip() or not lease_token.strip():
        raise ValueError("decision dispatch receipt_id and lease_token must be non-empty")
    now = int(time.time()) if now is None else now
    return bool(
        conn.execute(
            "UPDATE decision_dispatches"
            " SET lease_token = NULL, lease_until = NULL, updated_at = ?"
            " WHERE id = ? AND status IN ('planning', 'dispatching') AND lease_token = ?",
            (now, receipt_id, lease_token),
        ).rowcount
    )


def _claim_from_row(
    row: sqlite3.Row,
    *,
    acquired: bool,
    lease_token: str | None,
) -> DecisionDispatchClaim:
    decision = _load_json_object(row["decision_json"], "decision")
    decision_fingerprint = row["decision_fingerprint"]
    if decision is not None and (
        not isinstance(decision_fingerprint, str)
        or idempotency_fingerprint(decision) != decision_fingerprint
    ):
        raise ValueError(f"decision dispatch fingerprint is invalid: {row['id']}")
    return DecisionDispatchClaim(
        id=str(row["id"]),
        status=str(row["status"]),
        acquired=acquired,
        lease_token=lease_token,
        run_id=_optional_str(row["run_id"]),
        model=_optional_str(row["model"]),
        prompt_version=_optional_str(row["prompt_version"]),
        decision=decision,
        planner_result=_load_json_object(row["planner_result_json"], "planner result"),
        dispatch_context=_load_json_object(row["dispatch_context_json"], "dispatch context"),
        dispatch_result=_load_json_object(row["dispatch_result_json"], "dispatch result"),
    )


def _require_identity(tenant_id: str, source_id: str, trigger_event_id: str) -> None:
    if not tenant_id.strip() or not source_id.strip() or not trigger_event_id.strip():
        raise ValueError("decision dispatch tenant_id, source_id, and trigger_event_id must be non-empty")


def _dump_json(value: JsonObject) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _load_json_object(payload: str | None, label: str) -> JsonObject | None:
    if payload is None:
        return None
    return require_json_object(json.loads(payload), label=f"decision dispatch {label}")


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)
