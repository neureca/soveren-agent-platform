"""Lease-fenced persistence helpers for planner runs."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.runs.contracts import PlannerRunClaim


def claim_run(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    trigger_event_id: str,
    model: str,
    prompt_version: str,
    input_summary: str | None,
    stale_after_s: int,
    now: int | None = None,
) -> PlannerRunClaim:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    if stale_after_s < 1:
        raise ValueError("stale_after_s must be positive")
    now = now if now is not None else int(time.time())
    operation_key = json.dumps(
        [trigger_event_id, model, prompt_version],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    lease_token = uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM agent_runs WHERE tenant_id = ? AND source_id = ? AND operation_key = ?",
            (tenant_id, source_id, operation_key),
        ).fetchone()
        if row is None:
            run_id = "ar_" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO agent_runs"
                " (id, tenant_id, source_id, trigger_event_id, status, input_summary, model, prompt_version,"
                "  operation_key, lease_token, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    tenant_id,
                    source_id,
                    trigger_event_id,
                    input_summary,
                    model,
                    prompt_version,
                    operation_key,
                    lease_token,
                    now,
                    now,
                ),
            )
            conn.execute("COMMIT")
            return PlannerRunClaim(
                id=run_id,
                status="running",
                acquired=True,
                lease_token=lease_token,
                output=None,
            )

        output = _load_output(row["output_json"])
        if row["status"] in {"completed", "waiting_approval"} and output is not None:
            conn.execute("COMMIT")
            return PlannerRunClaim(
                id=row["id"],
                status=row["status"],
                acquired=False,
                lease_token=None,
                output=output,
            )
        if row["status"] == "running" and row["updated_at"] > now - stale_after_s:
            conn.execute("COMMIT")
            return PlannerRunClaim(
                id=row["id"],
                status="running",
                acquired=False,
                lease_token=None,
                output=None,
            )

        conn.execute(
            "UPDATE agent_runs SET status = 'running', input_summary = ?, output_json = NULL,"
            " lease_token = ?, updated_at = ? WHERE id = ?",
            (input_summary, lease_token, now, row["id"]),
        )
        conn.execute("COMMIT")
        return PlannerRunClaim(
            id=row["id"],
            status="running",
            acquired=True,
            lease_token=lease_token,
            output=None,
        )
    except Exception:
        conn.execute("ROLLBACK")
        raise


def finalize_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    lease_token: str,
    status: str,
    output: dict[str, Any] | None,
    now: int | None = None,
) -> bool:
    if status not in {"completed", "waiting_approval", "failed"}:
        raise ValueError(f"unsupported planner run status: {status}")
    now = now if now is not None else int(time.time())
    payload = json.dumps(output, ensure_ascii=False, default=str) if output is not None else None
    return bool(
        conn.execute(
            "UPDATE agent_runs SET status = ?, output_json = ?, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status = 'running' AND lease_token = ?",
            (status, payload, now, run_id, lease_token),
        ).rowcount
    )


def _load_output(payload: str | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("agent run output must be a JSON object")
    return value
