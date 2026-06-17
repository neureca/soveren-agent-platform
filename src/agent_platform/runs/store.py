"""CRUD helpers for `agent_runs`."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any


def insert_run(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    trigger_event_id: str,
    model: str,
    prompt_version: str,
    input_summary: str | None,
    now: int | None = None,
) -> str:
    now = now if now is not None else int(time.time())
    run_id = "ar_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO agent_runs"
        " (id, tenant_id, trigger_event_id, status, input_summary, model, prompt_version,"
        "  created_at, updated_at)"
        " VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?)",
        (run_id, tenant_id, trigger_event_id, input_summary, model, prompt_version, now, now),
    )
    return run_id


def finalize_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    status: str,
    output: dict[str, Any] | None,
    now: int | None = None,
) -> None:
    now = now if now is not None else int(time.time())
    payload = json.dumps(output, ensure_ascii=False, default=str) if output is not None else None
    conn.execute(
        "UPDATE agent_runs SET status = ?, output_json = ?, updated_at = ? WHERE id = ?",
        (status, payload, now, run_id),
    )

