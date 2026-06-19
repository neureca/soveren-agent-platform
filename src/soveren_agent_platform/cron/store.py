"""SQLite store for platform cron jobs."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from dateutil.rrule import rrulestr

from soveren_agent_platform.cron.contracts import CronJob


def insert_job(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    name: str,
    payload: dict[str, Any],
    run_at: int,
    rrule: str | None = None,
    timezone: str = "UTC",
    max_attempts: int = 5,
    now: int | None = None,
) -> str:
    now = now if now is not None else int(time.time())
    job_id = "cron_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO cron_jobs ("
        "  id, tenant_id, name, payload_json, status, run_at, rrule, timezone,"
        "  attempts, max_attempts, created_at, updated_at"
        ") VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?, ?)",
        (
            job_id,
            tenant_id,
            name,
            json.dumps(payload, ensure_ascii=False),
            run_at,
            rrule,
            timezone,
            max_attempts,
            now,
            now,
        ),
    )
    return job_id


def claim_due_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    now: int | None = None,
) -> list[CronJob]:
    now = now if now is not None else int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id FROM cron_jobs"
            " WHERE run_at <= ?"
            "   AND (status = 'pending'"
            "        OR (status = 'leased' AND lease_until <= ?))"
            " ORDER BY run_at ASC, created_at ASC"
            " LIMIT ?",
            (now, now, limit),
        ).fetchall()
        ids = [row["id"] for row in rows]
        if not ids:
            conn.execute("COMMIT")
            return []
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            "UPDATE cron_jobs SET"
            "  status = 'leased',"
            "  lease_owner = ?,"
            "  lease_until = ?,"
            "  attempts = attempts + 1,"
            "  updated_at = ?"
            f" WHERE id IN ({placeholders})",
            (lease_owner, now + lease_seconds, now, *ids),
        )
        claimed = conn.execute(
            f"SELECT * FROM cron_jobs WHERE id IN ({placeholders})"
            " ORDER BY run_at ASC, created_at ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return [_job_from_row(row) for row in claimed]
    except Exception:
        conn.execute("ROLLBACK")
        raise


def complete_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    fired_at: int | None = None,
) -> None:
    fired_at = fired_at if fired_at is not None else int(time.time())
    row = conn.execute(
        "SELECT run_at, rrule, timezone FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if row is None:
        return
    next_run_at = _next_run_at(row["run_at"], row["rrule"], row["timezone"], fired_at)
    if next_run_at is None:
        conn.execute(
            "UPDATE cron_jobs SET"
            "  status = 'fired',"
            "  lease_owner = NULL,"
            "  lease_until = NULL,"
            "  updated_at = ?"
            " WHERE id = ?",
            (fired_at, job_id),
        )
        return
    conn.execute(
        "UPDATE cron_jobs SET"
        "  status = 'pending',"
        "  run_at = ?,"
        "  lease_owner = NULL,"
        "  lease_until = NULL,"
        "  updated_at = ?"
        " WHERE id = ?",
        (next_run_at, fired_at, job_id),
    )


def fail_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    retry_at: int,
    last_error: str,
    now: int | None = None,
) -> None:
    now = now if now is not None else int(time.time())
    row = conn.execute(
        "SELECT attempts, max_attempts FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if row is None:
        return
    new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "pending"
    conn.execute(
        "UPDATE cron_jobs SET"
        "  status = ?,"
        "  run_at = ?,"
        "  last_error = ?,"
        "  lease_owner = NULL,"
        "  lease_until = NULL,"
        "  updated_at = ?"
        " WHERE id = ?",
        (new_status, retry_at, last_error, now, job_id),
    )


def _next_run_at(
    current_run_at: int,
    rrule_body: str | None,
    timezone: str,
    fired_at: int,
) -> int | None:
    if not rrule_body:
        return None
    tz = ZoneInfo(timezone)
    anchor = datetime.fromtimestamp(current_run_at, tz)
    after = datetime.fromtimestamp(fired_at, tz)
    next_dt = rrulestr(rrule_body, dtstart=anchor).after(after)
    return int(next_dt.timestamp()) if next_dt is not None else None


def _job_from_row(row: sqlite3.Row) -> CronJob:
    return CronJob(
        id=row["id"],
        tenant_id=row["tenant_id"],
        name=row["name"],
        payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
        run_at=row["run_at"],
        rrule=row["rrule"],
        timezone=row["timezone"],
        attempts=row["attempts"],
    )

