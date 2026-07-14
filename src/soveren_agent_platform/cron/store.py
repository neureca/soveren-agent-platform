"""SQLite store for platform cron jobs."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.cron.schedule import next_run_at, validate_schedule
from soveren_agent_platform.idempotency import (
    idempotency_fingerprint,
    require_idempotent_replay,
    stored_json_matches,
)


def insert_job(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    name: str,
    payload: dict[str, Any],
    run_at: int,
    rrule: str | None = None,
    timezone: str = "UTC",
    max_attempts: int = 5,
    idempotency_key: str | None = None,
    now: int | None = None,
) -> tuple[str, bool]:
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    now = now if now is not None else int(time.time())
    validate_schedule(run_at=run_at, rrule_body=rrule, timezone=timezone)
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    fingerprint = idempotency_fingerprint(
        {
            "name": name,
            "payload": payload,
            "run_at": run_at,
            "rrule": rrule,
            "timezone": timezone,
            "max_attempts": max_attempts,
        }
    )
    if idempotency_key is not None:
        existing = conn.execute(
            "SELECT * FROM cron_jobs WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is not None:
            _require_cron_replay(
                existing,
                key=idempotency_key,
                name=name,
                payload=payload,
                rrule=rrule,
                timezone=timezone,
                max_attempts=max_attempts,
                fingerprint=fingerprint,
            )
            return existing["id"], False
    job_id = "cron_" + uuid.uuid4().hex
    try:
        conn.execute(
            "INSERT INTO cron_jobs ("
            "  id, tenant_id, source_id, name, payload_json, status, run_at, rrule, timezone,"
            "  attempts, max_attempts, idempotency_key, idempotency_fingerprint, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?, ?, ?, ?)",
            (
                job_id,
                tenant_id,
                source_id,
                name,
                json.dumps(payload, ensure_ascii=False),
                run_at,
                rrule,
                timezone,
                max_attempts,
                idempotency_key,
                fingerprint,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        if idempotency_key is None:
            raise
        existing = conn.execute(
            "SELECT * FROM cron_jobs WHERE tenant_id = ? AND source_id = ? AND idempotency_key = ?",
            (tenant_id, source_id, idempotency_key),
        ).fetchone()
        if existing is None:
            raise
        _require_cron_replay(
            existing,
            key=idempotency_key,
            name=name,
            payload=payload,
            rrule=rrule,
            timezone=timezone,
            max_attempts=max_attempts,
            fingerprint=fingerprint,
        )
        return existing["id"], False
    return job_id, True


def _require_cron_replay(
    row: sqlite3.Row,
    *,
    key: str,
    name: str,
    payload: dict[str, Any],
    rrule: str | None,
    timezone: str,
    max_attempts: int,
    fingerprint: str,
) -> None:
    stored_fingerprint = row["idempotency_fingerprint"]
    # A recurring legacy row no longer contains its original run_at after it advances.
    require_idempotent_replay(
        stored_fingerprint == fingerprint
        if stored_fingerprint is not None
        else row["name"] == name
        and stored_json_matches(row["payload_json"], payload)
        and row["rrule"] == rrule
        and row["timezone"] == timezone
        and row["max_attempts"] == max_attempts,
        resource="cron job",
        key=key,
        existing_id=row["id"],
    )


def claim_due_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int,
    lease_owner: str,
    lease_seconds: int,
    now: int | None = None,
) -> list[CronJob]:
    if limit < 1:
        raise ValueError("limit must be positive")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    now = now if now is not None else int(time.time())
    lease_token = uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE cron_jobs SET"
            " status = 'uncertain',"
            " last_error = 'cron execution outcome is uncertain after lease expiry',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE status = 'running' AND lease_until <= ?",
            (now, now),
        )
        conn.execute(
            "UPDATE cron_jobs SET"
            " status = 'dead_letter',"
            " retry_at = NULL,"
            " last_error = 'cron lease expired after the maximum number of attempts',"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE status = 'leased' AND lease_until <= ? AND attempts >= max_attempts",
            (now, now),
        )
        ids: list[str] = []
        while len(ids) < limit:
            rows = conn.execute(
                "SELECT id, run_at, rrule, timezone FROM cron_jobs"
                " WHERE COALESCE(retry_at, run_at) <= ?"
                "   AND (status = 'pending'"
                "        OR (status = 'leased' AND lease_until <= ? AND attempts < max_attempts))"
                " ORDER BY COALESCE(retry_at, run_at) ASC, created_at ASC, rowid ASC"
                " LIMIT ?",
                (now, now, limit - len(ids)),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                try:
                    validate_schedule(
                        run_at=row["run_at"],
                        rrule_body=row["rrule"],
                        timezone=row["timezone"],
                    )
                except (TypeError, ValueError) as exc:
                    conn.execute(
                        "UPDATE cron_jobs SET status = 'dead_letter', last_error = ?,"
                        " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
                        " WHERE id = ?",
                        (f"invalid cron schedule: {exc}", now, row["id"]),
                    )
                else:
                    updated = conn.execute(
                        "UPDATE cron_jobs SET status = 'leased', lease_owner = ?,"
                        " lease_until = ?, lease_token = ?, attempts = attempts + 1, updated_at = ?"
                        " WHERE id = ? AND (status = 'pending'"
                        " OR (status = 'leased' AND lease_until <= ? AND attempts < max_attempts))",
                        (lease_owner, now + lease_seconds, lease_token, now, row["id"], now),
                    ).rowcount
                    if updated == 1:
                        ids.append(row["id"])
        if not ids:
            conn.execute("COMMIT")
            return []
        placeholders = ",".join("?" * len(ids))
        claimed = conn.execute(
            f"SELECT * FROM cron_jobs WHERE id IN ({placeholders})"
            " ORDER BY COALESCE(retry_at, run_at) ASC, created_at ASC, rowid ASC",
            ids,
        ).fetchall()
        conn.execute("COMMIT")
        return [_job_from_row(row) for row in claimed]
    except Exception:
        conn.execute("ROLLBACK")
        raise


def renew_lease(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    lease_token: str,
    lease_seconds: int,
    now: int | None = None,
) -> bool:
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    now = now if now is not None else int(time.time())
    return bool(
        conn.execute(
            "UPDATE cron_jobs SET lease_until = ?, updated_at = ?"
            " WHERE id = ? AND status IN ('leased','running') AND lease_token = ?"
            "   AND lease_until > ?",
            (now + lease_seconds, now, job_id, lease_token, now),
        ).rowcount
    )


def start_execution(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    lease_token: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else int(time.time())
    return bool(
        conn.execute(
            "UPDATE cron_jobs SET status = 'running', updated_at = ?"
            " WHERE id = ? AND status = 'leased' AND lease_token = ? AND lease_until > ?",
            (now, job_id, lease_token, now),
        ).rowcount
    )


def complete_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    lease_token: str,
    fired_at: int | None = None,
) -> bool:
    fired_at = fired_at if fired_at is not None else int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT run_at, rrule, timezone FROM cron_jobs WHERE id = ? AND status = 'running' AND lease_token = ?",
            (job_id, lease_token),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return False
        following_run_at = next_run_at(row["run_at"], row["rrule"], row["timezone"], fired_at)
        if following_run_at is None:
            updated = conn.execute(
                "UPDATE cron_jobs SET"
                "  status = 'fired', lease_owner = NULL, lease_until = NULL,"
                "  lease_token = NULL, retry_at = NULL, updated_at = ?"
                " WHERE id = ? AND status = 'running' AND lease_token = ?",
                (fired_at, job_id, lease_token),
            ).rowcount
        else:
            updated = conn.execute(
                "UPDATE cron_jobs SET"
                "  status = 'pending', run_at = ?, retry_at = NULL, attempts = 0,"
                "  lease_owner = NULL, lease_until = NULL, lease_token = NULL,"
                "  updated_at = ?"
                " WHERE id = ? AND status = 'running' AND lease_token = ?",
                (following_run_at, fired_at, job_id, lease_token),
            ).rowcount
        conn.execute("COMMIT")
        return updated == 1
    except Exception:
        conn.execute("ROLLBACK")
        raise


def fail_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    lease_token: str,
    retry_at: int,
    last_error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT attempts, max_attempts FROM cron_jobs WHERE id = ? AND status = 'running' AND lease_token = ?",
            (job_id, lease_token),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return False
        new_status = "dead_letter" if row["attempts"] >= row["max_attempts"] else "pending"
        next_retry_at = retry_at if new_status == "pending" else None
        updated = conn.execute(
            "UPDATE cron_jobs SET"
            "  status = ?, retry_at = ?, last_error = ?, lease_owner = NULL,"
            "  lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status = 'running' AND lease_token = ?",
            (new_status, next_retry_at, last_error, now, job_id, lease_token),
        ).rowcount
        conn.execute("COMMIT")
        return updated == 1
    except Exception:
        conn.execute("ROLLBACK")
        raise


def mark_uncertain(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    lease_token: str,
    last_error: str,
    now: int | None = None,
) -> bool:
    now = now if now is not None else int(time.time())
    return bool(
        conn.execute(
            "UPDATE cron_jobs SET status = 'uncertain', last_error = ?,"
            " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
            " WHERE id = ? AND status = 'running' AND lease_token = ?",
            (last_error, now, job_id, lease_token),
        ).rowcount
    )


def _job_from_row(row: sqlite3.Row) -> CronJob:
    return CronJob(
        id=row["id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        name=row["name"],
        payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
        run_at=row["run_at"],
        rrule=row["rrule"],
        timezone=row["timezone"],
        attempts=row["attempts"],
        lease_token=row["lease_token"],
        retry_at=row["retry_at"],
    )
