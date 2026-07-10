"""Lifecycle helpers for runtime execution sessions."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass

from soveren_agent_platform.sessions.backend import TenantBoundaryError, ensure_tenant_boundary
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.registry import SessionBackendMapping, normalize_session_backends
from soveren_agent_platform.sessions.store import set_session_status

log = logging.getLogger(__name__)


def _now() -> int:
    return int(time.time())


@dataclass(frozen=True, slots=True)
class SessionLifecyclePolicy:
    """Resource limits for automatic idle session cleanup."""

    max_active_sessions_per_source: int | None = None
    idle_ttl_s: int | None = None

    def __post_init__(self) -> None:
        if self.max_active_sessions_per_source is not None and self.max_active_sessions_per_source < 1:
            raise ValueError("max_active_sessions_per_source must be positive")
        if self.idle_ttl_s is not None and self.idle_ttl_s < 0:
            raise ValueError("idle_ttl_s must be non-negative")


@dataclass(frozen=True, slots=True)
class CloseSessionResult:
    session_id: str
    backend_session_id: str | None
    closed: bool
    status: str | None = None
    reason: str | None = None
    error: str | None = None
    cancelled_mailbox_count: int = 0


@dataclass(frozen=True, slots=True)
class _CloseClaim:
    row: sqlite3.Row | None
    result: CloseSessionResult | None
    cancelled_mailbox_count: int = 0


async def close_session(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    session_backends: SessionBackendMapping,
    reason: str = "session closed by lifecycle policy",
    force: bool = False,
    now: int | None = None,
) -> CloseSessionResult:
    """Close one runtime session through its registered backend and mark it closed."""
    now = now if now is not None else _now()
    initial = conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    if initial is None:
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=None,
            closed=False,
            reason="session not found",
        )
    if initial["status"] == "closed":
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=initial["backend_session_id"],
            closed=False,
            status="closed",
            reason="session already closed",
        )
    backend = normalize_session_backends(session_backends).get(initial["backend"])
    if backend is None:
        error = f"no backend registered for {initial['backend']!r}"
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=initial["backend_session_id"],
            closed=False,
            status=initial["status"],
            error=error,
        )
    try:
        ensure_tenant_boundary(backend, initial["tenant_id"], resource_name=f"session backend {initial['backend']!r}")
    except TenantBoundaryError as exc:
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=initial["backend_session_id"],
            closed=False,
            status=initial["status"],
            error=str(exc),
        )

    claim = _claim_session_for_close(conn, session_id, force=force, now=now)
    if claim.result is not None:
        return claim.result
    assert claim.row is not None
    row = claim.row

    try:
        await backend.close(row["backend_session_id"])
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        log.exception("session close failed session_id=%s backend=%s", session_id, row["backend"])
        set_session_status(conn, session_id, "failed", last_error=error, now=now)
        record_session_event(
            conn,
            session_id=session_id,
            direction="control",
            payload_text=f"close failed: {error}",
            marker=f"session.lifecycle.close_failed:{now}",
            now=now,
        )
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=row["backend_session_id"],
            closed=False,
            status="failed",
            error=error,
            cancelled_mailbox_count=claim.cancelled_mailbox_count,
        )

    set_session_status(conn, session_id, "closed", now=now)
    record_session_event(
        conn,
        session_id=session_id,
        direction="control",
        payload_text=reason,
        marker=f"session.lifecycle.closed:{now}",
        now=now,
    )
    return CloseSessionResult(
        session_id=session_id,
        backend_session_id=row["backend_session_id"],
        closed=True,
        status="closed",
        reason=reason,
        cancelled_mailbox_count=claim.cancelled_mailbox_count,
    )


async def close_idle_sessions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    session_backends: SessionBackendMapping,
    policy: SessionLifecyclePolicy,
    source_id: str | None = None,
    now: int | None = None,
) -> list[CloseSessionResult]:
    """Close idle sessions selected by TTL and per-source active-session limits."""
    now = now if now is not None else _now()
    candidates: dict[str, str] = {}

    if policy.idle_ttl_s is not None:
        cutoff = now - policy.idle_ttl_s
        for row in _idle_ttl_candidates(conn, tenant_id=tenant_id, source_id=source_id, cutoff=cutoff):
            candidates[row["id"]] = f"idle session exceeded ttl of {policy.idle_ttl_s}s"

    if policy.max_active_sessions_per_source is not None:
        for row in _overflow_candidates(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            max_active=policy.max_active_sessions_per_source,
        ):
            candidates.setdefault(
                row["id"],
                f"source exceeded active session limit of {policy.max_active_sessions_per_source}",
            )

    results: list[CloseSessionResult] = []
    for candidate_id, reason in candidates.items():
        results.append(
            await close_session(
                conn,
                candidate_id,
                session_backends=session_backends,
                reason=reason,
                now=now,
            )
        )
    return results


def _claim_session_for_close(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    force: bool,
    now: int,
) -> _CloseClaim:
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=None,
                    closed=False,
                    reason="session not found",
                ),
            )
        if row["status"] == "closed":
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=row["backend_session_id"],
                    closed=False,
                    status="closed",
                    reason="session already closed",
                ),
            )
        if row["status"] == "closing":
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=row["backend_session_id"],
                    closed=False,
                    status="closing",
                    reason="session already closing",
                ),
            )

        queued_count, sending_count = _pending_mailbox_counts(conn, session_id)
        if sending_count:
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=row["backend_session_id"],
                    closed=False,
                    status=row["status"],
                    reason="session has sending mailbox items",
                ),
            )
        if queued_count and not force:
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=row["backend_session_id"],
                    closed=False,
                    status=row["status"],
                    reason="session has pending mailbox items",
                ),
            )

        allowed_statuses = {"idle", "failed"}
        if row["status"] not in allowed_statuses:
            conn.execute("COMMIT")
            return _CloseClaim(
                row=None,
                result=CloseSessionResult(
                    session_id=session_id,
                    backend_session_id=row["backend_session_id"],
                    closed=False,
                    status=row["status"],
                    reason=f"session status {row['status']!r} is not closable",
                ),
            )

        cancelled_count = 0
        if queued_count and force:
            cancelled_count = conn.execute(
                "UPDATE session_mailbox"
                " SET status = 'cancelled', last_error = ?, updated_at = ?"
                " WHERE session_id = ? AND status = 'queued'",
                ("session closed forcefully", now, session_id),
            ).rowcount
        conn.execute(
            "UPDATE runtime_sessions SET"
            " status = 'closing', current_action_id = NULL, last_error = NULL,"
            " updated_at = ?, last_used_at = ?"
            " WHERE id = ? AND status = ?",
            (now, now, session_id, row["status"]),
        )
        claimed = conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
        conn.execute("COMMIT")
        return _CloseClaim(row=claimed, result=None, cancelled_mailbox_count=cancelled_count)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _pending_mailbox_counts(conn: sqlite3.Connection, session_id: str) -> tuple[int, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM session_mailbox"
        " WHERE session_id = ? AND status IN ('queued','sending')"
        " GROUP BY status",
        (session_id,),
    ).fetchall()
    counts = {str(row["status"]): int(row["count"]) for row in rows}
    return counts.get("queued", 0), counts.get("sending", 0)


def _idle_ttl_candidates(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str | None,
    cutoff: int,
) -> list[sqlite3.Row]:
    params: list[object] = [tenant_id, cutoff]
    source_clause = ""
    if source_id is not None:
        source_clause = " AND source_id = ?"
        params.append(source_id)
    return list(
        conn.execute(
            "SELECT * FROM runtime_sessions"
            " WHERE tenant_id = ?"
            "   AND status = 'idle'"
            "   AND COALESCE(last_used_at, updated_at, created_at) <= ?"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM session_mailbox pending"
            "     WHERE pending.session_id = runtime_sessions.id"
            "       AND pending.status IN ('queued','sending')"
            "   )"
            f"{source_clause}"
            " ORDER BY COALESCE(last_used_at, updated_at, created_at) ASC, created_at ASC",
            params,
        )
    )


def _overflow_candidates(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str | None,
    max_active: int,
) -> list[sqlite3.Row]:
    params: list[object] = [tenant_id]
    source_clause = ""
    if source_id is not None:
        source_clause = " AND source_id = ?"
        params.append(source_id)
    rows = list(
        conn.execute(
            "SELECT runtime_sessions.*, EXISTS ("
            "     SELECT 1 FROM session_mailbox pending"
            "     WHERE pending.session_id = runtime_sessions.id"
            "       AND pending.status IN ('queued','sending')"
            "   ) AS has_pending_mailbox"
            " FROM runtime_sessions"
            " WHERE tenant_id = ?"
            "   AND status != 'closed'"
            f"{source_clause}"
            " ORDER BY source_id ASC, COALESCE(last_used_at, updated_at, created_at) DESC, created_at DESC",
            params,
        )
    )

    selected: list[sqlite3.Row] = []
    by_source: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_source.setdefault(row["source_id"], []).append(row)
    for source_rows in by_source.values():
        for overflow in source_rows[max_active:]:
            if overflow["status"] == "idle" and not overflow["has_pending_mailbox"]:
                selected.append(overflow)
    selected.sort(key=lambda row: (row["source_id"], row["last_used_at"] or row["updated_at"] or row["created_at"]))
    return selected
