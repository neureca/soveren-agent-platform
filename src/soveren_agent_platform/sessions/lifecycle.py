"""Lifecycle helpers for runtime execution sessions."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass

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


async def close_session(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    session_backends: SessionBackendMapping,
    reason: str = "session closed by lifecycle policy",
    now: int | None = None,
) -> CloseSessionResult:
    """Close one runtime session through its registered backend and mark it closed."""
    now = now if now is not None else _now()
    row = conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=None,
            closed=False,
            reason="session not found",
        )
    if row["status"] == "closed":
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=row["backend_session_id"],
            closed=False,
            status="closed",
            reason="session already closed",
        )

    backend = normalize_session_backends(session_backends).get(row["backend"])
    if backend is None:
        error = f"no backend registered for {row['backend']!r}"
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=row["backend_session_id"],
            closed=False,
            status=row["status"],
            error=error,
        )

    set_session_status(conn, session_id, "closing", now=now)
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
            "SELECT * FROM runtime_sessions"
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
            if overflow["status"] == "idle":
                selected.append(overflow)
    selected.sort(key=lambda row: (row["source_id"], row["last_used_at"] or row["updated_at"] or row["created_at"]))
    return selected
