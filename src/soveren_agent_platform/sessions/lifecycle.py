"""Lifecycle helpers for runtime execution sessions."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass

from soveren_agent_platform.sessions.backend import TenantBoundaryError, ensure_conversation_boundary
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.registry import SessionBackendMapping, normalize_session_backends
from soveren_agent_platform.sessions.store import set_session_status
from soveren_agent_platform.storage.adapter import SQLiteAdapter, SQLiteConnectionHandle
from soveren_agent_platform.storage.sqlite import run_sqlite

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


class SQLiteSessionLifecycle(SQLiteAdapter):
    """Manage persisted sessions without exposing the underlying SQLite connection."""

    def __init__(
        self,
        handle: SQLiteConnectionHandle,
        *,
        session_backends: SessionBackendMapping,
    ) -> None:
        super().__init__(handle)
        self._session_backends = normalize_session_backends(session_backends)

    async def close_session(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
        reason: str = "session closed by lifecycle policy",
        force: bool = False,
        now: int | None = None,
    ) -> CloseSessionResult:
        return await close_session(
            self._conn,
            session_id,
            tenant_id=tenant_id,
            source_id=source_id,
            session_backends=self._session_backends,
            reason=reason,
            force=force,
            now=now,
        )

    async def close_idle_sessions(
        self,
        *,
        tenant_id: str,
        policy: SessionLifecyclePolicy,
        source_id: str | None = None,
        now: int | None = None,
    ) -> list[CloseSessionResult]:
        return await close_idle_sessions(
            self._conn,
            tenant_id=tenant_id,
            session_backends=self._session_backends,
            policy=policy,
            source_id=source_id,
            now=now,
        )

    async def recover_stale_closing_sessions(
        self,
        *,
        tenant_id: str,
        older_than_s: int = 300,
        limit: int = 100,
        now: int | None = None,
    ) -> list[str]:
        return await recover_stale_closing_sessions(
            self._conn,
            tenant_id=tenant_id,
            older_than_s=older_than_s,
            limit=limit,
            now=now,
        )


@dataclass(frozen=True, slots=True)
class _CloseClaim:
    row: sqlite3.Row | None
    result: CloseSessionResult | None
    cancelled_mailbox_count: int = 0


async def close_session(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    tenant_id: str,
    source_id: str,
    session_backends: SessionBackendMapping,
    reason: str = "session closed by lifecycle policy",
    force: bool = False,
    now: int | None = None,
) -> CloseSessionResult:
    """Close one runtime session through its registered backend and mark it closed."""
    now = now if now is not None else _now()
    initial = await run_sqlite(
        conn,
        _get_session,
        session_id,
        tenant_id=tenant_id,
        source_id=source_id,
    )
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
        ensure_conversation_boundary(
            backend,
            initial["tenant_id"],
            initial["source_id"],
            resource_name=f"session backend {initial['backend']!r}",
        )
    except TenantBoundaryError as exc:
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=initial["backend_session_id"],
            closed=False,
            status=initial["status"],
            error=str(exc),
        )

    claim = await run_sqlite(
        conn,
        _claim_session_for_close,
        session_id,
        tenant_id=tenant_id,
        source_id=source_id,
        force=force,
        now=now,
    )
    if claim.result is not None:
        return claim.result
    assert claim.row is not None
    row = claim.row

    try:
        await backend.close(row["backend_session_id"])
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        log.exception("session close failed session_id=%s backend=%s", session_id, row["backend"])
        try:
            await run_sqlite(
                conn,
                _record_close_failure,
                session_id=session_id,
                error=error,
                now=now,
            )
        except BaseException as persistence_error:
            raise BaseExceptionGroup(
                "session close and failed-state persistence both failed",
                [exc, persistence_error],
            ) from None
        if not isinstance(exc, Exception):
            raise
        return CloseSessionResult(
            session_id=session_id,
            backend_session_id=row["backend_session_id"],
            closed=False,
            status="failed",
            error=error,
            cancelled_mailbox_count=claim.cancelled_mailbox_count,
        )

    await run_sqlite(
        conn,
        _record_closed,
        session_id=session_id,
        reason=reason,
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


async def recover_stale_closing_sessions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    older_than_s: int = 300,
    limit: int = 100,
    now: int | None = None,
) -> list[str]:
    """Mark abandoned close operations failed with an explicitly uncertain outcome."""
    return await run_sqlite(
        conn,
        _recover_stale_closing_sessions,
        tenant_id=tenant_id,
        older_than_s=older_than_s,
        limit=limit,
        now=now,
    )


def _recover_stale_closing_sessions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    older_than_s: int,
    limit: int,
    now: int | None,
) -> list[str]:
    if older_than_s < 0:
        raise ValueError("older_than_s must be non-negative")
    if limit < 1:
        raise ValueError("limit must be positive")
    now = now if now is not None else _now()
    cutoff = now - older_than_s
    error = "session close outcome is uncertain after lifecycle recovery"
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id FROM runtime_sessions"
            " WHERE tenant_id = ? AND status = 'closing' AND updated_at <= ?"
            " ORDER BY updated_at ASC, id ASC LIMIT ?",
            (tenant_id, cutoff, limit),
        ).fetchall()
        recovered: list[str] = []
        for row in rows:
            session_id = str(row["id"])
            updated = conn.execute(
                "UPDATE runtime_sessions"
                " SET status = 'failed', current_action_id = NULL, last_error = ?,"
                " updated_at = ?, last_used_at = ?"
                " WHERE id = ? AND tenant_id = ? AND status = 'closing'",
                (error, now, now, session_id, tenant_id),
            ).rowcount
            if updated != 1:
                continue
            record_session_event(
                conn,
                session_id=session_id,
                direction="control",
                payload_text=error,
                marker=f"session.lifecycle.close_uncertain:{now}",
                now=now,
            )
            recovered.append(session_id)
        conn.execute("COMMIT")
        return recovered
    except Exception:
        conn.execute("ROLLBACK")
        raise


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
    candidates = await run_sqlite(
        conn,
        _select_close_candidates,
        tenant_id=tenant_id,
        policy=policy,
        source_id=source_id,
        now=now,
    )

    results: list[CloseSessionResult] = []
    for candidate_id, (candidate_source_id, reason) in candidates.items():
        results.append(
            await close_session(
                conn,
                candidate_id,
                tenant_id=tenant_id,
                source_id=candidate_source_id,
                session_backends=session_backends,
                reason=reason,
                now=now,
            )
        )
    return results


def _get_session(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    tenant_id: str,
    source_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runtime_sessions WHERE id = ? AND tenant_id = ? AND source_id = ?",
        (session_id, tenant_id, source_id),
    ).fetchone()


def _record_close_failure(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    error: str,
    now: int,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        set_session_status(conn, session_id, "failed", last_error=error, now=now)
        record_session_event(
            conn,
            session_id=session_id,
            direction="control",
            payload_text=f"close failed: {error}",
            marker=f"session.lifecycle.close_failed:{now}",
            now=now,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _record_closed(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    reason: str,
    now: int,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        set_session_status(conn, session_id, "closed", now=now)
        record_session_event(
            conn,
            session_id=session_id,
            direction="control",
            payload_text=reason,
            marker=f"session.lifecycle.closed:{now}",
            now=now,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _select_close_candidates(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    policy: SessionLifecyclePolicy,
    source_id: str | None,
    now: int,
) -> dict[str, tuple[str, str]]:
    candidates: dict[str, tuple[str, str]] = {}
    if policy.idle_ttl_s is not None:
        cutoff = now - policy.idle_ttl_s
        for row in _idle_ttl_candidates(conn, tenant_id=tenant_id, source_id=source_id, cutoff=cutoff):
            candidates[row["id"]] = (
                row["source_id"],
                f"idle session exceeded ttl of {policy.idle_ttl_s}s",
            )
    if policy.max_active_sessions_per_source is not None:
        for row in _overflow_candidates(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            max_active=policy.max_active_sessions_per_source,
        ):
            candidates.setdefault(
                row["id"],
                (
                    row["source_id"],
                    f"source exceeded active session limit of {policy.max_active_sessions_per_source}",
                ),
            )
    return candidates


def _claim_session_for_close(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    tenant_id: str,
    source_id: str,
    force: bool,
    now: int,
) -> _CloseClaim:
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM runtime_sessions WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (session_id, tenant_id, source_id),
        ).fetchone()
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
            " WHERE id = ? AND tenant_id = ? AND source_id = ? AND status = ?",
            (now, now, session_id, tenant_id, source_id, row["status"]),
        )
        claimed = conn.execute(
            "SELECT * FROM runtime_sessions WHERE id = ? AND tenant_id = ? AND source_id = ?",
            (session_id, tenant_id, source_id),
        ).fetchone()
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
