import asyncio

import pytest

from soveren_agent_platform.sessions import CaptureResult, OpenResult, OpenSpec
from soveren_agent_platform.sessions.lifecycle import (
    SessionLifecyclePolicy,
    close_idle_sessions,
    close_session,
)
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class ClosingBackend:
    name = "fake"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.closed: list[str] = []

    async def open(self, spec: OpenSpec) -> OpenResult:
        return OpenResult(backend_session_id="backend-1")

    async def send(self, backend_session_id: str, prompt: str) -> None:
        return None

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return CaptureResult(text="", timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        if self.fail:
            raise RuntimeError("close failed")
        self.closed.append(backend_session_id)


def test_close_session_marks_closed_and_records_control_event(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
        status="idle",
        now=100,
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            session_backends={"fake": backend},
            reason="manual close",
            now=200,
        )
    )

    row = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    event = conn.execute(
        "SELECT direction, payload_text, marker FROM runtime_session_events WHERE session_id = ?",
        (session_id,),
    ).fetchone()

    assert result.closed is True
    assert result.status == "closed"
    assert backend.closed == ["backend-1"]
    assert row["status"] == "closed"
    assert event["direction"] == "control"
    assert event["payload_text"] == "manual close"
    assert event["marker"] == "session.lifecycle.closed:200"


def test_close_session_marks_failed_when_backend_close_fails(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
        status="idle",
        now=100,
    )

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            session_backends={"fake": ClosingBackend(fail=True)},
            now=200,
        )
    )

    row = conn.execute("SELECT status, last_error FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    event = conn.execute(
        "SELECT payload_text FROM runtime_session_events WHERE session_id = ?",
        (session_id,),
    ).fetchone()

    assert result.closed is False
    assert result.status == "failed"
    assert result.error == "RuntimeError: close failed"
    assert row["status"] == "failed"
    assert row["last_error"] == "RuntimeError: close failed"
    assert event["payload_text"] == "close failed: RuntimeError: close failed"


def test_close_idle_sessions_applies_ttl_without_closing_busy_sessions(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    old_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-old-idle",
        status="idle",
        now=100,
    )
    new_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-new-idle",
        status="idle",
        now=250,
    )
    old_busy = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-old-busy",
        status="busy",
        now=100,
    )
    backend = ClosingBackend()

    results = asyncio.run(
        close_idle_sessions(
            conn,
            tenant_id="tenant-a",
            session_backends={"fake": backend},
            policy=SessionLifecyclePolicy(idle_ttl_s=100),
            now=250,
        )
    )

    statuses = {row["id"]: row["status"] for row in conn.execute("SELECT id, status FROM runtime_sessions")}
    assert [result.session_id for result in results] == [old_idle]
    assert backend.closed == ["backend-old-idle"]
    assert statuses[old_idle] == "closed"
    assert statuses[new_idle] == "idle"
    assert statuses[old_busy] == "busy"


def test_close_idle_sessions_enforces_per_source_limit_by_closing_oldest_idle(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    newest_busy = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-busy",
        status="busy",
        now=500,
    )
    newest_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-new-idle",
        status="idle",
        now=400,
    )
    old_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-old-idle",
        status="idle",
        now=300,
    )
    oldest_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-oldest-idle",
        status="idle",
        now=200,
    )
    other_source = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-2",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-other",
        status="idle",
        now=100,
    )
    backend = ClosingBackend()

    results = asyncio.run(
        close_idle_sessions(
            conn,
            tenant_id="tenant-a",
            session_backends={"fake": backend},
            policy=SessionLifecyclePolicy(max_active_sessions_per_source=2),
            now=600,
        )
    )

    statuses = {row["id"]: row["status"] for row in conn.execute("SELECT id, status FROM runtime_sessions")}
    assert [result.session_id for result in results] == [oldest_idle, old_idle]
    assert backend.closed == ["backend-oldest-idle", "backend-old-idle"]
    assert statuses[newest_busy] == "busy"
    assert statuses[newest_idle] == "idle"
    assert statuses[oldest_idle] == "closed"
    assert statuses[old_idle] == "closed"
    assert statuses[other_source] == "idle"


def test_session_lifecycle_policy_validates_limits():
    with pytest.raises(ValueError, match="positive"):
        SessionLifecyclePolicy(max_active_sessions_per_source=0)
    with pytest.raises(ValueError, match="non-negative"):
        SessionLifecyclePolicy(idle_ttl_s=-1)
