import asyncio

import pytest

from soveren_agent_platform.sessions import CaptureResult, OpenResult, OpenSpec
from soveren_agent_platform.sessions.lifecycle import (
    SessionLifecyclePolicy,
    close_idle_sessions,
    close_session,
    recover_stale_closing_sessions,
)
from soveren_agent_platform.sessions.mailbox import claim_next, enqueue_prompt, mark_sent
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


class WrongTenantClosingBackend(ClosingBackend):
    tenant_id = "tenant-b"


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
            tenant_id="tenant-a",
            source_id="chat-1",
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
            tenant_id="tenant-a",
            source_id="chat-1",
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


def test_close_session_persists_failure_before_propagating_cancellation(tmp_path):
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

    class CancellingBackend(ClosingBackend):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()

        async def close(self, backend_session_id: str) -> None:
            self.started.set()
            await asyncio.Event().wait()

    async def run() -> None:
        backend = CancellingBackend()
        task = asyncio.create_task(
            close_session(
                conn,
                session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                session_backends={"fake": backend},
                now=200,
            )
        )
        await backend.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    row = conn.execute(
        "SELECT status, last_error FROM runtime_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["last_error"].startswith("CancelledError:")


def test_recover_stale_closing_sessions_is_tenant_scoped(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    stale = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        kind="codex_cli",
        backend="fake",
        backend_session_id="a",
        status="idle",
        now=100,
    )
    other_tenant = insert_session(
        conn,
        tenant_id="tenant-b",
        source_id="chat-b",
        kind="codex_cli",
        backend="fake",
        backend_session_id="b",
        status="idle",
        now=100,
    )
    conn.execute(
        "UPDATE runtime_sessions SET status = 'closing', updated_at = 100 WHERE id IN (?, ?)",
        (stale, other_tenant),
    )

    recovered = asyncio.run(
        recover_stale_closing_sessions(
            conn,
            tenant_id="tenant-a",
            older_than_s=300,
            now=500,
        )
    )

    statuses = {row["id"]: row["status"] for row in conn.execute("SELECT id, status FROM runtime_sessions")}
    assert recovered == [stale]
    assert statuses[stale] == "failed"
    assert statuses[other_tenant] == "closing"


def test_close_session_rejects_backend_bound_to_another_tenant(tmp_path):
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
    )
    backend = WrongTenantClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            session_backends={"fake": backend},
        )
    )

    row = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    assert result.closed is False
    assert "tenant-b" in (result.error or "")
    assert backend.closed == []
    assert row["status"] == "idle"


def test_close_session_does_not_reveal_or_close_another_tenants_session(tmp_path):
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
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-b",
            source_id="chat-1",
            session_backends={"fake": backend},
        )
    )

    row = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    assert result.closed is False
    assert result.reason == "session not found"
    assert result.backend_session_id is None
    assert backend.closed == []
    assert row["status"] == "idle"


def test_close_session_refuses_pending_mailbox_without_force(tmp_path):
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
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="do work",
        now=150,
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            session_backends={"fake": backend},
            now=200,
        )
    )

    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    item = conn.execute("SELECT status FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()

    assert result.closed is False
    assert result.reason == "session has pending mailbox items"
    assert backend.closed == []
    assert session["status"] == "idle"
    assert item["status"] == "queued"


def test_close_session_force_cancels_pending_mailbox(tmp_path):
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
    queued_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="queued work",
        now=150,
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            session_backends={"fake": backend},
            force=True,
            reason="forced close",
            now=200,
        )
    )

    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    items = {row["id"]: row["status"] for row in conn.execute("SELECT id, status FROM session_mailbox")}

    assert result.closed is True
    assert result.cancelled_mailbox_count == 1
    assert backend.closed == ["backend-1"]
    assert session["status"] == "closed"
    assert items[queued_id] == "cancelled"


def test_close_session_force_refuses_sending_mailbox(tmp_path):
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
    sending_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="sending work",
        now=151,
    )
    conn.execute(
        "UPDATE session_mailbox SET status = 'sending' WHERE id = ?",
        (sending_id,),
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            session_backends={"fake": backend},
            force=True,
            reason="forced close",
            now=200,
        )
    )

    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    item = conn.execute("SELECT status FROM session_mailbox WHERE id = ?", (sending_id,)).fetchone()

    assert result.closed is False
    assert result.reason == "session has sending mailbox items"
    assert backend.closed == []
    assert session["status"] == "idle"
    assert item["status"] == "sending"


def test_close_session_force_refuses_busy_session(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
        status="busy",
        now=100,
    )
    backend = ClosingBackend()

    result = asyncio.run(
        close_session(
            conn,
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            session_backends={"fake": backend},
            force=True,
            reason="forced close",
            now=200,
        )
    )

    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()

    assert result.closed is False
    assert result.reason == "session status 'busy' is not closable"
    assert backend.closed == []
    assert session["status"] == "busy"


def test_cancelled_mailbox_item_is_not_marked_sent_by_late_worker(tmp_path):
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
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="do work",
        now=150,
    )
    item = claim_next(conn, session_id, tenant_id="tenant-a", source_id="chat-1")
    assert item is not None
    conn.execute(
        "UPDATE session_mailbox SET status = 'cancelled' WHERE id = ?",
        (mailbox_id,),
    )

    mark_sent(
        conn,
        mailbox_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        result={"output": "late", "timed_out": False},
        now=200,
    )

    row = conn.execute("SELECT status, result_json FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()
    assert row["status"] == "cancelled"
    assert row["result_json"] is None


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


def test_close_idle_sessions_skips_sessions_with_pending_mailbox(tmp_path):
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
    queued_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-queued-idle",
        status="idle",
        now=90,
    )
    sending_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-sending-idle",
        status="idle",
        now=80,
    )
    queued_id, _ = enqueue_prompt(
        conn,
        session_id=queued_idle,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="queued work",
        now=150,
    )
    sending_id, _ = enqueue_prompt(
        conn,
        session_id=sending_idle,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="sending work",
        now=151,
    )
    conn.execute(
        "UPDATE session_mailbox SET status = 'sending' WHERE id = ?",
        (sending_id,),
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
    mailbox_statuses = {row["id"]: row["status"] for row in conn.execute("SELECT id, status FROM session_mailbox")}

    assert [result.session_id for result in results] == [old_idle]
    assert backend.closed == ["backend-old-idle"]
    assert statuses[old_idle] == "closed"
    assert statuses[queued_idle] == "idle"
    assert statuses[sending_idle] == "idle"
    assert mailbox_statuses[queued_id] == "queued"
    assert mailbox_statuses[sending_id] == "sending"


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


def test_close_idle_sessions_counts_pending_sessions_toward_per_source_limit(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    newest_pending = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-pending",
        status="idle",
        now=500,
    )
    kept_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-kept",
        status="idle",
        now=400,
    )
    closed_idle = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-closed",
        status="idle",
        now=300,
    )
    enqueue_prompt(
        conn,
        session_id=newest_pending,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="pending work",
        now=550,
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
    assert [result.session_id for result in results] == [closed_idle]
    assert backend.closed == ["backend-closed"]
    assert statuses[newest_pending] == "idle"
    assert statuses[kept_idle] == "idle"
    assert statuses[closed_idle] == "closed"


def test_session_lifecycle_policy_validates_limits():
    with pytest.raises(ValueError, match="positive"):
        SessionLifecyclePolicy(max_active_sessions_per_source=0)
    with pytest.raises(ValueError, match="non-negative"):
        SessionLifecyclePolicy(idle_ttl_s=-1)
