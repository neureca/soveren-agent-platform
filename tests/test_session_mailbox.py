import asyncio
import json

import pytest

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec, SendReceipt
from soveren_agent_platform.sessions.contracts import MailboxItem, RuntimeSession, RuntimeSessionEvent
from soveren_agent_platform.sessions.mailbox import claim_next, enqueue_prompt, ready_session_ids
from soveren_agent_platform.sessions.mailbox_worker import drain_once, drain_store_once
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class RecordingBackend:
    name = "fake"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def open(self, spec: OpenSpec) -> OpenResult:
        return OpenResult(backend_session_id="backend-1")

    async def send(self, backend_session_id: str, prompt: str) -> None:
        self.sent.append((backend_session_id, prompt))

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return CaptureResult(text="ok", timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        return None


def test_mailbox_claims_only_idle_sessions(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    idle_session = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-idle",
        status="idle",
        now=100,
    )
    busy_session = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-busy",
        status="busy",
        now=100,
    )
    enqueue_prompt(
        conn,
        session_id=idle_session,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="idle prompt",
        action_id="action-1",
        now=101,
    )
    enqueue_prompt(
        conn,
        session_id=busy_session,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="busy prompt",
        action_id="action-2",
        now=101,
    )

    assert ready_session_ids(conn, tenant_id="tenant-a", limit=10) == [idle_session]
    item = claim_next(conn, idle_session)

    assert item is not None
    assert item["prompt"] == "idle prompt"
    assert item["status"] == "sending"


def test_mailbox_enqueue_is_idempotent_by_action_id(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
        now=100,
    )

    first = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="hello",
        action_id="action-1",
    )
    second = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="hello again",
        action_id="action-1",
    )

    assert first[0] == second[0]
    assert first[1] is True
    assert second[1] is False


@pytest.mark.parametrize("status", ["starting", "closing", "closed", "failed"])
def test_mailbox_enqueue_rejects_non_routable_session_status(tmp_path, status):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
        status=status,
        now=100,
    )

    with pytest.raises(RuntimeError, match="does not accept mailbox prompts"):
        enqueue_prompt(
            conn,
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            prompt="hello",
            action_id="action-1",
        )

    item = conn.execute("SELECT * FROM session_mailbox WHERE session_id = ?", (session_id,)).fetchone()
    assert item is None


def test_mailbox_worker_sends_prompt_and_returns_session_to_idle(tmp_path):
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
        action_id="action-1",
        now=101,
    )
    backend = RecordingBackend()

    processed = asyncio.run(
        drain_once(
            conn,
            tenant_id="tenant-a",
            session_backends={"fake": backend},
        )
    )

    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    item = conn.execute("SELECT status, result_json FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()

    assert processed == 1
    assert backend.sent == [("backend-1", "do work")]
    assert session["status"] == "idle"
    assert item["status"] == "sent"
    assert json.loads(item["result_json"]) == {"output": "ok", "timed_out": False}


class FakeSessionStore:
    def __init__(self) -> None:
        self.session = RuntimeSession(
            id="rs_1",
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="fake",
            backend_session_id="backend-1",
            status="idle",
        )
        self.statuses: list[tuple[str, str, str | None]] = []

    async def get(self, session_id: str):
        return self.session if session_id == self.session.id else None

    async def set_status(self, session_id: str, status: str, *, current_action_id=None, last_error=None):
        self.session.status = status
        self.session.current_action_id = current_action_id
        self.session.last_error = last_error
        self.statuses.append((session_id, status, current_action_id))


class FakeMailboxStore:
    def __init__(self, session_store: FakeSessionStore) -> None:
        self.session_store = session_store
        self.item = MailboxItem(
            id="sm_1",
            session_id="rs_1",
            tenant_id="tenant-a",
            source_id="chat-1",
            prompt="do work",
            status="queued",
            action_id="action-1",
        )
        self.sent: list[tuple[str, dict]] = []
        self.failed: list[tuple[str, str]] = []

    async def enqueue_prompt(self, **kwargs):
        return self.item.id, True

    async def ready_session_ids(self, *, tenant_id: str, limit: int):
        return [self.item.session_id] if self.item.status == "queued" else []

    async def claim_next(self, session_id: str):
        if session_id != self.item.session_id or self.item.status != "queued":
            return None
        self.item.status = "sending"
        return self.item

    async def mark_sent(self, mailbox_id: str, *, result=None):
        self.item.status = "sent"
        self.sent.append((mailbox_id, result or {}))

    async def mark_accepted(self, mailbox_id: str, *, backend_receipt=None):
        self.item.accepted_at = 1

    async def complete_delivery(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        result: dict,
        session_status: str,
        current_action_id=None,
    ):
        await self.mark_sent(mailbox_id, result=result)
        await self.session_store.set_status(
            session_id,
            session_status,
            current_action_id=current_action_id,
        )

    async def fail_delivery(self, mailbox_id: str, *, session_id: str, last_error: str):
        await self.mark_failed(mailbox_id, last_error=last_error)
        await self.session_store.set_status(session_id, "failed", last_error=last_error)

    async def defer_accepted(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        current_action_id,
        last_error: str,
        retry_after_s: int,
    ):
        self.item.attempts += 1
        terminal = self.item.attempts >= self.item.max_attempts
        if terminal:
            self.item.status = "failed"
        await self.session_store.set_status(
            session_id,
            "failed" if terminal else "busy",
            current_action_id=None if terminal else current_action_id,
            last_error=last_error,
        )
        return terminal

    async def defer_pending(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        current_action_id,
        last_error: str,
        retry_after_s: int,
    ):
        await self.session_store.set_status(
            session_id,
            "busy",
            current_action_id=current_action_id,
            last_error=last_error,
        )

    async def requeue(self, mailbox_id: str, *, last_error: str):
        self.item.status = "queued"

    async def mark_failed(self, mailbox_id: str, *, last_error: str):
        self.item.status = "failed"
        self.failed.append((mailbox_id, last_error))

    async def fail_stale_sending(self, *, tenant_id: str, older_than_s: int, reason: str, limit: int):
        return []


class FakeSessionEventStore:
    def __init__(self) -> None:
        self.events: list[RuntimeSessionEvent] = []

    async def record(self, *, session_id: str, direction: str, payload_text: str, action_id=None, marker=None):
        event = RuntimeSessionEvent(
            id=f"rse_{len(self.events) + 1}",
            session_id=session_id,
            direction=direction,
            payload_text=payload_text,
            action_id=action_id,
            marker=marker,
        )
        self.events.append(event)
        return event.id

    async def recent(self, session_id: str, *, limit: int):
        return [event for event in self.events if event.session_id == session_id][-limit:]


class FakeSessionSnapshotStore:
    def __init__(self) -> None:
        self.refreshed: list[str] = []

    async def refresh(self, session_id: str):
        self.refreshed.append(session_id)
        return "rss_1"

    async def latest(self, session_id: str):
        return None


def test_mailbox_drain_uses_session_and_mailbox_ports():
    session_store = FakeSessionStore()
    mailbox_store = FakeMailboxStore(session_store)
    backend = RecordingBackend()

    processed = asyncio.run(
        drain_store_once(
            session_store,
            mailbox_store,
            tenant_id="tenant-a",
            session_backends={"fake": backend},
        )
    )

    assert processed == 1
    assert backend.sent == [("backend-1", "do work")]
    assert mailbox_store.sent == [("sm_1", {"output": "ok", "timed_out": False})]
    assert session_store.statuses == [
        ("rs_1", "busy", "action-1"),
        ("rs_1", "idle", None),
    ]


def test_mailbox_drain_records_session_events_and_refreshes_snapshot():
    session_store = FakeSessionStore()
    mailbox_store = FakeMailboxStore(session_store)
    event_store = FakeSessionEventStore()
    snapshot_store = FakeSessionSnapshotStore()
    backend = RecordingBackend()

    processed = asyncio.run(
        drain_store_once(
            session_store,
            mailbox_store,
            tenant_id="tenant-a",
            session_backends={"fake": backend},
            event_store=event_store,
            snapshot_store=snapshot_store,
        )
    )

    assert processed == 1
    assert [(event.direction, event.payload_text, event.marker) for event in event_store.events] == [
        ("input", "do work", "mailbox:sm_1:input"),
        ("output", "ok", "mailbox:sm_1:output"),
    ]
    assert snapshot_store.refreshed == ["rs_1"]


def test_mailbox_does_not_resend_after_capture_failure(tmp_path):
    class CaptureRetryBackend(RecordingBackend):
        def __init__(self):
            super().__init__()
            self.capture_calls = 0

        async def capture(self, backend_session_id: str) -> CaptureResult:
            self.capture_calls += 1
            if self.capture_calls == 1:
                raise RuntimeError("transport interrupted after acceptance")
            return CaptureResult(text="recovered", timed_out=False)

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="do once",
    )
    backend = CaptureRetryBackend()

    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    conn.execute("UPDATE session_mailbox SET run_after = 0 WHERE id = ?", (mailbox_id,))
    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))

    item = conn.execute(
        "SELECT status, accepted_at, attempts FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    assert backend.sent == [("backend-1", "do once")]
    assert backend.capture_calls == 2
    assert item["status"] == "sent"
    assert item["accepted_at"] is not None
    assert item["attempts"] == 2


def test_mailbox_uses_persisted_receipt_to_recover_exact_delivery(tmp_path):
    class ReceiptRecoveryBackend(RecordingBackend):
        def __init__(self):
            super().__init__()
            self.capture_receipts: list[str | None] = []

        async def send(self, backend_session_id: str, prompt: str) -> SendReceipt:
            await super().send(backend_session_id, prompt)
            return SendReceipt(backend_operation_id="turn_exact")

        async def capture(self, backend_session_id: str) -> CaptureResult:
            raise AssertionError("receipt-aware backend must not use generic capture")

        async def capture_delivery(
            self,
            backend_session_id: str,
            receipt: SendReceipt,
        ) -> CaptureResult:
            self.capture_receipts.append(receipt.backend_operation_id)
            if len(self.capture_receipts) == 1:
                raise RuntimeError("app-server restarted")
            return CaptureResult(text="exact recovered output", timed_out=False)

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="thread-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="do exactly once",
    )
    backend = ReceiptRecoveryBackend()

    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    conn.execute("UPDATE session_mailbox SET run_after = 0 WHERE id = ?", (mailbox_id,))
    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))

    item = conn.execute(
        "SELECT status, backend_receipt_json, result_json FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    assert backend.sent == [("thread-1", "do exactly once")]
    assert backend.capture_receipts == ["turn_exact", "turn_exact"]
    assert json.loads(item["backend_receipt_json"])["backend_operation_id"] == "turn_exact"
    assert json.loads(item["result_json"])["output"] == "exact recovered output"
    assert item["status"] == "sent"


def test_mailbox_timeout_keeps_accepted_delivery_pending_without_resend(tmp_path):
    class SlowCaptureBackend(RecordingBackend):
        def __init__(self):
            super().__init__()
            self.capture_calls = 0

        async def capture(self, backend_session_id: str) -> CaptureResult:
            self.capture_calls += 1
            if self.capture_calls <= 2:
                return CaptureResult(text="partial", timed_out=True)
            return CaptureResult(text="complete", timed_out=False)

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="long turn",
    )
    backend = SlowCaptureBackend()

    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    pending = conn.execute(
        "SELECT status, accepted_at FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    conn.execute("UPDATE session_mailbox SET run_after = 0 WHERE id = ?", (mailbox_id,))
    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    conn.execute("UPDATE session_mailbox SET run_after = 0 WHERE id = ?", (mailbox_id,))
    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))

    completed = conn.execute(
        "SELECT status, attempts, result_json FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    assert pending["status"] == "sending"
    assert pending["accepted_at"] is not None
    assert backend.sent == [("backend-1", "long turn")]
    assert backend.capture_calls == 3
    assert completed["status"] == "sent"
    assert completed["attempts"] == 1
    assert json.loads(completed["result_json"])["output"] == "complete"
    assert session["status"] == "idle"


def test_mailbox_pending_delivery_fails_only_after_absolute_deadline(tmp_path):
    class PendingBackend(RecordingBackend):
        async def capture(self, backend_session_id: str) -> CaptureResult:
            return CaptureResult(text="partial", timed_out=True)

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="long-running",
    )
    backend = PendingBackend()

    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    conn.execute(
        "UPDATE session_mailbox SET accepted_at = 1, run_after = 0 WHERE id = ?",
        (mailbox_id,),
    )
    asyncio.run(drain_once(
        conn,
        tenant_id="tenant-a",
        session_backends={"fake": backend},
        capture_pending_timeout_s=1,
    ))

    item = conn.execute(
        "SELECT status, attempts, last_error FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    assert item["status"] == "failed"
    assert item["attempts"] == 1
    assert "deadline exceeded" in item["last_error"]


def test_mailbox_rejects_backend_bound_to_another_tenant(tmp_path):
    class WrongTenantBackend(RecordingBackend):
        tenant_id = "tenant-b"

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="must not cross tenants",
    )
    backend = WrongTenantBackend()

    asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))

    item = conn.execute("SELECT status, last_error FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()
    assert backend.sent == []
    assert item["status"] == "failed"
    assert "tenant-b" in item["last_error"]


def test_mailbox_send_failure_is_terminal_and_not_requeued(tmp_path):
    class FailingSendBackend(RecordingBackend):
        async def send(self, backend_session_id: str, prompt: str) -> None:
            self.sent.append((backend_session_id, prompt))
            raise RuntimeError("sandbox configuration failed")

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="do not duplicate",
    )
    backend = FailingSendBackend()

    first = asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))
    second = asyncio.run(drain_once(conn, tenant_id="tenant-a", session_backends={"fake": backend}))

    item = conn.execute("SELECT status, last_error FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()
    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    assert (first, second) == (1, 0)
    assert backend.sent == [("backend-1", "do not duplicate")]
    assert item["status"] == "failed"
    assert "automatic resend disabled" in item["last_error"]
    assert session["status"] == "failed"


def test_stale_unaccepted_delivery_fails_session_instead_of_leaving_it_busy(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="interrupted",
    )
    assert claim_next(conn, session_id) is not None
    conn.execute("UPDATE runtime_sessions SET status = 'busy' WHERE id = ?", (session_id,))
    conn.execute("UPDATE session_mailbox SET updated_at = 1 WHERE id = ?", (mailbox_id,))

    asyncio.run(
        drain_once(
            conn,
            tenant_id="tenant-a",
            session_backends={"fake": RecordingBackend()},
            stale_sending_s=1,
        )
    )

    item = conn.execute("SELECT status, result_json FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()
    session = conn.execute("SELECT status FROM runtime_sessions WHERE id = ?", (session_id,)).fetchone()
    assert item["status"] == "failed"
    assert json.loads(item["result_json"]) == {"delivery": "uncertain"}
    assert session["status"] == "failed"
