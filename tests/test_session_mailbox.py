import asyncio
import json

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec
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
    def __init__(self) -> None:
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
    mailbox_store = FakeMailboxStore()
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
    mailbox_store = FakeMailboxStore()
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
