import asyncio
import json

from agent_platform.sessions.backend import CaptureResult, OpenSpec, OpenResult
from agent_platform.sessions.mailbox import claim_next, enqueue_prompt, ready_session_ids
from agent_platform.sessions.mailbox_worker import drain_once
from agent_platform.sessions.store import insert_session, set_session_status
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


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

