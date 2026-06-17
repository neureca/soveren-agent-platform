import asyncio

import pytest

from agent_platform.sessions import CaptureResult, OpenSpec, OpenResult, SessionBackendRegistry
from agent_platform.sessions.mailbox import enqueue_prompt
from agent_platform.sessions.mailbox_worker import drain_once
from agent_platform.sessions.store import insert_session
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


class RecordingBackend:
    name = "recording"

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


def test_session_backend_registry_registers_and_guards_duplicates():
    backend = RecordingBackend()
    registry = SessionBackendRegistry()
    registry.register("fake", backend)

    assert registry.get("fake") is backend
    assert registry.require("fake") is backend
    assert registry.names() == ("fake",)
    assert "fake" in registry
    assert registry.as_dict() == {"fake": backend}

    with pytest.raises(ValueError, match="already registered"):
        registry.register("fake", backend)
    with pytest.raises(KeyError, match="missing"):
        registry.require("missing")


def test_mailbox_worker_accepts_session_backend_registry(tmp_path):
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
    enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="hello",
        action_id="action-1",
    )
    backend = RecordingBackend()
    registry = SessionBackendRegistry({"fake": backend})

    processed = asyncio.run(
        drain_once(
            conn,
            tenant_id="tenant-a",
            session_backends=registry,
        )
    )

    assert processed == 1
    assert backend.sent == [("backend-1", "hello")]

