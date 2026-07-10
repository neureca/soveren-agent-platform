import asyncio

import pytest

from soveren_agent_platform.sessions import (
    CaptureResult,
    OpenResult,
    OpenSpec,
    SessionOpenRequest,
    SessionRuntime,
    SQLiteSessionStore,
)
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class OpeningBackend:
    name = "codex"

    def __init__(self) -> None:
        self.closed: list[str] = []

    async def open(self, spec: OpenSpec) -> OpenResult:
        return OpenResult(
            backend_session_id="thread-1",
            session_handle="thread-1",
            metadata={"sandbox_cwd": "/workspace/chat-1"},
        )

    async def send(self, backend_session_id: str, prompt: str) -> None:
        return None

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return CaptureResult(text="", timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        self.closed.append(backend_session_id)


def test_session_runtime_opens_backend_and_persists_generalized_session(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    backend = OpeningBackend()
    runtime = SessionRuntime(SQLiteSessionStore(conn), {backend.name: backend})

    result = asyncio.run(runtime.open_session(SessionOpenRequest(
        tenant_id="tenant-a",
        source_id="chat-1",
        owner_id="user-1",
        kind="codex_cli",
        backend="codex",
        cwd="/ignored-host-path",
        title="Primary chat",
    )))

    row = conn.execute("SELECT * FROM runtime_sessions WHERE id = ?", (result.session_id,)).fetchone()
    assert result.backend_session_id == "thread-1"
    assert row["tenant_id"] == "tenant-a"
    assert row["source_id"] == "chat-1"
    assert row["owner_id"] == "user-1"
    assert row["backend"] == "codex"
    assert row["cwd"] == "/workspace/chat-1"


def test_session_runtime_closes_backend_when_persistence_fails():
    class FailingStore:
        async def create(self, **kwargs):
            raise RuntimeError("database unavailable")

    backend = OpeningBackend()
    runtime = SessionRuntime(FailingStore(), {backend.name: backend})

    with pytest.raises(RuntimeError, match="database unavailable"):
        asyncio.run(runtime.open_session(SessionOpenRequest(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex",
            cwd="/workspace",
        )))

    assert backend.closed == ["thread-1"]
