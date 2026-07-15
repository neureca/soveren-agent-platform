import asyncio
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.sessions import (
    CaptureResult,
    CodexAppServerBackend,
    CodexAppServerError,
    CodexCollaborationMode,
    CodexThreadInspector,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    OpenSpec,
    SendReceipt,
)
from soveren_agent_platform.sessions.backends import codex_app_server as codex_app_server_module
from soveren_agent_platform.sessions.backends.codex_app_server import (
    JsonRpcStdioClient,
    TurnState,
    extract_thread_text,
    parse_codex_version,
)


def test_parse_codex_app_server_version():
    assert parse_codex_version("Codex Desktop/0.130.0-alpha.5 (Mac OS)") == (0, 130, 0)
    assert parse_codex_version("Codex CLI/1.2.3") == (1, 2, 3)
    assert parse_codex_version("no-version") is None


def test_codex_collaboration_mode_validates_provider_contract():
    mode = CodexCollaborationMode(
        mode="plan",
        model=" gpt-5.4 ",
        reasoning_effort=" high ",
        developer_instructions="Plan before changing files.",
    )

    assert mode.app_server_payload() == {
        "mode": "plan",
        "settings": {
            "model": "gpt-5.4",
            "reasoning_effort": "high",
            "developer_instructions": "Plan before changing files.",
        },
    }
    with pytest.raises(ValueError, match="mode must be"):
        CodexCollaborationMode(mode="autonomous", model="gpt-5.4")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="model must be non-empty"):
        CodexCollaborationMode(mode="default", model=" ")
    with pytest.raises(ValueError, match="reasoning_effort must be non-empty"):
        CodexCollaborationMode(mode="default", model="gpt-5.4", reasoning_effort=" ")
    with pytest.raises(TypeError, match="model must be a string"):
        CodexCollaborationMode(mode="default", model=1)  # type: ignore[arg-type]


def test_codex_backend_rejects_untyped_collaboration_mode():
    with pytest.raises(TypeError, match="CodexCollaborationMode"):
        CodexAppServerBackend(collaboration_mode="autonomous")  # type: ignore[arg-type]


def test_codex_backend_env_filters_product_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENAI_API_KEY", "should-not-leak")
    monkeypatch.setenv("CODEX_API_KEY", "should-not-leak")
    monkeypatch.setenv("TG_BOT_TOKEN", "should-not-leak")
    monkeypatch.setenv("CLICKUP_API_TOKEN", "should-not-leak")
    monkeypatch.setenv("OPENROUTER_API_KEY", "should-not-leak")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy")
    backend = CodexAppServerBackend(codex_home=tmp_path)

    env = backend.env()

    assert env["CODEX_HOME"] == str(tmp_path)
    assert env["HTTPS_PROXY"] == "http://proxy"
    assert "OPENAI_API_KEY" not in env
    assert "CODEX_API_KEY" not in env
    assert "TG_BOT_TOKEN" not in env
    assert "CLICKUP_API_TOKEN" not in env
    assert "OPENROUTER_API_KEY" not in env


def test_extract_thread_text_only_returns_agent_messages():
    payload = {
        "thread": {
            "items": [
                {"role": "user", "text": "ignore me"},
                {"role": "assistant", "text": "answer one"},
                {"type": "agent_message", "content": "answer two"},
            ]
        }
    }

    assert extract_thread_text(payload) == "answer one\nanswer two"


class FakeCodexClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.last_turns: dict[str, object] = {}
        self.released_turns: list[tuple[str, str]] = []
        self.released_threads: list[str] = []
        self.closed = False

    async def request(self, method: str, params: dict):
        self.calls.append((method, params))
        if method == "initialize":
            return {"userAgent": "Codex CLI/0.130.0"}
        if method == "thread/start":
            return {
                "thread": {"id": "thread_new"},
                "model": params.get("model"),
                "modelProvider": "openai",
                "cwd": params.get("cwd"),
            }
        if method == "turn/start":
            return {"turn": {"id": "turn_1"}}
        if method == "thread/read":
            return {"thread": {"items": [{"role": "assistant", "text": "restored answer"}]}}
        return {}

    async def close(self) -> None:
        self.closed = True

    def set_last_turn(self, thread_id: str, turn_id: str):
        state = SimpleNamespace(turn_id=turn_id)
        self.last_turns[thread_id] = state
        return state

    def last_turn(self, thread_id: str):
        return self.last_turns.get(thread_id)

    def release_turn(self, thread_id: str, turn_id: str) -> None:
        self.released_turns.append((thread_id, turn_id))
        state = self.last_turns.get(thread_id)
        if state is not None and getattr(state, "turn_id", None) == turn_id:
            self.last_turns.pop(thread_id, None)

    def release_thread(self, thread_id: str) -> None:
        self.released_threads.append(thread_id)
        self.last_turns.pop(thread_id, None)


def test_codex_backend_open_initializes_and_starts_thread(tmp_path):
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake, model="gpt-5.4")
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))
        return fake, opened

    fake, opened = asyncio.run(run())

    assert opened.backend_session_id == "thread_new"
    assert opened.metadata["model"] == "gpt-5.4"
    assert fake.calls == [
        (
            "thread/start",
            {
                "cwd": str(tmp_path / "work"),
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
                "ephemeral": False,
                "threadSource": "user",
                "sessionStartSource": "startup",
                "model": "gpt-5.4",
            },
        )
    ]


def test_codex_backend_single_flights_initialization(monkeypatch):
    clients: list[FakeCodexClient] = []

    class SlowFakeCodexClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method == "initialize":
                await asyncio.sleep(0)
            return await super().request(method, params)

    def create_client(**kwargs):
        client = SlowFakeCodexClient()
        clients.append(client)
        return client

    monkeypatch.setattr(codex_app_server_module, "JsonRpcStdioClient", create_client)

    async def run():
        backend = CodexAppServerBackend()
        await asyncio.gather(*(backend.ensure_initialized() for _ in range(10)))
        await backend.shutdown()

    asyncio.run(run())

    assert len(clients) == 1
    assert [method for method, _ in clients[0].calls] == ["initialize"]
    assert clients[0].closed is True


def test_codex_backend_open_registers_dynamic_tools_and_turn_options(tmp_path):
    async def run():
        fake = FakeCodexClient()
        registry = DynamicToolRegistry()
        registry.register(
            DynamicToolSpec(
                name="list_sessions",
                description="List active sessions",
                input_schema={"type": "object", "properties": {}},
                namespace="platform",
            ),
            lambda call: DynamicToolResult.json({"ok": True}),
        )
        backend = CodexAppServerBackend(
            client=fake,
            dynamic_tools=registry,
            developer_instructions="Use platform tools as read-only helpers.",
            output_schema={"type": "object"},
            collaboration_mode=CodexCollaborationMode(
                mode="default",
                model="gpt-5.4",
                reasoning_effort="high",
            ),
        )
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))
        await backend.send(opened.backend_session_id, "hello")
        return fake

    fake = asyncio.run(run())

    assert fake.calls[0][0] == "thread/start"
    assert fake.calls[0][1]["developerInstructions"] == "Use platform tools as read-only helpers."
    assert fake.calls[0][1]["dynamicTools"] == [
        {
            "name": "list_sessions",
            "description": "List active sessions",
            "inputSchema": {"type": "object", "properties": {}},
            "namespace": "platform",
        }
    ]
    assert fake.calls[1] == (
        "turn/start",
        {
            "threadId": "thread_new",
            "input": [{"type": "text", "text": "hello"}],
            "outputSchema": {"type": "object"},
            "collaborationMode": {
                "mode": "default",
                "settings": {"model": "gpt-5.4", "reasoning_effort": "high"},
            },
        },
    )


def test_codex_backend_rejects_non_codex_kind(tmp_path):
    async def run():
        await CodexAppServerBackend(client=FakeCodexClient()).open(
            OpenSpec(kind="claude_cli", cwd=str(tmp_path))
        )

    with pytest.raises(CodexAppServerError, match="cannot open"):
        asyncio.run(run())


def test_codex_backend_rejects_dynamic_tool_specs_without_handlers():
    with pytest.raises(ValueError, match="without handlers"):
        CodexAppServerBackend(dynamic_tools=[{
            "name": "unsafe_stub",
            "description": "No handler",
            "inputSchema": {"type": "object"},
        }])


def test_codex_backend_resumes_persisted_thread_before_send():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(
            client=fake,
            model="gpt-5.4",
            developer_instructions="Use current policy.",
        )
        await backend.send("thread_existing", "hello")
        return fake

    fake = asyncio.run(run())

    assert fake.calls[0] == (
        "thread/resume",
        {
            "threadId": "thread_existing",
            "approvalPolicy": "never",
            "sandbox": "workspace-write",
            "model": "gpt-5.4",
            "developerInstructions": "Use current policy.",
        },
    )
    assert fake.calls[1] == (
        "turn/start",
        {"threadId": "thread_existing", "input": [{"type": "text", "text": "hello"}]},
    )


def test_codex_backend_capture_after_restart_reads_thread_history():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture("thread_existing")
        return fake, result

    fake, result = asyncio.run(run())

    assert isinstance(result, CaptureResult)
    assert result.text == "restored answer"
    assert fake.calls == [
        (
            "thread/resume",
            {
                "threadId": "thread_existing",
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            },
        ),
        ("thread/read", {"threadId": "thread_existing", "includeTurns": True}),
    ]


def test_codex_backend_recovers_exact_accepted_turn_after_restart():
    class RestartedClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/read":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "thread": {
                    "turns": [
                        {
                            "id": "turn_old",
                            "status": "completed",
                            "items": [{"type": "agentMessage", "text": "old answer"}],
                        },
                        {
                            "id": "turn_expected",
                            "status": "completed",
                            "items": [{"type": "agentMessage", "text": "expected answer"}],
                        },
                    ]
                }
            }

    async def run():
        fake = RestartedClient()
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_expected"),
        )
        return fake, result

    fake, result = asyncio.run(run())

    assert result == CaptureResult(text="expected answer", timed_out=False)
    assert fake.calls[-1] == (
        "thread/read",
        {"threadId": "thread_existing", "includeTurns": True},
    )
    assert fake.released_turns == [("thread_existing", "turn_expected")]


@pytest.mark.parametrize("status", ["inProgress", None])
def test_codex_backend_keeps_unfinished_or_not_yet_visible_turn_pending(status):
    class PendingClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/read":
                return await super().request(method, params)
            self.calls.append((method, params))
            turns = [] if status is None else [{
                "id": "turn_expected",
                "status": status,
                "items": [{"type": "agentMessage", "text": "partial"}],
            }]
            return {"thread": {"turns": turns}}

    async def run():
        backend = CodexAppServerBackend(client=PendingClient())
        return await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_expected"),
        )

    result = asyncio.run(run())

    assert result.timed_out is True
    assert result.text == ("partial" if status else "")


def test_codex_backend_releases_recovered_terminal_turn_without_removing_newer_turn():
    class RecoveredClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/read":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "thread": {
                    "turns": [
                        {
                            "id": "turn_old",
                            "status": "completed",
                            "items": [{"type": "agentMessage", "text": "old answer"}],
                        }
                    ]
                }
            }

    async def run():
        fake = RecoveredClient()
        newer = TurnState(turn_id="turn_new")
        fake.last_turns["thread_existing"] = newer
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_old"),
        )
        return fake, newer, result

    fake, newer, result = asyncio.run(run())

    assert result == CaptureResult(text="old answer", timed_out=False)
    assert fake.released_turns == [("thread_existing", "turn_old")]
    assert fake.last_turns["thread_existing"] is newer


def test_codex_backend_surfaces_failed_accepted_turn():
    class FailedClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/read":
                return await super().request(method, params)
            return {
                "thread": {
                    "turns": [{
                        "id": "turn_expected",
                        "status": "failed",
                        "error": {"message": "model unavailable"},
                        "items": [],
                    }]
                }
            }

    async def run():
        fake = FailedClient()
        backend = CodexAppServerBackend(client=fake)
        try:
            await backend.capture_delivery(
                "thread_existing",
                SendReceipt(backend_operation_id="turn_expected"),
            )
        finally:
            assert fake.released_turns == [("thread_existing", "turn_expected")]

    with pytest.raises(CodexAppServerError, match="model unavailable"):
        asyncio.run(run())


def test_codex_live_notification_surfaces_interrupted_turn():
    client = JsonRpcStdioClient(
        command=["codex"],
        cwd=None,
        env={},
        request_timeout_s=1,
    )
    state = client.set_last_turn("thread-1", "turn-1")

    client._handle_notification({  # noqa: SLF001
        "method": "turn/completed",
        "params": {
            "threadId": "thread-1",
            "turn": {"id": "turn-1", "status": "interrupted"},
        },
    })

    assert state.done.is_set()
    assert state.error == "Codex turn turn-1 interrupted: no details"


def test_codex_thread_inspector_returns_generalized_inspection():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        inspector = CodexThreadInspector(backend)
        inspection = await inspector.inspect(SimpleNamespace(
            id="rs_1",
            backend=backend.name,
            backend_session_id="thread_existing",
        ))
        return fake, inspection

    fake, inspection = asyncio.run(run())

    assert inspection is not None
    assert inspection.session_id == "rs_1"
    assert inspection.direction == "output"
    assert inspection.payload_text == "restored answer"
    assert inspection.marker.startswith("codex-thread:thread_existing:")
    assert fake.calls == [
        (
            "thread/resume",
            {
                "threadId": "thread_existing",
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            },
        ),
        ("thread/read", {"threadId": "thread_existing", "includeTurns": True}),
    ]


def test_codex_backend_capture_waits_for_last_turn():
    async def run():
        fake = FakeCodexClient()
        state = TurnState(turn_id="turn_1")
        state.text_parts.append("done")
        state.done.set()
        fake.last_turns["thread_existing"] = state
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture("thread_existing")
        return fake, result

    fake, result = asyncio.run(run())

    assert result.text == "done"
    assert result.timed_out is False
    assert fake.released_turns == [("thread_existing", "turn_1")]
    assert fake.last_turns == {}


def test_codex_backend_keeps_live_turn_state_after_capture_timeout():
    async def run():
        fake = FakeCodexClient()
        state = TurnState(turn_id="turn_pending")
        state.text_parts.append("partial")
        fake.last_turns["thread_existing"] = state
        backend = CodexAppServerBackend(client=fake, turn_timeout_s=0.001)
        result = await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_pending"),
        )
        return fake, state, result

    fake, state, result = asyncio.run(run())

    assert result == CaptureResult(text="partial", timed_out=True)
    assert state.timed_out
    assert fake.last_turns["thread_existing"] is state
    assert fake.released_turns == []


def test_codex_backend_captures_same_live_turn_after_initial_timeout():
    async def run():
        fake = FakeCodexClient()
        state = TurnState(turn_id="turn_pending")
        state.text_parts.append("partial")
        fake.last_turns["thread_existing"] = state
        backend = CodexAppServerBackend(client=fake, turn_timeout_s=0.001)
        receipt = SendReceipt(backend_operation_id="turn_pending")

        pending = await backend.capture_delivery("thread_existing", receipt)
        state.text_parts.append(" answer")
        state.done.set()
        completed = await backend.capture_delivery("thread_existing", receipt)
        return fake, pending, completed

    fake, pending, completed = asyncio.run(run())

    assert pending == CaptureResult(text="partial", timed_out=True)
    assert completed == CaptureResult(text="partial answer", timed_out=False)
    assert fake.released_turns == [("thread_existing", "turn_pending")]
    assert fake.last_turns == {}


def test_codex_backend_repeated_capture_falls_back_to_thread_history():
    async def run():
        fake = FakeCodexClient()
        state = TurnState(turn_id="turn_1")
        state.text_parts.append("live answer")
        state.done.set()
        fake.last_turns["thread_existing"] = state
        backend = CodexAppServerBackend(client=fake)
        first = await backend.capture("thread_existing")
        second = await backend.capture("thread_existing")
        return fake, first, second

    fake, first, second = asyncio.run(run())

    assert first == CaptureResult(text="live answer", timed_out=False)
    assert second == CaptureResult(text="restored answer", timed_out=False)
    assert fake.released_turns == [("thread_existing", "turn_1")]
    assert fake.calls[-1] == (
        "thread/read",
        {"threadId": "thread_existing", "includeTurns": True},
    )


def test_codex_backend_close_resumes_and_archives_thread():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.close("thread_existing")
        return fake

    fake = asyncio.run(run())

    assert fake.calls == [
        (
            "thread/resume",
            {
                "threadId": "thread_existing",
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            },
        ),
        ("thread/archive", {"threadId": "thread_existing"}),
    ]
    assert fake.released_threads == ["thread_existing"]


def test_codex_backend_aborts_exact_delivery_and_archives_thread():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.abort_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_exact"),
        )
        return fake

    fake = asyncio.run(run())

    assert fake.calls == [
        (
            "thread/resume",
            {
                "threadId": "thread_existing",
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            },
        ),
        ("turn/interrupt", {"threadId": "thread_existing", "turnId": "turn_exact"}),
        ("thread/archive", {"threadId": "thread_existing"}),
    ]
    assert fake.released_threads == ["thread_existing"]


def test_codex_backend_archives_and_releases_when_interrupt_fails():
    class InterruptFailingClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method == "turn/interrupt":
                self.calls.append((method, params))
                raise RuntimeError("interrupt failed")
            return await super().request(method, params)

    async def run():
        fake = InterruptFailingClient()
        backend = CodexAppServerBackend(client=fake)
        with pytest.raises(RuntimeError, match="interrupt failed"):
            await backend.abort_delivery(
                "thread_existing",
                SendReceipt(backend_operation_id="turn_exact"),
            )
        return fake

    fake = asyncio.run(run())

    assert fake.calls[-2:] == [
        ("turn/interrupt", {"threadId": "thread_existing", "turnId": "turn_exact"}),
        ("thread/archive", {"threadId": "thread_existing"}),
    ]
    assert fake.released_threads == ["thread_existing"]


def test_json_rpc_client_releases_exact_turn_without_removing_newer_turn():
    client = JsonRpcStdioClient(
        command=["codex"],
        cwd=None,
        env={},
        request_timeout_s=1,
    )
    first = client.set_last_turn("thread-1", "turn-1")
    second = client.set_last_turn("thread-1", "turn-2")

    client.release_turn("thread-1", "turn-1")

    assert client.last_turn("thread-1") is second
    assert ("thread-1", "turn-1") not in client._turns  # noqa: SLF001
    assert client._turns[("thread-1", "turn-2")] is second  # noqa: SLF001
    assert first is not second

    client.release_thread("thread-1")
    assert client.last_turn("thread-1") is None
    assert client._turns == {}  # noqa: SLF001


def test_json_rpc_client_close_clears_turn_state_before_process_start():
    client = JsonRpcStdioClient(
        command=["codex"],
        cwd=None,
        env={},
        request_timeout_s=1,
    )
    client.set_last_turn("thread-1", "turn-1")

    asyncio.run(client.close())

    assert client.last_turn("thread-1") is None
    assert client._turns == {}  # noqa: SLF001


class FakeStdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


class FakeStdout:
    def __init__(self) -> None:
        self.lines: asyncio.Queue[bytes] = asyncio.Queue()

    async def readline(self) -> bytes:
        return await self.lines.get()


class FakeProcess:
    def __init__(self, *, stdin: FakeStdin, stdout: FakeStdout) -> None:
        self.stdin = stdin
        self.stdout = stdout
        self.returncode: int | None = None

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        assert self.returncode is not None
        return self.returncode


def test_json_rpc_client_handles_dynamic_tool_call_request():
    async def run():
        registry = DynamicToolRegistry()
        registry.register(
            DynamicToolSpec(
                name="echo",
                description="Echo arguments",
                input_schema={"type": "object"},
            ),
            lambda call: DynamicToolResult.json({"arguments": call.arguments}),
        )
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        stdin = FakeStdin()
        client._proc = SimpleNamespace(stdin=stdin)  # noqa: SLF001
        await client._handle_server_request({  # noqa: SLF001
            "jsonrpc": "2.0",
            "id": 7,
            "method": "item/tool/call",
            "params": {
                "callId": "call-1",
                "threadId": "thread-1",
                "turnId": "turn-1",
                "tool": "echo",
                "arguments": {"value": 42},
            },
        })
        return stdin

    stdin = asyncio.run(run())
    response = json.loads(stdin.writes[0].decode())

    assert response["id"] == 7
    assert response["result"]["success"] is True
    assert response["result"]["contentItems"][0]["type"] == "inputText"
    assert json.loads(response["result"]["contentItems"][0]["text"]) == {"arguments": {"value": 42}}


def test_json_rpc_reader_continues_while_dynamic_tool_is_pending():
    async def run() -> tuple[dict, list[dict]]:
        tool_started = asyncio.Event()
        release_tool = asyncio.Event()

        async def slow_tool(call):
            tool_started.set()
            await release_tool.wait()
            return DynamicToolResult.text("done")

        registry = DynamicToolRegistry()
        registry.register(
            DynamicToolSpec(name="slow", description="Slow tool", input_schema={"type": "object"}),
            slow_tool,
        )
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        stdin = FakeStdin()
        stdout = FakeStdout()
        client._proc = FakeProcess(stdin=stdin, stdout=stdout)  # noqa: SLF001
        response = asyncio.get_running_loop().create_future()
        client._pending[1] = response  # noqa: SLF001
        await stdout.lines.put(
            (
                json.dumps({
                    "jsonrpc": "2.0",
                    "id": 7,
                    "method": "item/tool/call",
                    "params": {
                        "callId": "call-slow",
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "tool": "slow",
                        "arguments": {},
                    },
                })
                + "\n"
            ).encode()
        )
        await stdout.lines.put((json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"ok": True}}) + "\n").encode())
        reader = asyncio.create_task(client._read_stdout())  # noqa: SLF001

        await tool_started.wait()
        result = await asyncio.wait_for(asyncio.shield(response), timeout=0.2)
        assert client._server_request_tasks  # noqa: SLF001

        release_tool.set()
        await asyncio.gather(*tuple(client._server_request_tasks))  # noqa: SLF001
        await asyncio.sleep(0)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        return result, [json.loads(value.decode()) for value in stdin.writes]

    result, writes = asyncio.run(run())

    assert result == {"ok": True}
    assert writes[0]["id"] == 7
    assert writes[0]["result"]["success"] is True


def test_json_rpc_client_close_cancels_pending_dynamic_tools():
    async def run() -> tuple[bool, int]:
        tool_started = asyncio.Event()
        tool_cancelled = asyncio.Event()

        async def slow_tool(call):
            tool_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                tool_cancelled.set()

        registry = DynamicToolRegistry()
        registry.register(
            DynamicToolSpec(name="slow", description="Slow tool", input_schema={"type": "object"}),
            slow_tool,
        )
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        stdin = FakeStdin()
        stdout = FakeStdout()
        client._proc = FakeProcess(stdin=stdin, stdout=stdout)  # noqa: SLF001
        await stdout.lines.put(
            (
                json.dumps({
                    "jsonrpc": "2.0",
                    "id": 8,
                    "method": "item/tool/call",
                    "params": {
                        "callId": "call-slow",
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "tool": "slow",
                        "arguments": {},
                    },
                })
                + "\n"
            ).encode()
        )
        client._reader_task = asyncio.create_task(client._read_stdout())  # noqa: SLF001

        await tool_started.wait()
        await client.close()
        return tool_cancelled.is_set(), len(client._server_request_tasks)  # noqa: SLF001

    tool_cancelled, remaining_tasks = asyncio.run(run())

    assert tool_cancelled is True
    assert remaining_tasks == 0


def test_json_rpc_client_close_fails_pending_requests():
    async def run() -> tuple[bool, str]:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
        )
        client._proc = FakeProcess(stdin=FakeStdin(), stdout=FakeStdout())  # noqa: SLF001
        pending = asyncio.get_running_loop().create_future()
        client._pending[1] = pending  # noqa: SLF001

        await client.close()

        with pytest.raises(CodexAppServerError) as exc_info:
            await pending
        return client.failed, str(exc_info.value)

    failed, error = asyncio.run(run())

    assert failed is True
    assert error == "codex app-server client is closed"


def test_json_rpc_client_rejects_requests_after_terminal_failure():
    async def run() -> tuple[str, list[bytes]]:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
        )
        stdin = FakeStdin()
        client._proc = FakeProcess(stdin=stdin, stdout=FakeStdout())  # noqa: SLF001
        client._mark_failed("terminal transport failure")  # noqa: SLF001

        with pytest.raises(CodexAppServerError) as exc_info:
            await client.request("thread/read", {"threadId": "thread-1"})
        return str(exc_info.value), stdin.writes

    error, writes = asyncio.run(run())

    assert error == "terminal transport failure"
    assert writes == []


def test_dynamic_tool_registry_fail_closed_for_unknown_tool():
    async def run():
        return await DynamicToolRegistry().call({
            "callId": "call-1",
            "threadId": "thread-1",
            "turnId": "turn-1",
            "tool": "missing",
            "arguments": {},
        })

    result = asyncio.run(run())

    assert result["success"] is False
    assert "not registered" in result["contentItems"][0]["text"]


def test_dynamic_tool_registry_does_not_return_exception_details_to_model():
    registry = DynamicToolRegistry()
    registry.register(
        DynamicToolSpec(name="fails", description="fails", input_schema={"type": "object"}),
        lambda call: (_ for _ in ()).throw(RuntimeError("secret-provider-token")),
    )

    result = asyncio.run(registry.call({
        "callId": "call-safe-reference",
        "threadId": "thread-1",
        "turnId": "turn-1",
        "tool": "fails",
        "arguments": {},
    }))

    text = result["contentItems"][0]["text"]
    assert result["success"] is False
    assert "secret-provider-token" not in text
    assert "call-safe-reference" in text


def test_dynamic_tool_registry_fails_closed_for_malformed_call_envelope():
    result = asyncio.run(DynamicToolRegistry().call({
        "callId": "malformed-call",
        "tool": "missing-thread-and-turn",
        "arguments": {},
    }))

    assert result["success"] is False
    assert result["contentItems"][0]["text"] == "Dynamic tool failed. Reference: malformed-call"


def test_codex_backend_recreates_failed_owned_client_before_resuming(monkeypatch):
    clients = []

    class RecoverableFakeCodexClient(FakeCodexClient):
        def __init__(self):
            super().__init__()
            self.failed = False

    def create_client(**kwargs):
        client = RecoverableFakeCodexClient()
        clients.append(client)
        return client

    monkeypatch.setattr(codex_app_server_module, "JsonRpcStdioClient", create_client)

    async def run():
        backend = CodexAppServerBackend()
        await backend.ensure_initialized()
        clients[0].failed = True
        await backend.send("thread-existing", "continue")
        await backend.shutdown()

    asyncio.run(run())

    assert len(clients) == 2
    assert clients[0].closed is True
    assert [method for method, _ in clients[1].calls] == ["initialize", "thread/resume", "turn/start"]
