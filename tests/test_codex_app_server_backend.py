import asyncio
import json
from types import SimpleNamespace

import pytest

from agent_platform.sessions import (
    CaptureResult,
    CodexAppServerBackend,
    CodexAppServerError,
    CodexThreadInspector,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    OpenSpec,
)
from agent_platform.sessions.backends.codex_app_server import (
    JsonRpcStdioClient,
    TurnState,
    extract_thread_text,
    parse_codex_version,
)


def test_parse_codex_app_server_version():
    assert parse_codex_version("Codex Desktop/0.130.0-alpha.5 (Mac OS)") == (0, 130, 0)
    assert parse_codex_version("Codex CLI/1.2.3") == (1, 2, 3)
    assert parse_codex_version("no-version") is None


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
            collaboration_mode="autonomous",
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
            "collaborationMode": "autonomous",
        },
    )


def test_codex_backend_rejects_non_codex_kind(tmp_path):
    async def run():
        await CodexAppServerBackend(client=FakeCodexClient()).open(
            OpenSpec(kind="claude_cli", cwd=str(tmp_path))
        )

    with pytest.raises(CodexAppServerError, match="cannot open"):
        asyncio.run(run())


def test_codex_backend_resumes_persisted_thread_before_send():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.send("thread_existing", "hello")
        return fake

    fake = asyncio.run(run())

    assert fake.calls[0] == ("thread/resume", {"threadId": "thread_existing"})
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
        ("thread/resume", {"threadId": "thread_existing"}),
        ("thread/read", {"threadId": "thread_existing", "includeTurns": True}),
    ]


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
        ("thread/resume", {"threadId": "thread_existing"}),
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
        return result

    result = asyncio.run(run())

    assert result.text == "done"
    assert result.timed_out is False


def test_codex_backend_close_resumes_and_archives_thread():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.close("thread_existing")
        return fake

    fake = asyncio.run(run())

    assert fake.calls == [
        ("thread/resume", {"threadId": "thread_existing"}),
        ("thread/archive", {"threadId": "thread_existing"}),
    ]


class FakeStdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


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
