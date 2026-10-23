import asyncio
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.execution import ToolExecutionContext
from soveren_agent_platform.sessions import (
    CaptureResult,
    CodexAppServerBackend,
    CodexAppServerError,
    CodexClientInfo,
    CodexCollaborationMode,
    CodexThreadInspector,
    CodexTurnFailure,
    ConversationScope,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    OpenSpec,
    RuntimeSession,
    SendReceipt,
    TenantBoundaryError,
    UnsupportedContextualSessionError,
    ensure_conversation_boundary,
)
from soveren_agent_platform.sessions.backend import DeliveryPauseResult
from soveren_agent_platform.sessions.backends import codex_app_server as codex_app_server_module
from soveren_agent_platform.sessions.backends.codex_app_server import (
    JsonRpcStdioClient,
    TurnState,
    extract_thread_text,
    parse_codex_version,
)


def test_codex_initialize_uses_application_identity(monkeypatch):
    clients = []

    def create_client(**kwargs):
        client = FakeCodexClient()
        clients.append(client)
        return client

    monkeypatch.setattr(codex_app_server_module, "JsonRpcStdioClient", create_client)

    async def run():
        backend = CodexAppServerBackend(client_info=CodexClientInfo("Pulsy", "0.1.0", "Pulsy"))
        try:
            await backend.ensure_initialized()
            assert clients[0].calls[0][0] == "initialize"
            assert clients[0].calls[0][1]["clientInfo"] == {"name": "Pulsy", "version": "0.1.0", "title": "Pulsy"}
        finally:
            await backend.shutdown()

    asyncio.run(run())


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
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
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
        if state is not None:
            setattr(state, "execution_context", None)
            setattr(state, "dynamic_tool_allowlist", None)
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


def test_codex_backend_applies_output_schema_to_one_requested_turn(tmp_path):
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        opened = await backend.open(
            OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work"))
        )
        await backend.send(opened.backend_session_id, "unstructured")
        await backend.send_with_output_schema(
            opened.backend_session_id,
            "structured",
            {"type": "object"},
        )
        return fake

    fake = asyncio.run(run())
    turns = [params for method, params in fake.calls if method == "turn/start"]

    assert "outputSchema" not in turns[0]
    assert turns[1]["outputSchema"] == {"type": "object"}


def test_output_schema_marks_turn_and_receipt_for_structured_capture(tmp_path):
    class TurnStateClient(FakeCodexClient):
        def set_last_turn(self, thread_id: str, turn_id: str) -> TurnState:
            state = TurnState(turn_id=turn_id)
            self.last_turns[thread_id] = state
            return state

    async def run():
        client = TurnStateClient()
        backend = CodexAppServerBackend(client=client)
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))
        receipt = await backend.send_with_output_schema(
            opened.backend_session_id,
            "return JSON",
            {"type": "object"},
        )
        state = client.last_turns[opened.backend_session_id]
        assert isinstance(state, TurnState)
        state.text_parts.extend(['{"ok":false}', '{"ok":true}'])
        state.text_bytes = len(''.join(state.text_parts).encode())
        state.record_agent_message_phase("commentary", "commentary")
        state.append_agent_message_delta("commentary", '{"ok":false}')
        state.record_agent_message_phase("final", "final_answer")
        state.append_agent_message_delta("final", '{"ok":true}')
        state.done.set()
        capture = await backend.capture(opened.backend_session_id)
        return receipt, state, capture

    receipt, state, capture = asyncio.run(run())

    assert receipt.metadata == {"structured_output": True}
    assert state.structured_output
    assert capture == CaptureResult(text='{"ok":true}', timed_out=False)


def test_structured_turn_uses_final_agent_message_from_native_items_before_ack():
    async def run() -> TurnState:
        client = JsonRpcStdioClient(command=["codex"], cwd=None, env={}, request_timeout_s=1)
        client.prepare_turn_context(
            "thread-1",
            execution_context=None,
            dynamic_tool_allowlist=None,
            structured_output=True,
        )
        notifications = [
            ("item/started", {"id": "item-comment", "type": "agentMessage", "text": "", "phase": None}),
            ("item/agentMessage/delta", {"itemId": "item-comment", "delta": '{"ok":false}'}),
            ("item/started", {"id": "item-final", "type": "agentMessage", "text": "", "phase": None}),
            ("item/agentMessage/delta", {"itemId": "item-final", "delta": '{"ok":true}'}),
            (
                "item/completed",
                {"id": "item-comment", "type": "agentMessage", "text": '{"ok":false}', "phase": "commentary"},
            ),
            (
                "item/completed",
                {"id": "item-final", "type": "agentMessage", "text": '{"ok":true}', "phase": "final_answer"},
            ),
        ]
        for method, item in notifications:
            params = {"threadId": "thread-1", "turnId": "turn-1", **item}
            if method in {"item/started", "item/completed"}:
                params["item"] = {key: value for key, value in item.items() if key != "itemId"}
            client._handle_notification({"method": method, "params": params})  # noqa: SLF001

        state = client.set_last_turn("thread-1", "turn-1")
        client.acknowledge_turn_context("thread-1", "turn-1", state)
        client._handle_notification({  # noqa: SLF001
            "method": "turn/completed",
            "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}},
        })
        return state

    state = asyncio.run(run())

    assert state.structured_output
    assert state.done.is_set()
    assert state.text == '{"ok":true}'
    assert state.text_bytes == len(b'{"ok":false}{"ok":true}')


def test_structured_turn_unknown_phases_preserve_legacy_text():
    client = JsonRpcStdioClient(command=["codex"], cwd=None, env={}, request_timeout_s=1)
    client.prepare_turn_context(
        "thread-1",
        execution_context=None,
        dynamic_tool_allowlist=None,
        structured_output=True,
    )
    state = client.set_last_turn("thread-1", "turn-1")
    client.acknowledge_turn_context("thread-1", "turn-1", state)

    for item_id, delta in (("item-1", "first "), ("item-2", "answer")):
        client._handle_notification({  # noqa: SLF001
            "method": "item/started",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {"id": item_id, "type": "agentMessage", "text": "", "phase": None},
            },
        })
        client._handle_notification({  # noqa: SLF001
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": item_id, "delta": delta},
        })
        client._handle_notification({  # noqa: SLF001
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {"id": item_id, "type": "agentMessage", "text": delta, "phase": None},
            },
        })

    assert state.text == "first answer"


def test_unstructured_turn_preserves_all_agent_message_phases():
    client = JsonRpcStdioClient(command=["codex"], cwd=None, env={}, request_timeout_s=1)
    state = client.set_last_turn("thread-1", "turn-1")
    for item_id, phase, delta in (
        ("item-comment", "commentary", "commentary "),
        ("item-final", "final_answer", "final"),
    ):
        client._handle_notification({  # noqa: SLF001
            "method": "item/started",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {"id": item_id, "type": "agentMessage", "text": "", "phase": phase},
            },
        })
        client._handle_notification({  # noqa: SLF001
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": item_id, "delta": delta},
        })
        client._handle_notification({  # noqa: SLF001
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {"id": item_id, "type": "agentMessage", "text": delta, "phase": phase},
            },
        })

    assert state.text == "commentary final"


def test_structured_turn_timeout_preserves_final_item_text():
    async def run():
        client = JsonRpcStdioClient(command=["codex"], cwd=None, env={}, request_timeout_s=1)
        state = client.set_last_turn("thread-1", "turn-1")
        state.structured_output = True
        for item_id, phase, delta in (
            ("item-comment", "commentary", '{"ok":false}'),
            ("item-final", "final_answer", '{"ok":true}'),
        ):
            client._handle_notification({  # noqa: SLF001
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"id": item_id, "type": "agentMessage", "text": "", "phase": phase},
                },
            })
            client._handle_notification({  # noqa: SLF001
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": item_id,
                    "delta": delta,
                },
            })
        backend = CodexAppServerBackend(client=client, turn_timeout_s=0.001)
        return await backend.capture("thread-1")

    capture = asyncio.run(run())

    assert capture == CaptureResult(text='{"ok":true}', timed_out=True)


def test_codex_thread_start_filters_request_local_tool_allowlist(tmp_path):
    async def run():
        fake = FakeCodexClient()
        registry = DynamicToolRegistry()
        registry.register(
            DynamicToolSpec(
                name="business_action",
                description="Run a business action",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: DynamicToolResult.text("called"),
        )
        backend = CodexAppServerBackend(client=fake, dynamic_tools=registry)
        await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            dynamic_tool_allowlist=(),
        ))
        return fake

    fake = asyncio.run(run())

    assert fake.calls[0][0] == "thread/start"
    assert "dynamicTools" not in fake.calls[0][1]


def test_codex_backend_enforces_bound_dynamic_tool_conversation_before_io(tmp_path):
    fake = FakeCodexClient()
    registry = DynamicToolRegistry()
    registry.bind_conversation(tenant_id="tenant-a", source_id="chat-a")
    backend = CodexAppServerBackend(client=fake, dynamic_tools=registry)

    ensure_conversation_boundary(
        backend,
        "tenant-a",
        "chat-a",
        resource_name="Codex backend",
    )
    with pytest.raises(TenantBoundaryError, match="chat-a.*chat-b"):
        asyncio.run(
            backend.open(
                OpenSpec(
                    kind="codex_cli",
                    cwd=str(tmp_path / "work"),
                    conversation_scope=ConversationScope(
                        tenant_id="tenant-a",
                        source_id="chat-b",
                    ),
                )
            )
        )

    assert fake.calls == []


def test_codex_backend_requires_scope_for_bound_dynamic_tools_before_io(tmp_path):
    fake = FakeCodexClient()
    registry = DynamicToolRegistry()
    registry.bind_conversation(tenant_id="tenant-a", source_id="chat-a")
    backend = CodexAppServerBackend(client=fake, dynamic_tools=registry)

    with pytest.raises(TenantBoundaryError, match="requires a trusted conversation scope"):
        asyncio.run(
            backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))
        )

    assert fake.calls == []


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


def test_codex_backend_resume_restores_exact_host_scope_and_allowlist(tmp_path):
    class ContextAwareFakeClient(FakeCodexClient):
        def prepare_turn_context(
            self,
            thread_id,
            *,
            execution_context,
            dynamic_tool_allowlist,
            structured_output=False,
        ):
            return SimpleNamespace(
                thread_id=thread_id,
                execution_context=execution_context,
                dynamic_tool_allowlist=dynamic_tool_allowlist,
                structured_output=structured_output,
            )

        def acknowledge_turn_context(self, thread_id, turn_id, state):
            del thread_id, turn_id
            state.start_acknowledged = True

        def fail_turn_context(self, thread_id):
            del thread_id

    async def run():
        fake = ContextAwareFakeClient()
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id="tenant-a", source_id="chat-a")
        for name in ("inspect", "write"):
            registry.register(
                DynamicToolSpec(
                    name=name,
                    description=f"{name} a repository",
                    input_schema={"type": "object"},
                    namespace="github",
                ),
                lambda call: DynamicToolResult.text("called"),
            )
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        allowlist = (("github", "inspect"),)
        backend = CodexAppServerBackend(client=fake, dynamic_tools=registry, model="gpt-5.4")
        spec = OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
            dynamic_tool_allowlist=allowlist,
        )

        resumed = await backend.resume(spec, "thread-persisted")
        with pytest.raises(TenantBoundaryError):
            await backend.resume(
                OpenSpec(
                    kind="codex_cli",
                    cwd=str(tmp_path / "work"),
                    conversation_scope=ConversationScope("tenant-a", "chat-other"),
                    dynamic_tool_allowlist=allowlist,
                ),
                "thread-persisted",
            )
        with pytest.raises(CodexAppServerError, match="allowlist conflicts"):
            await backend.resume(
                OpenSpec(
                    kind="codex_cli",
                    cwd=str(tmp_path / "work"),
                    conversation_scope=scope,
                    dynamic_tool_allowlist=(("github", "write"),),
                ),
                "thread-persisted",
            )
        with pytest.raises(TenantBoundaryError, match="does not match"):
            await backend.send_with_context(
                "thread-persisted",
                "wrong scope",
                execution_context=ToolExecutionContext(
                    scope=ConversationScope("tenant-a", "chat-other"),
                    execution_id="exec-other",
                    task_id="task-a",
                ),
                output_schema=None,
                dynamic_tool_allowlist=allowlist,
            )
        with pytest.raises(CodexAppServerError, match="allowlist"):
            await backend.send_with_context(
                "thread-persisted",
                "wrong allowlist",
                execution_context=ToolExecutionContext(scope=scope, execution_id="exec-a", task_id="task-a"),
                output_schema=None,
                dynamic_tool_allowlist=(("github", "write"),),
            )

        receipt = await backend.send_with_context(
            "thread-persisted",
            "continue",
            execution_context=ToolExecutionContext(scope=scope, execution_id="exec-a", task_id="task-a"),
            output_schema=None,
            dynamic_tool_allowlist=allowlist,
        )
        return fake, backend, resumed, receipt, scope, allowlist

    fake, backend, resumed, receipt, scope, allowlist = asyncio.run(run())

    assert resumed.backend_session_id == "thread-persisted"
    assert receipt.backend_operation_id == "turn_1"
    assert backend._thread_scopes["thread-persisted"] == scope  # noqa: SLF001
    assert backend._thread_tool_allowlists["thread-persisted"] == allowlist  # noqa: SLF001
    assert [method for method, _ in fake.calls] == ["thread/resume", "turn/start"]
    assert "dynamicTools" not in fake.calls[0][1]
    assert fake.calls[1][1]["threadId"] == "thread-persisted"


def test_codex_backend_resume_rejects_native_thread_identity_mismatch():
    class MismatchedClient(FakeCodexClient):
        async def request(self, method, params):
            if method == "thread/resume":
                self.calls.append((method, params))
                return {"thread": {"id": "different-thread"}}
            return await super().request(method, params)

    async def run():
        fake = MismatchedClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.resume(
            OpenSpec(
                kind="codex_cli",
                cwd="/workspace",
                conversation_scope=ConversationScope("tenant-a", "chat-a"),
            ),
            "thread-persisted",
        )

    with pytest.raises(CodexAppServerError, match="expected persisted thread id"):
        asyncio.run(run())


@pytest.mark.parametrize("status", ["inProgress", None])
def test_codex_backend_pause_waits_for_exact_turn_terminal_status(status):
    class PendingInterruptClient(FakeCodexClient):
        async def request(self, method, params):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            data = [] if status is None else [{
                "id": "turn-exact",
                "status": status,
                "items": [{"type": "agentMessage", "text": "partial output"}],
            }]
            return {"data": data, "nextCursor": None}

    async def run():
        fake = PendingInterruptClient()
        backend = CodexAppServerBackend(client=fake, request_timeout_s=1, turn_timeout_s=0.01)
        result = await backend.pause_delivery(
            "thread-persisted",
            SendReceipt(backend_operation_id="turn-exact"),
        )
        return fake, result

    fake, result = asyncio.run(run())

    assert result == DeliveryPauseResult(status="pending")
    expected_interrupts = (
        [{"threadId": "thread-persisted", "turnId": "turn-exact"}]
        if status == "inProgress"
        else []
    )
    assert [params for method, params in fake.calls if method == "turn/interrupt"] == expected_interrupts
    assert fake.released_turns == []
    assert "thread/archive" not in [method for method, _ in fake.calls]
    assert "turn/start" not in [method for method, _ in fake.calls]


def test_codex_backend_pause_recovers_exact_completed_turn_winning_interrupt_race():
    class CompletionRaceClient(FakeCodexClient):
        def __init__(self):
            super().__init__()
            self.status_reads = 0

        async def request(self, method, params):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            self.status_reads += 1
            if self.status_reads == 1:
                return {
                    "data": [{
                        "id": "turn-newer",
                        "status": "completed",
                        "items": [{"type": "agentMessage", "text": "unrelated"}],
                    }],
                    "nextCursor": "older",
                }
            if self.status_reads == 2:
                return {
                    "data": [{
                        "id": "turn-expected",
                        "status": "inProgress",
                        "items": [{"type": "agentMessage", "text": "partial"}],
                    }],
                    "nextCursor": None,
                }
            return {
                "data": [{
                    "id": "turn-expected",
                    "status": "completed",
                    "items": [
                        {
                            "type": "agentMessage",
                            "id": "commentary",
                            "phase": "commentary",
                            "text": "progress only",
                        },
                        {
                            "type": "agentMessage",
                            "id": "final",
                            "phase": "final_answer",
                            "text": '{"answer":"done"}',
                        },
                    ],
                }],
                "nextCursor": None,
            }

    async def run():
        fake = CompletionRaceClient()
        backend = CodexAppServerBackend(client=fake, request_timeout_s=1, turn_timeout_s=1)
        state = TurnState(turn_id="turn-expected")
        state.execution_context = ToolExecutionContext(
            scope=ConversationScope("tenant-a", "chat-a"),
            execution_id="exec-a",
            task_id="task-a",
        )
        state.dynamic_tool_allowlist = (("github", "inspect"),)
        fake.last_turns["thread-persisted"] = state
        result = await backend.pause_delivery(
            "thread-persisted",
            SendReceipt(
                backend_operation_id="turn-expected",
                metadata={"structured_output": True},
            ),
        )
        return fake, state, result

    fake, state, result = asyncio.run(run())

    assert result == DeliveryPauseResult(status="completed", text='{"answer":"done"}')
    interrupt = [params for method, params in fake.calls if method == "turn/interrupt"]
    assert interrupt == [{"threadId": "thread-persisted", "turnId": "turn-expected"}]
    assert fake.released_turns == [("thread-persisted", "turn-expected")]
    assert state.execution_context is None
    assert state.dynamic_tool_allowlist is None
    assert "thread/archive" not in [method for method, _ in fake.calls]
    assert "turn/start" not in [method for method, _ in fake.calls]


def test_codex_backend_pause_does_not_treat_partial_structured_output_as_final():
    class PartialStructuredClient(FakeCodexClient):
        async def request(self, method, params):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "data": [{
                    "id": "turn-exact",
                    "status": "completed",
                    "items": [{
                        "type": "agentMessage",
                        "phase": "commentary",
                        "text": '{"partial":true}',
                    }],
                }],
                "nextCursor": None,
            }

    async def run():
        fake = PartialStructuredClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.pause_delivery(
            "thread-persisted",
            SendReceipt(
                backend_operation_id="turn-exact",
                metadata={"structured_output": True},
            ),
        )

    with pytest.raises(CodexTurnFailure) as exc_info:
        asyncio.run(run())

    assert exc_info.value.reason == "structured_final_missing"
    assert exc_info.value.terminal_observed is True


@pytest.mark.parametrize(
    ("status", "reason", "terminal"),
    [("failed", "turn_failed", True), ("paused", "turn_status_unknown", False)],
)
def test_codex_backend_pause_surfaces_failed_or_unknown_turn_status(status, reason, terminal):
    class StatusClient(FakeCodexClient):
        async def request(self, method, params):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "data": [{"id": "turn-exact", "status": status, "items": []}],
                "nextCursor": None,
            }

    async def run():
        fake = StatusClient()
        backend = CodexAppServerBackend(client=fake, request_timeout_s=1, turn_timeout_s=1)
        await backend.pause_delivery(
            "thread-persisted",
            SendReceipt(backend_operation_id="turn-exact"),
        )

    with pytest.raises(CodexTurnFailure) as exc_info:
        asyncio.run(run())

    assert exc_info.value.reason == reason
    assert exc_info.value.terminal_observed is terminal


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
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            if params.get("cursor") is None:
                return {
                    "data": [{
                        "id": "turn_old",
                        "status": "completed",
                        "items": [{"type": "agentMessage", "text": "old answer"}],
                    }],
                    "nextCursor": "next-page",
                }
            return {
                "data": [{
                    "id": "turn_expected",
                    "status": "completed",
                    "items": [{"type": "agentMessage", "text": "expected answer"}],
                }],
                "nextCursor": None,
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
    assert fake.calls[-2:] == [
        (
            "thread/turns/list",
            {"threadId": "thread_existing", "limit": 1, "sortDirection": "desc", "itemsView": "full"},
        ),
        (
            "thread/turns/list",
            {
                "threadId": "thread_existing",
                "limit": 1,
                "sortDirection": "desc",
                "itemsView": "full",
                "cursor": "next-page",
            },
        ),
    ]
    assert fake.released_turns == [("thread_existing", "turn_expected")]


@pytest.mark.parametrize(
    ("structured_output", "phases", "expected"),
    [
        (True, ("commentary", "final_answer"), '{"ok":true}'),
        (True, (None, None), '{"ok":false}\n{"ok":true}'),
        (False, ("commentary", "final_answer"), '{"ok":false}\n{"ok":true}'),
    ],
)
def test_codex_backend_recovers_structured_final_item_after_restart(
    structured_output: bool,
    phases: tuple[str | None, str | None],
    expected: str,
):
    class RestartedClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "data": [{
                    "id": "turn_expected",
                    "status": "completed",
                    "items": [
                        {
                            "type": "agentMessage",
                            "id": "item-comment",
                            "text": '{"ok":false}',
                            "phase": phases[0],
                        },
                        {
                            "type": "agentMessage",
                            "id": "item-final",
                            "text": '{"ok":true}',
                            "phase": phases[1],
                        },
                    ],
                }],
                "nextCursor": None,
            }

    async def run():
        backend = CodexAppServerBackend(client=RestartedClient())
        metadata = {"structured_output": True} if structured_output else None
        return await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_expected", metadata=metadata),
        )

    result = asyncio.run(run())

    assert result == CaptureResult(text=expected, timed_out=False)


@pytest.mark.parametrize("status", ["inProgress", None])
def test_codex_backend_keeps_unfinished_or_not_yet_visible_turn_pending(status):
    class PendingClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            turns = [] if status is None else [{
                "id": "turn_expected",
                "status": status,
                "items": [{"type": "agentMessage", "text": "partial"}],
            }]
            return {"data": turns, "nextCursor": None}

    async def run():
        fake = PendingClient()
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_expected"),
        )
        return fake, result

    fake, result = asyncio.run(run())

    assert result.timed_out is True
    assert result.text == ("partial" if status else "")
    assert fake.released_turns == []


def test_codex_backend_releases_recovered_terminal_turn_without_removing_newer_turn():
    class RecoveredClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/turns/list":
                return await super().request(method, params)
            self.calls.append((method, params))
            return {
                "data": [{
                    "id": "turn_old",
                    "status": "completed",
                    "items": [{"type": "agentMessage", "text": "old answer"}],
                }],
                "nextCursor": None,
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
            if method != "thread/turns/list":
                return await super().request(method, params)
            return {
                "data": [{
                    "id": "turn_expected",
                    "status": "failed",
                    "error": {"message": "model unavailable"},
                    "items": [],
                }],
                "nextCursor": None,
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


def test_codex_backend_rejects_recovered_turn_above_output_limit():
    class OversizedClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method != "thread/turns/list":
                return await super().request(method, params)
            return {
                "data": [{
                    "id": "turn_expected",
                    "status": "completed",
                    "items": [{"type": "agentMessage", "text": "too long"}],
                }],
                "nextCursor": None,
            }

    async def run():
        backend = CodexAppServerBackend(client=OversizedClient(), max_turn_output_bytes=5)
        await backend.capture_delivery(
            "thread_existing",
            SendReceipt(backend_operation_id="turn_expected"),
        )

    with pytest.raises(CodexAppServerError, match="output exceeds 5 bytes"):
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
        inspection = await inspector.inspect(
            RuntimeSession(
                id="rs_1",
                tenant_id="tenant-a",
                source_id="chat-a",
                kind="codex_cli",
                backend=backend.name,
                backend_session_id="thread_existing",
                status="idle",
            )
        )
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


def test_codex_thread_inspector_rejects_backend_scope_mismatch_before_io():
    async def run():
        fake = FakeCodexClient()
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id="tenant-a", source_id="chat-a")
        backend = CodexAppServerBackend(client=fake, dynamic_tools=registry)
        inspector = CodexThreadInspector(backend)

        with pytest.raises(TenantBoundaryError, match="chat-a.*chat-b"):
            await inspector.inspect(
                RuntimeSession(
                    id="rs_1",
                    tenant_id="tenant-a",
                    source_id="chat-b",
                    kind="codex_cli",
                    backend=backend.name,
                    backend_session_id="thread_existing",
                    status="idle",
                )
            )
        return fake

    fake = asyncio.run(run())

    assert fake.calls == []


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

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        assert separator == b"\n"
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


def test_json_rpc_client_configures_explicit_subprocess_stream_limit(monkeypatch):
    async def run() -> int:
        calls: list[dict] = []

        class HangingStream:
            async def readuntil(self, separator: bytes) -> bytes:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def read(self, limit: int) -> bytes:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        async def create_process(*args, **kwargs):
            calls.append(kwargs)
            process = FakeProcess(stdin=FakeStdin(), stdout=HangingStream())
            process.stderr = HangingStream()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_json_rpc_frame_bytes=123_456,
        )
        await client.start()
        await client.close()
        return calls[0]["limit"]

    assert asyncio.run(run()) == 123_456


def test_json_rpc_reader_accepts_frame_above_asyncio_default_limit():
    async def run() -> TurnState:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_json_rpc_frame_bytes=100_000,
            max_turn_output_bytes=100_000,
        )
        stdout = asyncio.StreamReader(limit=100_000)
        client._proc = SimpleNamespace(stdout=stdout)  # noqa: SLF001
        state = client.set_last_turn("thread-1", "turn-1")
        stdout.feed_data(
            (
                json.dumps({
                    "jsonrpc": "2.0",
                    "method": "item/agentMessage/delta",
                    "params": {"threadId": "thread-1", "turnId": "turn-1", "delta": "x" * 70_000},
                })
                + "\n"
                + json.dumps({
                    "jsonrpc": "2.0",
                    "method": "turn/completed",
                    "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}},
                })
                + "\n"
            ).encode()
        )
        reader = asyncio.create_task(client._read_stdout())  # noqa: SLF001
        await asyncio.wait_for(state.done.wait(), timeout=1)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        return state

    state = asyncio.run(run())

    assert state.error is None
    assert state.text_bytes == 70_000
    assert state.text == "x" * 70_000


def test_json_rpc_reader_fails_explicitly_above_configured_frame_limit():
    async def run() -> JsonRpcStdioClient:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_json_rpc_frame_bytes=256,
        )
        stdout = asyncio.StreamReader(limit=256)
        client._proc = SimpleNamespace(stdout=stdout)  # noqa: SLF001
        stdout.feed_data(b"x" * 257 + b"\n")
        await client._read_stdout()  # noqa: SLF001
        return client

    client = asyncio.run(run())

    assert client.failed is True
    assert client._terminal_error == "codex app-server JSON-RPC frame exceeds 256 bytes"  # noqa: SLF001


def test_json_rpc_writer_rejects_frame_above_configured_limit_before_write():
    async def run() -> FakeStdin:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_json_rpc_frame_bytes=128,
        )
        stdin = FakeStdin()
        client._proc = FakeProcess(stdin=stdin, stdout=FakeStdout())  # noqa: SLF001

        with pytest.raises(CodexAppServerError, match="outbound Codex JSON-RPC frame exceeds 128 bytes"):
            await client._write_message({"jsonrpc": "2.0", "method": "test", "params": {"value": "x" * 256}})  # noqa: SLF001
        return stdin

    stdin = asyncio.run(run())

    assert stdin.writes == []


def test_json_rpc_turn_output_limit_interrupts_once_and_preserves_error():
    async def run() -> tuple[TurnState, list[dict]]:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_turn_output_bytes=5,
        )
        stdin = FakeStdin()
        client._proc = FakeProcess(stdin=stdin, stdout=FakeStdout())  # noqa: SLF001
        state = client.set_last_turn("thread-1", "turn-1")
        for delta in ("hello", "!", "ignored"):
            client._handle_notification({  # noqa: SLF001
                "method": "item/agentMessage/delta",
                "params": {"threadId": "thread-1", "turnId": "turn-1", "delta": delta},
            })
        await asyncio.sleep(0)
        writes = [json.loads(value.decode()) for value in stdin.writes]
        client._handle_response({"id": writes[0]["id"], "result": {}})  # noqa: SLF001
        await asyncio.gather(*tuple(client._turn_interrupt_tasks))  # noqa: SLF001
        client._handle_notification({  # noqa: SLF001
            "method": "turn/completed",
            "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}},
        })
        return state, writes

    state, writes = asyncio.run(run())

    assert state.done.is_set()
    assert state.text == "hello"
    assert state.text_bytes == 5
    assert state.error == "Codex turn turn-1 output exceeds 5 bytes"
    assert len(writes) == 1
    assert writes[0]["method"] == "turn/interrupt"
    assert writes[0]["params"] == {"threadId": "thread-1", "turnId": "turn-1"}


def test_structured_turn_output_limit_counts_commentary_and_final_text():
    async def run() -> tuple[TurnState, list[dict]]:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_turn_output_bytes=len(b'{"ok":false}'),
        )
        stdin = FakeStdin()
        client._proc = FakeProcess(stdin=stdin, stdout=FakeStdout())  # noqa: SLF001
        state = client.set_last_turn("thread-1", "turn-1")
        state.structured_output = True
        for item_id, phase, delta in (
            ("item-comment", "commentary", '{"ok":false}'),
            ("item-final", "final_answer", '{"ok":true}'),
        ):
            client._handle_notification({  # noqa: SLF001
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"id": item_id, "type": "agentMessage", "text": "", "phase": phase},
                },
            })
            client._handle_notification({  # noqa: SLF001
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": item_id,
                    "delta": delta,
                },
            })
        await asyncio.sleep(0)
        writes = [json.loads(value.decode()) for value in stdin.writes]
        client._handle_response({"id": writes[0]["id"], "result": {}})  # noqa: SLF001
        await asyncio.gather(*tuple(client._turn_interrupt_tasks))  # noqa: SLF001
        return state, writes

    state, writes = asyncio.run(run())

    assert state.output_limit_exceeded
    assert state.text_bytes == len(b'{"ok":false}')
    assert state.error == "Codex turn turn-1 output exceeds 12 bytes"
    assert len(writes) == 1
    assert writes[0]["method"] == "turn/interrupt"


def test_json_rpc_stderr_reader_drains_after_bounded_log_budget(caplog):
    class FakeStderr:
        def __init__(self) -> None:
            self.chunks = [b"x" * 8192 for _ in range(9)] + [b""]

        async def read(self, limit: int) -> bytes:
            assert limit == 8192
            return self.chunks.pop(0)

    async def run() -> None:
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
        )
        client._proc = SimpleNamespace(stderr=FakeStderr())  # noqa: SLF001
        await client._read_stderr()  # noqa: SLF001

    caplog.set_level("INFO")
    asyncio.run(run())

    info_records = [record for record in caplog.records if record.levelname == "INFO"]
    warning_records = [record for record in caplog.records if record.levelname == "WARNING"]
    assert len(info_records) == 8
    assert len(warning_records) == 1
    assert "suppressed after 65536 bytes" in warning_records[0].message


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
        client.prepare_turn_context(
            "thread-1",
            execution_context=None,
            dynamic_tool_allowlist=None,
        )
        callback = asyncio.create_task(client._handle_server_request({  # noqa: SLF001
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
        }))
        await asyncio.sleep(0)
        assert not callback.done()
        state = client.set_last_turn("thread-1", "turn-1")
        client.acknowledge_turn_context("thread-1", "turn-1", state)
        await callback
        return stdin

    stdin = asyncio.run(run())
    response = json.loads(stdin.writes[0].decode())

    assert response["id"] == 7
    assert response["result"]["success"] is True
    assert response["result"]["contentItems"][0]["type"] == "inputText"
    assert json.loads(response["result"]["contentItems"][0]["text"]) == {"arguments": {"value": 42}}


class EarlyContextCallbackClient(JsonRpcStdioClient):
    def __init__(
        self,
        registry: DynamicToolRegistry,
        *,
        wrong_turn_id: bool = False,
        fail_start: bool = False,
        callback_tool: str = "inspect",
        hold_ack: bool = False,
    ) -> None:
        super().__init__(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        self._proc = SimpleNamespace(stdin=FakeStdin())  # noqa: SLF001
        self.wrong_turn_id = wrong_turn_id
        self.fail_start = fail_start
        self.callback_tool = callback_tool
        self.hold_ack = hold_ack
        self.release_start_ack = asyncio.Event()
        self.thread_count = 0
        self.calls: list[tuple[str, dict]] = []
        self.callback_tasks: list[asyncio.Task[None]] = []
        self.waiting_threads: list[str] = []
        self.any_callback_waiting = asyncio.Event()
        self.all_callbacks_waiting = asyncio.Event()
        self.tool_started: list[str] = []

    async def request(self, method: str, params: dict):
        self.calls.append((method, params))
        if method == "thread/start":
            self.thread_count += 1
            return {"thread": {"id": f"thread-{self.thread_count}"}}
        if method == "turn/start":
            thread_id = params["threadId"]
            suffix = thread_id.rsplit("-", 1)[1]
            actual_turn_id = f"turn-{suffix}"
            callback_turn_id = "turn-forged" if self.wrong_turn_id else actual_turn_id
            self.callback_tasks.append(asyncio.create_task(self._handle_server_request({  # noqa: SLF001
                "jsonrpc": "2.0",
                "id": f"call-{suffix}",
                "method": "item/tool/call",
                "params": {
                    "callId": f"call-{suffix}",
                    "threadId": thread_id,
                    "turnId": callback_turn_id,
                    "tool": self.callback_tool,
                    "namespace": "product",
                    "arguments": {"raw": "ok"},
                    "execution_context": {
                        "scope": {"tenant_id": "forged", "source_id": "forged"},
                        "execution_id": "forged",
                        "task_id": "forged",
                    },
                },
            })))
            await asyncio.sleep(0)
            if self.hold_ack:
                await self.release_start_ack.wait()
            if self.fail_start:
                raise RuntimeError("turn/start acknowledgement lost")
            return {"turn": {"id": actual_turn_id}}
        raise AssertionError(f"unexpected Codex request: {method}")

    async def contextual_turn_for_tool_call(self, thread_id: str, turn_id: str):
        self.waiting_threads.append(thread_id)
        self.any_callback_waiting.set()
        if len(self.waiting_threads) >= 2:
            self.all_callbacks_waiting.set()
        return await super().contextual_turn_for_tool_call(thread_id, turn_id)


def test_contextual_callbacks_wait_for_ack_and_use_concurrent_host_contexts(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        contexts: dict[str, ToolExecutionContext | None] = {}
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)

        def inspect(call):
            contexts[call.call_id] = call.execution_context
            return DynamicToolResult.text("trusted")

        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            inspect,
        )
        client = EarlyContextCallbackClient(registry, hold_ack=True)
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await asyncio.gather(*(
            backend.open(OpenSpec(
                kind="codex_cli",
                cwd=str(tmp_path / f"work-{index}"),
                conversation_scope=scope,
                dynamic_tool_allowlist=(("product", "inspect"),),
            ))
            for index in (1, 2)
        ))
        executions = [
            ToolExecutionContext(scope=scope, execution_id=f"exec-{index}", task_id=f"task-{index}")
            for index in (1, 2)
        ]
        sends = [
            asyncio.create_task(backend.send_with_context(
                session.backend_session_id,
                "work",
                execution_context=execution,
                output_schema={"type": "object"},
                dynamic_tool_allowlist=(("product", "inspect"),),
            ))
            for session, execution in zip(opened, executions, strict=True)
        ]
        await asyncio.wait_for(client.all_callbacks_waiting.wait(), timeout=1)
        assert client.waiting_threads == ["thread-1", "thread-2"]
        thread_specs = [
            params["dynamicTools"]
            for method, params in client.calls
            if method == "thread/start"
        ]
        assert len(thread_specs) == 2
        assert all(
            [(tool["namespace"], tool["name"]) for tool in specs]
            == [("product", "inspect")]
            for specs in thread_specs
        )
        assert contexts == {}
        client.release_start_ack.set()
        await asyncio.gather(*sends)
        await asyncio.gather(*client.callback_tasks)
        assert contexts == {
            "call-1": executions[0],
            "call-2": executions[1],
        }
        responses = [json.loads(payload) for payload in client._proc.stdin.writes]  # noqa: SLF001
        assert [response["result"]["success"] for response in responses] == [True, True]
        assert "execution_context" not in client.calls[-1][1]
        assert all("execution_context" not in params for method, params in client.calls if method == "turn/start")
        return client, backend, opened[0], executions[0], contexts

    client, backend, opened, execution, contexts = asyncio.run(run())
    state = client.last_turn(opened.backend_session_id)
    assert state is not None
    assert state.execution_context is execution
    state.done.set()
    client.release_turn(opened.backend_session_id, state.turn_id)
    client.release_thread(opened.backend_session_id)
    assert state.execution_context is None

    before = len(contexts)
    asyncio.run(client._handle_server_request({  # noqa: SLF001
        "jsonrpc": "2.0",
        "id": "late-call",
        "method": "item/tool/call",
        "params": {
            "callId": "late-call",
            "threadId": opened.backend_session_id,
            "turnId": state.turn_id,
            "tool": "inspect",
            "namespace": "product",
            "arguments": {},
        },
    }))
    assert len(contexts) == before
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_legacy_tool_callback_waits_for_turn_ack_and_has_no_context(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        contexts = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: contexts.append(call.execution_context) or DynamicToolResult.text("legacy"),
        )
        client = EarlyContextCallbackClient(registry, hold_ack=True)
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
        ))
        send = asyncio.create_task(backend.send(opened.backend_session_id, "work"))
        await asyncio.wait_for(client.any_callback_waiting.wait(), timeout=1)
        assert contexts == []
        client.release_start_ack.set()
        await send
        await asyncio.gather(*client.callback_tasks)
        return client, contexts

    client, contexts = asyncio.run(run())

    assert contexts == [None]
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is True  # noqa: SLF001


def test_released_context_turns_leave_no_old_thread_ids_and_stale_callbacks_fail():
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: calls.append(call.execution_context) or DynamicToolResult.text("called"),
        )
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        client._proc = SimpleNamespace(stdin=FakeStdin(), returncode=0)  # noqa: SLF001

        for index in range(100):
            thread_id = f"thread-{index}"
            turn_id = f"turn-{index}"
            context = ToolExecutionContext(
                scope=scope,
                execution_id=f"execution-{index}",
                task_id=f"task-{index}",
            )
            client.prepare_turn_context(
                thread_id,
                execution_context=context,
                dynamic_tool_allowlist=(("product", "inspect"),),
            )
            state = client.set_last_turn(thread_id, turn_id)
            client.acknowledge_turn_context(thread_id, turn_id, state)
            client.release_turn(thread_id, turn_id)
            assert state.execution_context is None
            assert state.dynamic_tool_allowlist is None

        assert client._prepared_turn_contexts == {}  # noqa: SLF001
        assert client._turns == {}  # noqa: SLF001
        assert client._last_turn_by_thread == {}  # noqa: SLF001
        assert client._failed_turn_by_thread == {}  # noqa: SLF001
        assert not hasattr(client, "_contextual_thread_ids")

        await client._handle_server_request({  # noqa: SLF001
            "jsonrpc": "2.0",
            "id": "late-call",
            "method": "item/tool/call",
            "params": {
                "callId": "late-call",
                "threadId": "thread-99",
                "turnId": "turn-99",
                "tool": "inspect",
                "namespace": "product",
                "arguments": {},
            },
        })
        return client, calls

    client, calls = asyncio.run(run())

    assert calls == []
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_contextual_tool_call_with_forged_turn_id_fails_closed(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: calls.append(call.execution_context) or DynamicToolResult.text("called"),
        )
        client = EarlyContextCallbackClient(registry, wrong_turn_id=True)
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
            dynamic_tool_allowlist=(("product", "inspect"),),
        ))
        execution = ToolExecutionContext(scope=scope, execution_id="exec-1", task_id="task-1")
        await backend.send_with_context(
            opened.backend_session_id,
            "work",
            execution_context=execution,
            output_schema=None,
            dynamic_tool_allowlist=(("product", "inspect"),),
        )
        await asyncio.gather(*client.callback_tasks)
        return client, calls

    client, calls = asyncio.run(run())

    assert calls == []
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001
    assert not hasattr(client, "_contextual_thread_ids")


def test_contextual_callback_after_client_close_fails_closed():
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: calls.append(call) or DynamicToolResult.text("called"),
        )
        client = JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            dynamic_tools=registry,
        )
        client._proc = SimpleNamespace(stdin=FakeStdin(), returncode=0)  # noqa: SLF001
        client.prepare_turn_context(
            "thread-closed",
            execution_context=ToolExecutionContext(scope=scope, execution_id="exec-1"),
            dynamic_tool_allowlist=(("product", "inspect"),),
        )
        await client.close()
        await client._handle_server_request({  # noqa: SLF001
            "jsonrpc": "2.0",
            "id": "late-call",
            "method": "item/tool/call",
            "params": {
                "callId": "late-call",
                "threadId": "thread-closed",
                "turnId": "turn-1",
                "tool": "inspect",
                "namespace": "product",
                "arguments": {},
            },
        })
        return client, calls

    client, calls = asyncio.run(run())

    assert calls == []
    assert client._prepared_turn_contexts == {}  # noqa: SLF001
    assert client._turns == {}  # noqa: SLF001
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_contextual_callback_cannot_call_unlisted_registered_tool(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        for name in ("inspect", "delete"):
            registry.register(
                DynamicToolSpec(
                    name=name,
                    description=name,
                    input_schema={"type": "object"},
                    namespace="product",
                ),
                lambda call, name=name: calls.append((name, call.execution_context))
                or DynamicToolResult.text("called"),
            )
        client = EarlyContextCallbackClient(registry, callback_tool="delete")
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
            dynamic_tool_allowlist=(("product", "inspect"),),
        ))
        await backend.send_with_context(
            opened.backend_session_id,
            "work",
            execution_context=ToolExecutionContext(scope=scope, execution_id="exec-1", task_id="task-1"),
            output_schema=None,
            dynamic_tool_allowlist=(("product", "inspect"),),
        )
        await asyncio.gather(*client.callback_tasks)
        return client, calls

    client, calls = asyncio.run(run())

    advertised = next(params["dynamicTools"] for method, params in client.calls if method == "thread/start")
    assert [(tool["namespace"], tool["name"]) for tool in advertised] == [("product", "inspect")]
    assert calls == []
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_route_context_advertises_and_allows_no_business_tools(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: calls.append(call) or DynamicToolResult.text("called"),
        )
        client = EarlyContextCallbackClient(registry)
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
            dynamic_tool_allowlist=(),
        ))
        await backend.send_with_context(
            opened.backend_session_id,
            "route",
            execution_context=ToolExecutionContext(scope=scope, execution_id="route-1"),
            output_schema={"type": "object"},
            dynamic_tool_allowlist=(),
        )
        await asyncio.gather(*client.callback_tasks)
        return client, calls

    client, calls = asyncio.run(run())

    assert "dynamicTools" not in client.calls[0][1]
    assert calls == []
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_allowlisted_thread_cannot_bypass_request_filter_via_legacy_send(tmp_path):
    client = FakeCodexClient()
    backend = CodexAppServerBackend(client=client)
    opened = asyncio.run(backend.open(OpenSpec(
        kind="codex_cli",
        cwd=str(tmp_path / "work"),
        conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-a"),
        dynamic_tool_allowlist=(),
    )))

    with pytest.raises(UnsupportedContextualSessionError, match="require send_with_context"):
        asyncio.run(backend.send(opened.backend_session_id, "route"))

    assert [method for method, _ in client.calls] == ["thread/start"]


def test_contextual_callback_after_unacknowledged_failed_start_fails_closed(tmp_path):
    async def run():
        scope = ConversationScope(tenant_id="tenant-a", source_id="chat-a")
        calls = []
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
        registry.register(
            DynamicToolSpec(
                name="inspect",
                description="Inspect a task",
                input_schema={"type": "object"},
                namespace="product",
            ),
            lambda call: calls.append(call) or DynamicToolResult.text("called"),
        )
        client = EarlyContextCallbackClient(registry, fail_start=True)
        backend = CodexAppServerBackend(client=client, dynamic_tools=registry)
        opened = await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=scope,
            dynamic_tool_allowlist=(("product", "inspect"),),
        ))
        with pytest.raises(RuntimeError, match="acknowledgement lost"):
            await backend.send_with_context(
                opened.backend_session_id,
                "work",
                execution_context=ToolExecutionContext(scope=scope, execution_id="exec-1", task_id="task-1"),
                output_schema=None,
                dynamic_tool_allowlist=(("product", "inspect"),),
            )
        await asyncio.gather(*client.callback_tasks)
        return client, calls

    client, calls = asyncio.run(run())

    assert calls == []
    assert json.loads(client._proc.stdin.writes[-1])["result"]["success"] is False  # noqa: SLF001


def test_contextual_send_rejects_context_from_another_scope_before_turn_start(tmp_path):
    fake = FakeCodexClient()
    backend = CodexAppServerBackend(client=fake)
    opened = asyncio.run(backend.open(OpenSpec(
        kind="codex_cli",
        cwd=str(tmp_path / "work"),
        conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-a"),
    )))

    with pytest.raises(TenantBoundaryError, match="does not match"):
        asyncio.run(backend.send_with_context(
            opened.backend_session_id,
            "work",
            execution_context=ToolExecutionContext(
                scope=ConversationScope(tenant_id="tenant-a", source_id="chat-b"),
                execution_id="exec-1",
                task_id="task-1",
            ),
            output_schema=None,
            dynamic_tool_allowlist=None,
        ))

    assert [method for method, _ in fake.calls] == ["thread/start"]


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
        state = client.set_last_turn("thread-1", "turn-1")
        state.start_acknowledged = True
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


def test_json_rpc_client_rejects_dynamic_tools_above_the_conversation_limit():
    async def run() -> tuple[list[str], list[dict], int]:
        started: list[str] = []
        two_started = asyncio.Event()
        later_call_started = asyncio.Event()
        release_tools = asyncio.Event()

        async def slow_tool(call):
            started.append(call.call_id)
            if len(started) == 2:
                two_started.set()
            if call.call_id == "call-3":
                later_call_started.set()
            await release_tools.wait()
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
            max_concurrent_dynamic_tool_calls=2,
        )
        stdin = FakeStdin()
        stdout = FakeStdout()
        client._proc = FakeProcess(stdin=stdin, stdout=stdout)  # noqa: SLF001
        state = client.set_last_turn("thread-1", "turn-1")
        state.start_acknowledged = True

        def tool_request(request_id: int) -> bytes:
            return (
                json.dumps({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "item/tool/call",
                    "params": {
                        "callId": f"call-{request_id}",
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "tool": "slow",
                        "arguments": {},
                    },
                })
                + "\n"
            ).encode()

        for request_id in range(3):
            await stdout.lines.put(tool_request(request_id))
        reader = asyncio.create_task(client._read_stdout())  # noqa: SLF001

        await asyncio.wait_for(two_started.wait(), timeout=0.2)
        await asyncio.sleep(0)
        assert len(client._server_request_tasks) == 2  # noqa: SLF001
        rejection = json.loads(stdin.writes[0].decode())

        release_tools.set()
        await asyncio.gather(*tuple(client._server_request_tasks))  # noqa: SLF001
        await asyncio.sleep(0)
        assert not client._server_request_tasks  # noqa: SLF001

        await stdout.lines.put(tool_request(3))
        await asyncio.wait_for(later_call_started.wait(), timeout=0.2)
        remaining = tuple(client._server_request_tasks)  # noqa: SLF001
        if remaining:
            await asyncio.gather(*remaining)
        await asyncio.sleep(0)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        return started, [rejection], len(client._server_request_tasks)  # noqa: SLF001

    started, writes, remaining_tasks = asyncio.run(run())

    assert started == ["call-0", "call-1", "call-3"]
    assert writes == [{
        "jsonrpc": "2.0",
        "id": 2,
        "result": {
            "success": False,
            "contentItems": [{
                "type": "inputText",
                "text": "Dynamic tool capacity is exhausted for this conversation.",
            }],
        },
    }]
    assert remaining_tasks == 0


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_json_rpc_client_rejects_invalid_dynamic_tool_capacity(limit):
    with pytest.raises(ValueError, match="positive integer"):
        JsonRpcStdioClient(
            command=["codex"],
            cwd=None,
            env={},
            request_timeout_s=1,
            max_concurrent_dynamic_tool_calls=limit,
        )


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
        state = client.set_last_turn("thread-1", "turn-1")
        state.start_acknowledged = True
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
