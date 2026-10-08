import asyncio
import json
import os
import re
from types import SimpleNamespace

import pytest

from soveren_agent_platform.conversation_history import (
    CONVERSATION_HISTORY_TOOL_NAMESPACE,
    SQLiteConversationHistoryStore,
    register_conversation_history_tools,
)
from soveren_agent_platform.cron import SCHEDULE_TOOL_NAMESPACE, SQLiteCronStore, register_scheduled_job_tools
from soveren_agent_platform.memory import MEMORY_TOOL_NAMESPACE, SQLiteMemoryStore, register_memory_tools
from soveren_agent_platform.sessions import (
    SESSION_TOOL_NAMESPACE,
    CodexAppServerBackend,
    CodexAppServerError,
    ConversationScope,
    DynamicToolRegistry,
    DynamicToolSpec,
    OpenSpec,
    SQLiteSessionDirectoryTools,
)
from soveren_agent_platform.sessions.backends.codex_app_server import JsonRpcStdioClient
from soveren_agent_platform.sessions.backends.codex_tools import normalize_dynamic_tool_specs
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


@pytest.fixture
def platform_tools(tmp_path):
    conn = open_sqlite(tmp_path / "platform.db")
    apply_platform_migrations(conn)
    registry = DynamicToolRegistry()
    scope = {"tenant_id": "tenant-a", "source_id": "chat-a"}
    register_conversation_history_tools(registry, SQLiteConversationHistoryStore._from_connection(conn), **scope)
    register_scheduled_job_tools(registry, SQLiteCronStore._from_connection(conn), **scope)
    SQLiteSessionDirectoryTools._from_connection(conn).register(registry, **scope)
    register_memory_tools(registry, SQLiteMemoryStore._from_connection(conn), allow_write=True, **scope)
    try:
        yield registry
    finally:
        conn.close()


class NamespaceContractClient:
    """The flat dynamicTools contract of Codex 0.143.0 in deploy/sandbox/Dockerfile."""

    def __init__(self):
        self.calls = []

    async def request(self, method, params):
        self.calls.append((method, params))
        assert method == "thread/start"
        for spec in params["dynamicTools"]:
            # The old permissive fake accepted dotted namespaces and hid the outage.
            assert re.fullmatch(r"[a-zA-Z0-9_-]+", spec["namespace"])
        return {"thread": {"id": "thread-1"}}


class RecordingStdin:
    def __init__(self):
        self.writes = []

    def write(self, data):
        self.writes.append(json.loads(data))

    async def drain(self):
        pass


def test_platform_namespaces_are_public_app_server_identifiers():
    assert (
        CONVERSATION_HISTORY_TOOL_NAMESPACE,
        SCHEDULE_TOOL_NAMESPACE,
        SESSION_TOOL_NAMESPACE,
        MEMORY_TOOL_NAMESPACE,
    ) == ("platform_conversation", "platform_schedules", "platform_sessions", "platform_memory")


def test_backend_registers_platform_namespaces_and_dispatches_wire_calls(platform_tools, tmp_path):
    async def run():
        contract = NamespaceContractClient()
        backend = CodexAppServerBackend(client=contract, dynamic_tools=platform_tools)
        await backend.open(OpenSpec(
            kind="codex_cli",
            cwd=str(tmp_path / "work"),
            conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-a"),
        ))
        specs = contract.calls[0][1]["dynamicTools"]
        assert {(spec["namespace"], spec["name"]) for spec in specs} == {
            ("platform_conversation", "read_recent_messages"),
            ("platform_conversation", "search_message_history"),
            ("platform_schedules", "list_scheduled_jobs"),
            ("platform_schedules", "cancel_scheduled_job"),
            ("platform_sessions", "list_runtime_sessions"),
            ("platform_sessions", "search_session_snapshots"),
            ("platform_sessions", "get_session_context"),
            ("platform_memory", "search_memory"),
            ("platform_memory", "get_memory"),
            ("platform_memory", "remember"),
            ("platform_memory", "forget"),
        }
        client = JsonRpcStdioClient(
            command=["codex"], cwd=None, env={}, request_timeout_s=1, dynamic_tools=platform_tools,
        )
        stdin = RecordingStdin()
        client._proc = SimpleNamespace(stdin=stdin)
        for request_id, (namespace, tool, result_key) in enumerate([
            ("platform_conversation", "read_recent_messages", "messages"),
            ("platform_schedules", "list_scheduled_jobs", "jobs"),
            ("platform_sessions", "list_runtime_sessions", "sessions"),
            ("platform_memory", "search_memory", "memories"),
        ], start=1):
            await client._handle_server_request({
                "jsonrpc": "2.0", "id": request_id, "method": "item/tool/call",
                "params": {
                    "callId": f"call-{request_id}", "threadId": "thread-1", "turnId": "turn-1",
                    "namespace": namespace, "tool": tool, "arguments": {},
                },
            })
            response = stdin.writes[-1]
            assert response["id"] == request_id
            assert response["result"]["success"] is True
            payload = json.loads(response["result"]["contentItems"][0]["text"])
            assert payload[result_key] == []
    asyncio.run(run())


@pytest.mark.parametrize("namespace", [
    "platform.conversation", "platform.schedules", "platform.sessions", "platform.memory", "platform.database",
    "pulsy.database", "", "has space", "slash/name", "unicode_я", "trailing\n",
])
def test_invalid_namespace_rejected_for_typed_and_raw_specs(namespace):
    with pytest.raises(ValueError, match="dynamic tool namespace"):
        DynamicToolSpec(name="probe", description="probe", input_schema={"type": "object"}, namespace=namespace)
    with pytest.raises(ValueError, match="dynamic tool namespace"):
        normalize_dynamic_tool_specs([{
            "name": "probe", "description": "probe", "inputSchema": {"type": "object"}, "namespace": namespace,
        }])


@pytest.mark.parametrize("namespace", [None, "platform", "platform_database", "App-TOOLS_123"])
def test_valid_namespace_serialization_is_unchanged(namespace):
    spec = DynamicToolSpec(name="probe", description="probe", input_schema={"type": "object"}, namespace=namespace)
    payload = spec.to_app_server()
    assert payload.get("namespace") == namespace
    assert ("namespace" in payload) is (namespace is not None)
    assert normalize_dynamic_tool_specs([payload]) == [payload]


def test_backend_rejects_invalid_raw_namespace_before_thread_start(tmp_path):
    client = NamespaceContractClient()
    backend = CodexAppServerBackend(client=client, dynamic_tools=[{
        "name": "probe", "description": "probe", "inputSchema": {"type": "object"},
        "namespace": "platform.database",
    }])
    with pytest.raises(ValueError, match="dynamic tool namespace"):
        asyncio.run(backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work"))))
    assert client.calls == []


def test_same_tool_name_dispatches_to_its_registered_namespace():
    registry = DynamicToolRegistry()
    for namespace in ["platform_memory", "platform_database"]:
        registry.register(
            DynamicToolSpec(name="lookup", description="lookup", input_schema={"type": "object"}, namespace=namespace),
            lambda call: {"namespace": call.namespace},
        )
    for namespace in ["platform_memory", "platform_database"]:
        result = asyncio.run(registry.call({
            "callId": "call-1", "threadId": "thread-1", "turnId": "turn-1",
            "namespace": namespace, "tool": "lookup", "arguments": {},
        }))
        assert result["success"] is True
        assert json.loads(result["contentItems"][0]["text"]) == {"namespace": namespace}
    result = asyncio.run(registry.call({
        "callId": "call-2", "threadId": "thread-1", "turnId": "turn-1",
        "namespace": "platform.memory", "tool": "lookup", "arguments": {},
    }))
    assert result["success"] is False


@pytest.mark.skipif(
    not os.environ.get("SOVEREN_TEST_CODEX_BINARY"),
    reason="requires an explicit Codex app-server binary",
)
def test_real_app_server_accepts_all_platform_dynamic_tools(platform_tools, tmp_path):
    # No auth, model turn, network request, or production thread is needed.
    async def run():
        codex_home = tmp_path / "codex-home"
        codex_home.mkdir()
        backend = CodexAppServerBackend(
            command=[os.environ["SOVEREN_TEST_CODEX_BINARY"], "app-server", "--listen", "stdio://"],
            codex_home=codex_home,
            dynamic_tools=platform_tools,
            request_timeout_s=15,
        )
        try:
            opened = await backend.open(OpenSpec(
                kind="codex_cli", cwd=str(tmp_path / "work"),
                conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-a"),
            ))
            assert opened.backend_session_id
            assert backend._client is not None
            # Bypass the local guard to keep its oracle independent of our regex.
            for namespace in [
                "platform.conversation", "platform.schedules", "platform.sessions",
                "platform.memory", "platform.database",
            ]:
                with pytest.raises(CodexAppServerError, match="dynamic tool namespace must match"):
                    await backend._client.request("thread/start", {
                        "cwd": str(tmp_path / "work"),
                        "dynamicTools": [{
                            "name": "probe", "description": "probe", "inputSchema": {"type": "object"},
                            "namespace": namespace,
                        }],
                    })
        finally:
            await backend.shutdown()
    asyncio.run(run())
