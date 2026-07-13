import asyncio
import json

from soveren_agent_platform.sessions import (
    SESSION_TOOL_NAMESPACE,
    DynamicToolRegistry,
    RuntimeSession,
    SessionInspection,
    SessionInspectorRegistry,
)
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.snapshots import refresh_snapshot
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.sessions.tools import register_session_directory_tools
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class FakeInspector:
    async def inspect(self, session: RuntimeSession):
        return SessionInspection(
            session_id=session.id,
            payload_text="refreshed codex app server thread context",
            marker="inspect:v1",
        )


def _tool_params(tool: str, arguments: dict):
    return {
        "callId": "call-1",
        "threadId": "thread-1",
        "turnId": "turn-1",
        "namespace": SESSION_TOOL_NAMESPACE,
        "tool": tool,
        "arguments": arguments,
    }


def _json_result(result: dict):
    return json.loads(result["contentItems"][0]["text"])


def test_session_directory_tools_read_generalized_index(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex_app_server",
        backend_session_id="thread-1",
        title="agent platform sessions",
        cwd="/tmp/soveren-agent-platform",
        now=100,
    )
    other_session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-2",
        kind="codex_cli",
        backend="codex_app_server",
        backend_session_id="thread-2",
        title="other private chat",
        cwd="/tmp/other",
        now=100,
    )
    record_session_event(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        direction="output",
        payload_text="routing snapshots and mailbox worker",
        now=101,
    )
    refresh_snapshot(
        conn,
        session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=102,
    )
    registry = DynamicToolRegistry()
    register_session_directory_tools(registry, conn, tenant_id="tenant-a", source_id="chat-1")

    listed = _json_result(asyncio.run(registry.call(_tool_params("list_runtime_sessions", {}))))
    found = _json_result(asyncio.run(registry.call(_tool_params("search_session_snapshots", {"query": "mailbox"}))))
    context = _json_result(asyncio.run(registry.call(_tool_params("get_session_context", {"session_id": session_id}))))
    cross_source_list = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "list_runtime_sessions",
                    {"source_id": "chat-2"},
                )
            )
        )
    )
    cross_source_context = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "get_session_context",
                    {"session_id": other_session_id},
                )
            )
        )
    )

    assert listed["sessions"][0]["session_id"] == session_id
    assert found["sessions"][0]["session_id"] == session_id
    assert context["session"]["snapshot"]["topic_key"] == "agent platform sessions"
    assert context["events"][0]["payload_text"] == "routing snapshots and mailbox worker"
    assert "action_id" not in context["events"][0]
    assert "marker" not in context["events"][0]
    assert "last_error" not in context["session"]
    assert [item["session_id"] for item in cross_source_list["sessions"]] == [session_id]
    assert cross_source_context["session"] is None
    assert "source_id" not in listed["sessions"][0]
    assert "backend_session_id" not in listed["sessions"][0]
    list_spec = next(spec for spec in registry.app_server_specs() if spec["name"] == "list_runtime_sessions")
    assert "source_id" not in list_spec["inputSchema"]["properties"]


def test_refresh_session_candidate_tool_uses_registered_inspector(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex_app_server",
        backend_session_id="thread-1",
        now=100,
    )
    inspectors = SessionInspectorRegistry()
    inspectors.register("codex_app_server", FakeInspector())
    registry = DynamicToolRegistry()
    register_session_directory_tools(
        registry,
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        session_inspectors=inspectors,
    )

    first = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "refresh_session_candidate",
                    {"session_id": session_id},
                )
            )
        )
    )
    second = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "refresh_session_candidate",
                    {"session_id": session_id},
                )
            )
        )
    )

    assert first["refreshed"] is True
    assert first["snapshot_id"].startswith("rss_")
    assert second == {"refreshed": False, "reason": "already current", "session_id": session_id}
