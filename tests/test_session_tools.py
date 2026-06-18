import asyncio
import json

from agent_platform.sessions import (
    SESSION_TOOL_NAMESPACE,
    DynamicToolRegistry,
    RuntimeSession,
    SessionInspection,
    SessionInspectorRegistry,
    record_session_event,
    register_session_directory_tools,
)
from agent_platform.sessions.snapshots import refresh_snapshot
from agent_platform.sessions.store import insert_session
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


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
        cwd="/tmp/agent-platform",
        now=100,
    )
    record_session_event(
        conn,
        session_id=session_id,
        direction="output",
        payload_text="routing snapshots and mailbox worker",
        now=101,
    )
    refresh_snapshot(conn, session_id, now=102)
    registry = DynamicToolRegistry()
    register_session_directory_tools(registry, conn, tenant_id="tenant-a", source_id="chat-1")

    listed = _json_result(asyncio.run(registry.call(_tool_params("list_runtime_sessions", {}))))
    found = _json_result(asyncio.run(registry.call(_tool_params("search_session_snapshots", {"query": "mailbox"}))))
    context = _json_result(asyncio.run(registry.call(_tool_params("get_session_context", {"session_id": session_id}))))

    assert listed["sessions"][0]["session_id"] == session_id
    assert found["sessions"][0]["session_id"] == session_id
    assert context["session"]["snapshot"]["topic_key"] == "agent platform sessions"
    assert context["events"][0]["payload_text"] == "routing snapshots and mailbox worker"


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
        session_inspectors=inspectors,
    )

    first = _json_result(asyncio.run(registry.call(_tool_params(
        "refresh_session_candidate",
        {"session_id": session_id},
    ))))
    second = _json_result(asyncio.run(registry.call(_tool_params(
        "refresh_session_candidate",
        {"session_id": session_id},
    ))))

    assert first["refreshed"] is True
    assert first["snapshot_id"].startswith("rss_")
    assert second == {"refreshed": False, "reason": "already current", "session_id": session_id}
