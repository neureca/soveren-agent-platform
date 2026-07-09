import asyncio
import concurrent.futures
import json
import threading

from soveren_agent_platform.memory import (
    MEMORY_TOOL_NAMESPACE,
    SQLiteMemoryStore,
    get_memory,
    remember,
    search_memory,
)
from soveren_agent_platform.memory.tools import MemoryToolAccess, register_memory_tools
from soveren_agent_platform.sessions import DynamicToolRegistry
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def _tool_params(tool: str, arguments: dict):
    return {
        "callId": "call-1",
        "threadId": "thread-1",
        "turnId": "turn-1",
        "namespace": MEMORY_TOOL_NAMESPACE,
        "tool": tool,
        "arguments": arguments,
    }


def _json_result(result: dict):
    return json.loads(result["contentItems"][0]["text"])


def test_memory_store_remembers_searches_and_forgets_records(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    memory_id, created = remember(
        conn,
        tenant_id="tenant-a",
        scope="user",
        subject_id="telegram:123",
        text="User prefers ClickUp tasks grouped by project.",
        kind="preference",
        metadata={"source": "telegram"},
        idempotency_key="memory:preference:1",
        now=100,
    )
    duplicate_id, duplicate_created = remember(
        conn,
        tenant_id="tenant-a",
        scope="user",
        subject_id="telegram:123",
        text="duplicate ignored",
        idempotency_key="memory:preference:1",
        now=101,
    )

    found = search_memory(
        conn,
        tenant_id="tenant-a",
        scope="user",
        subject_id="telegram:123",
        query="ClickUp project",
        now=102,
    )

    assert created is True
    assert duplicate_id == memory_id
    assert duplicate_created is False
    assert [item.id for item in found] == [memory_id]
    assert found[0].metadata == {"source": "telegram"}

    store = SQLiteMemoryStore(conn)
    assert asyncio.run(store.forget(memory_id, tenant_id="tenant-a")) is True
    assert search_memory(conn, tenant_id="tenant-a", query="ClickUp", now=103) == []


def test_memory_tools_are_read_only_by_default(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    remember(
        conn,
        tenant_id="tenant-a",
        scope="source",
        subject_id="chat-1",
        text="Remember deployment window preference.",
        now=100,
    )
    registry = DynamicToolRegistry()
    register_memory_tools(
        registry,
        SQLiteMemoryStore(conn),
        tenant_id="tenant-a",
        access=MemoryToolAccess(scope="source", subject_id="chat-1"),
    )

    search = _json_result(asyncio.run(registry.call(_tool_params("search_memory", {"query": "deployment"}))))
    write = asyncio.run(registry.call(_tool_params("remember", {"text": "should not write"})))

    assert search["memories"][0]["text"] == "Remember deployment window preference."
    assert write["success"] is False
    assert "not registered" in write["contentItems"][0]["text"]


def test_memory_tools_can_write_when_enabled(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DynamicToolRegistry()
    register_memory_tools(
        registry,
        SQLiteMemoryStore(conn),
        tenant_id="tenant-a",
        access=MemoryToolAccess(scope="source", subject_id="chat-1"),
        allow_write=True,
    )

    remembered = _json_result(asyncio.run(registry.call(_tool_params(
        "remember",
        {"text": "Use concise status updates.", "kind": "preference"},
    ))))
    fetched = _json_result(asyncio.run(registry.call(_tool_params(
        "get_memory",
        {"memory_id": remembered["memory_id"]},
    ))))
    forgotten = _json_result(asyncio.run(registry.call(_tool_params(
        "forget",
        {"memory_id": remembered["memory_id"]},
    ))))

    assert remembered["created"] is True
    assert fetched["memory"]["kind"] == "preference"
    assert forgotten["forgotten"] is True
    remember_spec = next(
        spec for spec in registry.app_server_specs()
        if spec["name"] == "remember"
    )
    properties = remember_spec["inputSchema"]["properties"]
    assert {"source_id", "source_event_id", "created_by"}.isdisjoint(properties)


def test_memory_tools_enforce_registered_subject_access(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    allowed_id, _ = remember(
        conn,
        tenant_id="tenant-a",
        scope="source",
        subject_id="chat-1",
        text="Allowed chat memory.",
        now=100,
    )
    other_id, _ = remember(
        conn,
        tenant_id="tenant-a",
        scope="source",
        subject_id="chat-2",
        text="Other chat memory.",
        now=100,
    )
    registry = DynamicToolRegistry()
    register_memory_tools(
        registry,
        SQLiteMemoryStore(conn),
        tenant_id="tenant-a",
        access=MemoryToolAccess(scope="source", subject_id="chat-1"),
        allow_write=True,
    )

    search = asyncio.run(registry.call(_tool_params(
        "search_memory",
        {"query": "memory", "subject_id": "chat-2"},
    )))
    fetched_allowed = _json_result(asyncio.run(registry.call(_tool_params(
        "get_memory",
        {"memory_id": allowed_id},
    ))))
    fetched_other = _json_result(asyncio.run(registry.call(_tool_params(
        "get_memory",
        {"memory_id": other_id},
    ))))
    remembered_override = asyncio.run(registry.call(_tool_params(
        "remember",
        {"text": "wrong subject write", "subject_id": "chat-2"},
    )))

    assert search["success"] is False
    assert "outside the registered memory access policy" in search["contentItems"][0]["text"]
    assert fetched_allowed["memory"]["id"] == allowed_id
    assert fetched_other["memory"] is None
    assert remembered_override["success"] is False


def test_memory_tool_payload_redacts_routing_and_nested_channel_identifiers(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    memory_id, _ = remember(
        conn,
        tenant_id="tenant-a",
        scope="source",
        subject_id="telegram:123",
        text="Safe memory text.",
        metadata={"chat_id": 123, "nested": {"user_id": 789}, "safe": "visible"},
        source_id="123",
        source_event_id="456",
        created_by="789",
        now=100,
    )
    registry = DynamicToolRegistry()
    register_memory_tools(
        registry,
        SQLiteMemoryStore(conn),
        tenant_id="tenant-a",
        access=MemoryToolAccess(scope="source", subject_id="telegram:123"),
    )

    fetched = _json_result(asyncio.run(registry.call(_tool_params(
        "get_memory",
        {"memory_id": memory_id},
    ))))["memory"]

    assert fetched["text"] == "Safe memory text."
    assert fetched["metadata"] == {
        "chat_id": "[redacted:chat_id]",
        "nested": {"user_id": "[redacted:user_id]"},
        "safe": "visible",
    }
    for key in ("tenant_id", "subject_id", "source_id", "source_event_id", "created_by"):
        assert key not in fetched


def test_memory_get_and_tool_hide_expired_records(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    memory_id, _ = remember(
        conn,
        tenant_id="tenant-a",
        scope="source",
        subject_id="chat-1",
        text="Expired private memory.",
        expires_at=2,
        now=1,
    )
    registry = DynamicToolRegistry()
    register_memory_tools(
        registry,
        SQLiteMemoryStore(conn),
        tenant_id="tenant-a",
        access=MemoryToolAccess(scope="source", subject_id="chat-1"),
        allow_write=True,
    )

    fetched = _json_result(asyncio.run(registry.call(_tool_params(
        "get_memory",
        {"memory_id": memory_id},
    ))))
    forgotten = _json_result(asyncio.run(registry.call(_tool_params(
        "forget",
        {"memory_id": memory_id},
    ))))
    deleted_at = conn.execute(
        "SELECT deleted_at FROM memory_records WHERE id = ?",
        (memory_id,),
    ).fetchone()["deleted_at"]

    assert get_memory(conn, memory_id, tenant_id="tenant-a", now=2) is None
    assert fetched["memory"] is None
    assert forgotten == {"memory_id": memory_id, "forgotten": False}
    assert deleted_at is None


def test_memory_remember_is_idempotent_across_concurrent_connections(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    conn.close()
    barrier = threading.Barrier(2)

    def write() -> tuple[str, bool]:
        worker_conn = open_sqlite(db_path)
        try:
            barrier.wait()
            return remember(
                worker_conn,
                tenant_id="tenant-a",
                scope="source",
                subject_id="chat-1",
                text="Idempotent memory.",
                idempotency_key="memory:concurrent:1",
            )
        finally:
            worker_conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: write(), range(2)))

    assert len({memory_id for memory_id, _ in results}) == 1
    assert sorted(created for _, created in results) == [False, True]
