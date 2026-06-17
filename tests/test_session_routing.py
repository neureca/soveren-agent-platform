import asyncio

from agent_platform.sessions.events import record_session_event
from agent_platform.sessions.routing import DeterministicSessionRouter, SessionRouteRequest
from agent_platform.sessions.snapshots import latest_snapshot, refresh_snapshot, snapshot_keywords
from agent_platform.sessions.store import insert_session
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


def test_refresh_snapshot_indexes_session_events(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-1",
        title="agent platform extraction",
        cwd="/tmp/agent-platform",
        metadata={"branch": "feature/platform"},
        now=100,
    )
    record_session_event(
        conn,
        session_id=session_id,
        direction="input",
        payload_text="continue batching runtime work in src/agent_platform/batching/store.py",
        now=101,
    )
    record_session_event(
        conn,
        session_id=session_id,
        direction="output",
        payload_text="batching store updated",
        now=102,
    )

    snapshot_id = refresh_snapshot(conn, session_id, now=103)
    snapshot = latest_snapshot(conn, session_id)

    assert snapshot_id is not None
    assert snapshot is not None
    assert snapshot["topic_key"] == "agent platform extraction"
    assert "batching" in snapshot_keywords(snapshot)
    assert "src/agent_platform/batching/store.py" in snapshot["files_json"]


def test_deterministic_router_routes_existing_semantic_match(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    target_session = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-target",
        owner_id="user-1",
        title="agent platform batching",
        cwd="/tmp/agent-platform",
        status="idle",
        now=100,
    )
    other_session = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-other",
        owner_id="user-1",
        title="unrelated billing",
        cwd="/tmp/billing",
        status="idle",
        now=101,
    )
    record_session_event(
        conn,
        session_id=target_session,
        direction="input",
        payload_text="work on durable batching and runtime planner",
        now=102,
    )
    refresh_snapshot(conn, target_session, now=103)
    refresh_snapshot(conn, other_session, now=104)

    result = asyncio.run(
        DeterministicSessionRouter(conn).route(
            SessionRouteRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                preferred_kind="codex_cli",
                user_id="user-1",
                text="продолжи batching runtime",
            )
        )
    )
    audit = conn.execute("SELECT * FROM runtime_session_route_decisions").fetchone()

    assert result.hint.action == "route_existing"
    assert result.hint.session_id == target_session
    assert result.snapshots[0].session_id == target_session
    assert audit["selected_session_id"] == target_session


def test_deterministic_router_asks_when_only_recency_matches(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-1",
        title="billing",
        cwd="/tmp/billing",
        status="idle",
        now=100,
    )

    result = asyncio.run(
        DeterministicSessionRouter(conn).route(
            SessionRouteRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                preferred_kind="codex_cli",
                text="совсем другая тема без совпадений",
            )
        )
    )

    assert result.hint.action == "ask_clarification"
    assert result.hint.session_id is None


def test_deterministic_router_no_match_without_candidates(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    result = asyncio.run(
        DeterministicSessionRouter(conn).route(
            SessionRouteRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                preferred_kind="codex_cli",
                text="anything",
            )
        )
    )

    assert result.hint.action == "no_match"
    assert result.snapshots == []

