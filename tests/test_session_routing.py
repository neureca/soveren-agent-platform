import asyncio
import threading

import soveren_agent_platform.sessions.routing as session_routing
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.routing import DeterministicSessionRouter, SessionRouteRequest
from soveren_agent_platform.sessions.snapshots import latest_snapshot, refresh_snapshot, snapshot_keywords
from soveren_agent_platform.sessions.sqlite import SQLiteSessionEventStore, SQLiteSessionSnapshotStore
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


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
        cwd="/tmp/soveren-agent-platform",
        metadata={"branch": "feature/platform"},
        now=100,
    )
    record_session_event(
        conn,
        session_id=session_id,
        direction="input",
        payload_text="continue batching runtime work in src/soveren_agent_platform/batching/store.py",
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
    assert "src/soveren_agent_platform/batching/store.py" in snapshot["files_json"]


def test_session_event_and_snapshot_stores_expose_typed_ports(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-1",
        title="runtime routing",
        cwd="/tmp/soveren-agent-platform",
        metadata={"branch": "feature/platform"},
        now=100,
    )
    event_store = SQLiteSessionEventStore._from_connection(conn)
    snapshot_store = SQLiteSessionSnapshotStore._from_connection(conn)

    event_id = asyncio.run(event_store.record(
        session_id=session_id,
        direction="input",
        payload_text="inspect session snapshots in routing.py",
        marker="m1",
    ))
    events = asyncio.run(event_store.recent(session_id, limit=10))
    snapshot_id = asyncio.run(snapshot_store.refresh(session_id))
    snapshot = asyncio.run(snapshot_store.latest(session_id))

    assert events[0].id == event_id
    assert events[0].marker == "m1"
    assert snapshot_id is not None
    assert snapshot is not None
    assert snapshot.topic_key == "runtime routing"
    assert "routing.py" in snapshot.files


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
        cwd="/tmp/soveren-agent-platform",
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
        DeterministicSessionRouter._from_connection(conn).route(
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
        DeterministicSessionRouter._from_connection(conn).route(
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
        DeterministicSessionRouter._from_connection(conn).route(
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


def test_deterministic_router_runs_sqlite_work_off_event_loop(tmp_path, monkeypatch):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_loop_thread = threading.get_ident()
    sqlite_threads: list[int] = []
    original = session_routing._active_candidates

    def recording_candidates(*args, **kwargs):
        sqlite_threads.append(threading.get_ident())
        return original(*args, **kwargs)

    monkeypatch.setattr(session_routing, "_active_candidates", recording_candidates)

    asyncio.run(
        DeterministicSessionRouter._from_connection(conn).route(
            SessionRouteRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                text="anything",
            )
        )
    )

    assert sqlite_threads
    assert sqlite_threads[0] != event_loop_thread
