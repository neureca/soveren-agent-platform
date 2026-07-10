import asyncio

from soveren_agent_platform.sessions import (
    RuntimeSession,
    RuntimeSessionEvent,
    SessionInspection,
    SessionInspectorRegistry,
    index_store_once,
)


class FakeSessionStore:
    def __init__(self) -> None:
        self.sessions = [
            RuntimeSession(
                id="rs_1",
                tenant_id="tenant-a",
                source_id="chat-1",
                kind="codex_cli",
                backend="codex_app_server",
                backend_session_id="thread-1",
                status="idle",
            )
        ]

    async def get(self, session_id: str):
        return self.sessions[0] if session_id == "rs_1" else None

    async def list_active(self, *, tenant_id: str, limit: int):
        return self.sessions[:limit] if tenant_id == "tenant-a" else []

    async def set_status(self, session_id: str, status: str, *, current_action_id=None, last_error=None):
        return None


class FakeEventStore:
    def __init__(self) -> None:
        self.events: list[RuntimeSessionEvent] = []

    async def record(self, *, session_id: str, direction: str, payload_text: str, action_id=None, marker=None):
        event = RuntimeSessionEvent(
            id=f"rse_{len(self.events) + 1}",
            session_id=session_id,
            direction=direction,
            payload_text=payload_text,
            action_id=action_id,
            marker=marker,
        )
        self.events.insert(0, event)
        return event.id

    async def recent(self, session_id: str, *, limit: int):
        return [event for event in self.events if event.session_id == session_id][:limit]


class FakeSnapshotStore:
    def __init__(self) -> None:
        self.refreshed: list[str] = []

    async def refresh(self, session_id: str):
        self.refreshed.append(session_id)
        return f"rss_{len(self.refreshed)}"

    async def latest(self, session_id: str):
        return None


class FakeInspector:
    async def inspect(self, session: RuntimeSession):
        return SessionInspection(
            session_id=session.id,
            direction="output",
            payload_text="thread summary about routing",
            marker="inspect:v1",
        )


class WrongTenantInspector(FakeInspector):
    tenant_id = "tenant-b"


def test_session_indexer_records_inspection_and_refreshes_snapshot():
    session_store = FakeSessionStore()
    event_store = FakeEventStore()
    snapshot_store = FakeSnapshotStore()
    registry = SessionInspectorRegistry()
    registry.register("codex_app_server", FakeInspector())

    first = asyncio.run(
        index_store_once(
            session_store,
            event_store,
            snapshot_store,
            tenant_id="tenant-a",
            session_inspectors=registry,
        )
    )
    second = asyncio.run(
        index_store_once(
            session_store,
            event_store,
            snapshot_store,
            tenant_id="tenant-a",
            session_inspectors=registry,
        )
    )

    assert first == 1
    assert second == 0
    assert event_store.events[0].payload_text == "thread summary about routing"
    assert event_store.events[0].marker == "inspect:v1"
    assert snapshot_store.refreshed == ["rs_1"]


def test_session_indexer_rejects_inspector_bound_to_another_tenant():
    event_store = FakeEventStore()
    snapshot_store = FakeSnapshotStore()

    refreshed = asyncio.run(index_store_once(
        FakeSessionStore(),
        event_store,
        snapshot_store,
        tenant_id="tenant-a",
        session_inspectors={"codex_app_server": WrongTenantInspector()},
    ))

    assert refreshed == 0
    assert event_store.events == []
    assert snapshot_store.refreshed == []
