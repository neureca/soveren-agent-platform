import asyncio
import sqlite3

import pytest

from soveren_agent_platform.sessions import (
    RuntimeSession,
    SessionIndexUpdate,
    SessionInspection,
    SessionInspectorRegistry,
    SQLiteSessionEventStore,
    SQLiteSessionIndexStore,
    SQLiteSessionSnapshotStore,
    SQLiteSessionStore,
    index_store_once,
    run_session_indexer_store_worker,
)
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


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

    async def get(self, session_id: str, *, tenant_id: str, source_id: str):
        session = self.sessions[0]
        if (tenant_id, source_id) != (session.tenant_id, session.source_id):
            return None
        return session if session_id == "rs_1" else None

    async def list_active(
        self,
        *,
        tenant_id: str,
        limit: int,
        after_session_id: str | None = None,
    ):
        if tenant_id != "tenant-a":
            return []
        sessions = sorted(self.sessions, key=lambda session: session.id)
        if after_session_id is not None:
            sessions = [session for session in sessions if session.id > after_session_id]
        return sessions[:limit]

    async def set_status(
        self,
        session_id: str,
        status: str,
        *,
        tenant_id: str,
        source_id: str,
        current_action_id=None,
        last_error=None,
    ):
        return None


class FakeIndexStore:
    def __init__(self) -> None:
        self.inspections: list[SessionInspection] = []
        self.markers: set[str] = set()

    async def index_inspection(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        inspection: SessionInspection,
    ) -> SessionIndexUpdate:
        assert (tenant_id, source_id) == ("tenant-a", "chat-1")
        if inspection.marker and inspection.marker in self.markers:
            return SessionIndexUpdate(recorded=False, snapshot_id=None)
        self.inspections.append(inspection)
        if inspection.marker:
            self.markers.add(inspection.marker)
        return SessionIndexUpdate(recorded=True, snapshot_id=f"rss_{len(self.inspections)}")


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


class MultiSessionStore(FakeSessionStore):
    def __init__(self) -> None:
        self.sessions = [
            RuntimeSession(
                id=f"rs_{index}",
                tenant_id="tenant-a",
                source_id="chat-1",
                kind="codex_cli",
                backend="codex_app_server",
                backend_session_id=f"thread-{index}",
                status="idle",
            )
            for index in range(1, 4)
        ]


class PerSessionInspector:
    async def inspect(self, session: RuntimeSession):
        return SessionInspection(
            session_id=session.id,
            payload_text=f"state for {session.id}",
            marker=f"inspect:{session.id}",
        )


def test_session_indexer_worker_scans_beyond_first_batch():
    async def run() -> list[str]:
        stop_event = asyncio.Event()
        seen: list[str] = []

        class RecordingInspector(PerSessionInspector):
            async def inspect(self, session: RuntimeSession):
                seen.append(session.id)
                if len(seen) == 3:
                    stop_event.set()
                return await super().inspect(session)

        await asyncio.wait_for(
            run_session_indexer_store_worker(
                MultiSessionStore(),
                FakeIndexStore(),
                stop_event,
                tenant_id="tenant-a",
                session_inspectors={"codex_app_server": RecordingInspector()},
                poll_interval_s=0,
                batch_size=2,
            ),
            timeout=1,
        )
        return seen

    assert asyncio.run(run()) == ["rs_1", "rs_2", "rs_3"]


def test_sqlite_session_store_pages_active_sessions_by_stable_id(tmp_path):
    async def run() -> tuple[list[str], list[str], list[str]]:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        session_ids = [
            await sessions.create(
                tenant_id="tenant-a",
                source_id="chat-1",
                kind="codex_cli",
                backend="codex_app_server",
                backend_session_id=f"thread-{index}",
            )
            for index in range(3)
        ]

        first = await sessions.list_active(tenant_id="tenant-a", limit=2)
        second = await sessions.list_active(
            tenant_id="tenant-a",
            limit=2,
            after_session_id=first[-1].id,
        )
        conn.close()
        return (
            sorted(session_ids),
            [session.id for session in first],
            [session.id for session in second],
        )

    expected, first, second = asyncio.run(run())

    assert first == expected[:2]
    assert second == expected[2:]


def test_session_indexer_records_inspection_and_refreshes_snapshot():
    session_store = FakeSessionStore()
    index_store = FakeIndexStore()
    registry = SessionInspectorRegistry()
    registry.register("codex_app_server", FakeInspector())

    first = asyncio.run(
        index_store_once(
            session_store,
            index_store,
            tenant_id="tenant-a",
            session_inspectors=registry,
        )
    )
    second = asyncio.run(
        index_store_once(
            session_store,
            index_store,
            tenant_id="tenant-a",
            session_inspectors=registry,
        )
    )

    assert first == 1
    assert second == 0
    assert index_store.inspections[0].payload_text == "thread summary about routing"
    assert index_store.inspections[0].marker == "inspect:v1"


def test_session_indexer_rejects_inspector_bound_to_another_tenant():
    index_store = FakeIndexStore()

    refreshed = asyncio.run(index_store_once(
        FakeSessionStore(),
        index_store,
        tenant_id="tenant-a",
        session_inspectors={"codex_app_server": WrongTenantInspector()},
    ))

    assert refreshed == 0
    assert index_store.inspections == []


def test_session_inspection_rejects_invalid_direction():
    with pytest.raises(ValueError, match="direction"):
        SessionInspection(
            session_id="rs_1",
            payload_text="invalid",
            direction="sideways",
        )


def test_session_indexer_rejects_mismatched_inspection_session(caplog):
    class WrongSessionInspector:
        async def inspect(self, session: RuntimeSession):
            return SessionInspection(
                session_id="rs_other",
                payload_text="wrong session",
            )

    index_store = FakeIndexStore()

    refreshed = asyncio.run(
        index_store_once(
            FakeSessionStore(),
            index_store,
            tenant_id="tenant-a",
            session_inspectors={"codex_app_server": WrongSessionInspector()},
        )
    )

    assert refreshed == 0
    assert index_store.inspections == []
    assert "session inspection rejected session_id=rs_1" in caplog.text


def test_session_indexer_continues_after_item_persistence_failure(caplog):
    class PartiallyFailingIndexStore:
        def __init__(self) -> None:
            self.attempted: list[str] = []

        async def index_inspection(
            self,
            *,
            session_id: str,
            tenant_id: str,
            source_id: str,
            inspection: SessionInspection,
        ) -> SessionIndexUpdate:
            self.attempted.append(session_id)
            if session_id == "rs_1":
                raise RuntimeError("one session write failed")
            return SessionIndexUpdate(recorded=True, snapshot_id=f"rss_{session_id}")

    index_store = PartiallyFailingIndexStore()

    refreshed = asyncio.run(
        index_store_once(
            MultiSessionStore(),
            index_store,
            tenant_id="tenant-a",
            session_inspectors={"codex_app_server": PerSessionInspector()},
        )
    )

    assert refreshed == 2
    assert index_store.attempted == ["rs_1", "rs_2", "rs_3"]
    assert "session index persistence failed session_id=rs_1" in caplog.text


def test_session_indexer_propagates_inspector_cancellation():
    class CancelledInspector:
        async def inspect(self, session: RuntimeSession):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            index_store_once(
                FakeSessionStore(),
                FakeIndexStore(),
                tenant_id="tenant-a",
                session_inspectors={"codex_app_server": CancelledInspector()},
            )
        )


def test_session_indexer_propagates_persistence_cancellation():
    class CancelledIndexStore:
        async def index_inspection(self, **kwargs):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            index_store_once(
                FakeSessionStore(),
                CancelledIndexStore(),
                tenant_id="tenant-a",
                session_inspectors={"codex_app_server": FakeInspector()},
            )
        )


def test_sqlite_session_index_rolls_back_event_when_snapshot_refresh_fails(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        conn.execute(
            "CREATE TRIGGER fail_snapshot_insert BEFORE INSERT ON runtime_session_context_snapshots"
            " BEGIN SELECT RAISE(ABORT, 'snapshot insert failed'); END"
        )
        inspection = SessionInspection(
            session_id=session_id,
            payload_text="new backend state",
            marker="inspect:new",
        )

        with pytest.raises(sqlite3.IntegrityError, match="snapshot insert failed"):
            await index_store.index_inspection(
                session_id=session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                inspection=inspection,
            )

        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        conn.execute("DROP TRIGGER fail_snapshot_insert")

        retried = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=inspection,
        )

        assert retried.recorded is True
        assert retried.snapshot_id is not None
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_rejects_inspection_for_another_session(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )

        with pytest.raises(ValueError, match="belongs to 'rs_other'"):
            await index_store.index_inspection(
                session_id=session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                inspection=SessionInspection(
                    session_id="rs_other",
                    payload_text="private state",
                ),
            )

        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_deduplicates_marker_outside_recent_window(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        events = SQLiteSessionEventStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        inspection = SessionInspection(
            session_id=session_id,
            payload_text="stable backend state",
            marker="inspect:stable",
        )
        first = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=inspection,
        )
        for index in range(6):
            await events.record(
                session_id=session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                direction="control",
                payload_text=f"newer event {index}",
                marker=f"control:{index}",
            )

        duplicate = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=inspection,
        )

        assert first.recorded is True
        assert duplicate == SessionIndexUpdate(recorded=False, snapshot_id=None)
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ? AND marker = ?",
            (session_id, inspection.marker),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_repairs_marker_without_snapshot(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        events = SQLiteSessionEventStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        marker_event_id = await events.record(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            direction="output",
            payload_text="legacy partial inspection",
            marker="inspect:partial",
        )

        update = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=SessionInspection(
                session_id=session_id,
                payload_text="legacy partial inspection",
                marker="inspect:partial",
            ),
        )

        assert update.recorded is False
        assert update.snapshot_id is not None
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        snapshot = conn.execute(
            "SELECT * FROM runtime_session_context_snapshots WHERE id = ?",
            (update.snapshot_id,),
        ).fetchone()
        assert snapshot["source_event_id"] == marker_event_id
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_repairs_snapshot_older_than_marker(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        events = SQLiteSessionEventStore._from_connection(conn)
        snapshots = SQLiteSessionSnapshotStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        await events.record(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            direction="control",
            payload_text="older state",
        )
        await snapshots.refresh(
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
        )
        marker_event_id = await events.record(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            direction="output",
            payload_text="new inspected state",
            marker="inspect:newer",
        )

        update = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=SessionInspection(
                session_id=session_id,
                payload_text="new inspected state",
                marker="inspect:newer",
            ),
        )

        assert update.recorded is False
        assert update.snapshot_id is not None
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 2
        latest = await snapshots.latest(
            session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
        )
        assert latest is not None
        assert latest.source_event_id == marker_event_id
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_retries_failed_legacy_snapshot_repair(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        events = SQLiteSessionEventStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        await events.record(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            direction="output",
            payload_text="repair me",
            marker="inspect:repair",
        )
        inspection = SessionInspection(
            session_id=session_id,
            payload_text="repair me",
            marker="inspect:repair",
        )
        conn.execute(
            "CREATE TRIGGER fail_repair BEFORE INSERT ON runtime_session_context_snapshots"
            " BEGIN SELECT RAISE(ABORT, 'repair failed'); END"
        )

        with pytest.raises(sqlite3.IntegrityError, match="repair failed"):
            await index_store.index_inspection(
                session_id=session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                inspection=inspection,
            )

        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        conn.execute("DROP TRIGGER fail_repair")

        retried = await index_store.index_inspection(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            inspection=inspection,
        )

        assert retried.recorded is False
        assert retried.snapshot_id is not None
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_rejects_wrong_conversation_boundary(tmp_path):
    async def run() -> None:
        conn = open_sqlite(tmp_path / "app.db")
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        index_store = SQLiteSessionIndexStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )

        with pytest.raises(LookupError, match="requested conversation"):
            await index_store.index_inspection(
                session_id=session_id,
                tenant_id="tenant-a",
                source_id="chat-2",
                inspection=SessionInspection(
                    session_id=session_id,
                    payload_text="private backend state",
                    marker="inspect:private",
                ),
            )

        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 0
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_records_concurrent_marker_once(tmp_path):
    async def run() -> None:
        db_path = tmp_path / "app.db"
        conn = open_sqlite(db_path)
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        conn.close()
        first_store = await SQLiteSessionIndexStore.open(db_path)
        second_store = await SQLiteSessionIndexStore.open(db_path)
        inspection = SessionInspection(
            session_id=session_id,
            payload_text="concurrent backend state",
            marker="inspect:concurrent",
        )
        try:
            results = await asyncio.gather(
                first_store.index_inspection(
                    session_id=session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    inspection=inspection,
                ),
                second_store.index_inspection(
                    session_id=session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    inspection=inspection,
                ),
            )
        finally:
            await first_store.close()
            await second_store.close()

        assert sorted(update.recorded for update in results) == [False, True]
        conn = open_sqlite(db_path)
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ? AND marker = ?",
            (session_id, inspection.marker),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        conn.close()

    asyncio.run(run())


def test_sqlite_session_index_repairs_concurrent_legacy_marker_once(tmp_path):
    async def run() -> None:
        db_path = tmp_path / "app.db"
        conn = open_sqlite(db_path)
        apply_platform_migrations(conn)
        sessions = SQLiteSessionStore._from_connection(conn)
        events = SQLiteSessionEventStore._from_connection(conn)
        session_id = await sessions.create(
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="codex_cli",
            backend="codex_app_server",
            backend_session_id="thread-1",
        )
        await events.record(
            session_id=session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            direction="output",
            payload_text="concurrent repair",
            marker="inspect:repair-concurrent",
        )
        conn.close()
        first_store = await SQLiteSessionIndexStore.open(db_path)
        second_store = await SQLiteSessionIndexStore.open(db_path)
        inspection = SessionInspection(
            session_id=session_id,
            payload_text="concurrent repair",
            marker="inspect:repair-concurrent",
        )
        try:
            results = await asyncio.gather(
                first_store.index_inspection(
                    session_id=session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    inspection=inspection,
                ),
                second_store.index_inspection(
                    session_id=session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    inspection=inspection,
                ),
            )
        finally:
            await first_store.close()
            await second_store.close()

        assert sum(update.snapshot_id is not None for update in results) == 1
        assert all(update.recorded is False for update in results)
        conn = open_sqlite(db_path)
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM runtime_session_context_snapshots WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0] == 1
        conn.close()

    asyncio.run(run())
