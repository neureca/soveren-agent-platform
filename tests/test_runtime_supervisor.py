import asyncio

import pytest

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.app_api import AgentPlatformApp, WorkerSpec, WorkerSupervisor
from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.sessions import SessionBackendRegistry, SessionInspectorRegistry
from soveren_agent_platform.storage.migrations import assert_platform_schema
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_worker_supervisor_starts_and_stops_workers():
    events: list[str] = []

    async def worker(stop_event: asyncio.Event) -> None:
        events.append("started")
        await stop_event.wait()
        events.append("stopped")

    async def run() -> None:
        supervisor = WorkerSupervisor([WorkerSpec("test", worker)])
        await supervisor.start()
        await asyncio.sleep(0)
        await supervisor.stop()

    asyncio.run(run())

    assert events == ["started", "stopped"]


def test_worker_supervisor_propagates_worker_failure_and_stops_siblings():
    events: list[str] = []

    async def failing_worker(stop_event: asyncio.Event) -> None:
        events.append("failing-started")
        raise RuntimeError("boom")

    async def sibling_worker(stop_event: asyncio.Event) -> None:
        events.append("sibling-started")
        try:
            await stop_event.wait()
        finally:
            events.append("sibling-stopped")

    async def run() -> None:
        supervisor = WorkerSupervisor([
            WorkerSpec("failing", failing_worker),
            WorkerSpec("sibling", sibling_worker),
        ])
        with pytest.raises(RuntimeError, match="boom"):
            await supervisor.wait()

    asyncio.run(run())

    assert "failing-started" in events
    assert "sibling-stopped" in events


class NoopAgentHandler:
    async def handle(self, event: AgentEvent) -> None:
        return None


class NoopCronHandler:
    async def handle(self, job: CronJob) -> None:
        return None


class ManagedSessionBackend:
    name = "managed"

    def __init__(self) -> None:
        self.shutdown_calls = 0

    async def open(self, spec):
        raise NotImplementedError

    async def send(self, backend_session_id, prompt):
        raise NotImplementedError

    async def capture(self, backend_session_id):
        raise NotImplementedError

    async def close(self, backend_session_id):
        return None

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def test_soveren_agent_platform_app_registers_standard_workers(tmp_path):
    app = (
        AgentPlatformApp(db_path=tmp_path / "app.db")
        .use_batching()
        .use_agent(handler=NoopAgentHandler(), idle_initial_s=0.01)
        .use_actions(registry=ActionRegistry())
        .use_outbound(registry=OutboundRegistry(), channels=["telegram", "email"])
        .use_cron(handler=NoopCronHandler(), poll_interval_s=0.01)
        .use_session_mailbox(tenant_id="tenant-a", session_backends=SessionBackendRegistry())
        .use_session_indexer(tenant_id="tenant-a", session_inspectors=SessionInspectorRegistry())
    )

    assert app.worker_names == (
        "batching",
        "agent",
        "actions",
        "outbound:telegram",
        "outbound:email",
        "cron",
        "session_mailbox",
        "session_indexer",
    )


def test_soveren_agent_platform_app_bootstraps_storage_before_start(tmp_path):
    db_path = tmp_path / "app.db"

    async def run() -> None:
        app = AgentPlatformApp(db_path=db_path)
        await app.start()
        await app.stop()

    asyncio.run(run())

    conn = open_sqlite(db_path)
    try:
        assert_platform_schema(conn)
    finally:
        conn.close()


def test_soveren_agent_platform_app_shuts_down_registered_session_resources(tmp_path):
    backend = ManagedSessionBackend()
    registry = SessionBackendRegistry({backend.name: backend})

    async def run() -> None:
        app = AgentPlatformApp(db_path=tmp_path / "app.db").use_session_mailbox(
            tenant_id="tenant-a",
            session_backends=registry,
        )
        await app.start()
        await app.stop()

    asyncio.run(run())

    assert backend.shutdown_calls == 1
