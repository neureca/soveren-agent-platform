import asyncio

import pytest

from agent_platform.actions.registry import ActionRegistry
from agent_platform.agent.contracts import AgentEvent
from agent_platform.app_api import AgentPlatformApp, WorkerSpec, WorkerSupervisor
from agent_platform.cron.contracts import CronJob
from agent_platform.outbound.registry import OutboundRegistry


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


def test_agent_platform_app_registers_standard_workers(tmp_path):
    app = (
        AgentPlatformApp(db_path=tmp_path / "app.db")
        .use_batching()
        .use_agent(handler=NoopAgentHandler(), idle_initial_s=0.01)
        .use_actions(registry=ActionRegistry())
        .use_outbound(registry=OutboundRegistry(), channels=["telegram", "email"])
        .use_cron(handler=NoopCronHandler(), poll_interval_s=0.01)
        .use_session_mailbox(tenant_id="tenant-a", session_backends={})
    )

    assert app.worker_names == (
        "batching",
        "agent",
        "actions",
        "outbound:telegram",
        "outbound:email",
        "cron",
        "session_mailbox",
    )
