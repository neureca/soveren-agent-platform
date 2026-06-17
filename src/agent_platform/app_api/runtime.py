"""Runtime container and worker supervisor for platform apps."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_platform.actions.registry import ActionRegistry
from agent_platform.actions.worker import run_actions_worker
from agent_platform.agent.contracts import AgentHandler
from agent_platform.agent.worker import run_agent_worker
from agent_platform.batching.worker import run_batching_worker
from agent_platform.cron.contracts import CronHandler
from agent_platform.cron.worker import run_cron_worker
from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.worker import run_outbound_worker
from agent_platform.sessions.backend import SessionBackend
from agent_platform.sessions.mailbox_worker import run_session_mailbox_worker

WorkerFactory = Callable[[asyncio.Event], Awaitable[None]]


@dataclass(slots=True)
class WorkerSpec:
    name: str
    factory: WorkerFactory


class WorkerSupervisor:
    """Start, stop, and monitor a set of cooperative async workers."""

    def __init__(self, specs: Iterable[WorkerSpec] | None = None) -> None:
        self._specs: list[WorkerSpec] = list(specs or [])
        self._stop_event: asyncio.Event | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def worker_names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self._specs)

    @property
    def stop_event(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        return self._stop_event

    def add(self, spec: WorkerSpec) -> None:
        if self._tasks:
            raise RuntimeError("cannot add workers after supervisor has started")
        if spec.name in {existing.name for existing in self._specs}:
            raise ValueError(f"worker already registered: {spec.name!r}")
        self._specs.append(spec)

    async def start(self) -> None:
        if self._tasks:
            return
        stop_event = self.stop_event
        for spec in self._specs:
            self._tasks[spec.name] = asyncio.create_task(
                spec.factory(stop_event),
                name=f"agent-platform:{spec.name}",
            )

    async def wait(self) -> None:
        """Wait until a worker exits or the supervisor is stopped.

        If any worker exits with an exception, all workers are stopped and that
        exception is re-raised.
        """
        await self.start()
        if not self._tasks:
            return
        done, _ = await asyncio.wait(
            self._tasks.values(),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            exc = task.exception()
            if exc is not None:
                await self.stop()
                raise exc
        if not self.stop_event.is_set():
            await self.stop()

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if not self._tasks:
            return
        tasks = list(self._tasks.values())
        done, pending = await asyncio.wait(tasks, timeout=timeout_s)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc
        self._tasks.clear()


class AgentPlatformApp:
    """Composition helper for the standard platform worker set."""

    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path
        self.supervisor = WorkerSupervisor()

    @property
    def worker_names(self) -> tuple[str, ...]:
        return self.supervisor.worker_names

    def add_worker(self, name: str, factory: WorkerFactory) -> "AgentPlatformApp":
        self.supervisor.add(WorkerSpec(name=name, factory=factory))
        return self

    def use_batching(self, **kwargs: Any) -> "AgentPlatformApp":
        return self.add_worker(
            "batching",
            lambda stop_event: run_batching_worker(self.db_path, stop_event, **kwargs),
        )

    def use_agent(self, *, handler: AgentHandler, **kwargs: Any) -> "AgentPlatformApp":
        return self.add_worker(
            "agent",
            lambda stop_event: run_agent_worker(
                self.db_path,
                stop_event,
                handler=handler,
                **kwargs,
            ),
        )

    def use_actions(self, *, registry: ActionRegistry, **kwargs: Any) -> "AgentPlatformApp":
        return self.add_worker(
            "actions",
            lambda stop_event: run_actions_worker(
                self.db_path,
                stop_event,
                registry=registry,
                **kwargs,
            ),
        )

    def use_outbound(
        self,
        *,
        registry: OutboundRegistry,
        channels: Iterable[str],
    ) -> "AgentPlatformApp":
        for channel in channels:
            self.add_worker(
                f"outbound:{channel}",
                lambda stop_event, channel=channel: run_outbound_worker(
                    self.db_path,
                    stop_event,
                    registry=registry,
                    channel=channel,
                ),
            )
        return self

    def use_cron(self, *, handler: CronHandler, **kwargs: Any) -> "AgentPlatformApp":
        return self.add_worker(
            "cron",
            lambda stop_event: run_cron_worker(
                self.db_path,
                stop_event,
                handler=handler,
                **kwargs,
            ),
        )

    def use_session_mailbox(
        self,
        *,
        tenant_id: str,
        session_backends: dict[str, SessionBackend],
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        return self.add_worker(
            "session_mailbox",
            lambda stop_event: run_session_mailbox_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                session_backends=session_backends,
                **kwargs,
            ),
        )

    async def start(self) -> None:
        await self.supervisor.start()

    async def wait(self) -> None:
        await self.supervisor.wait()

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        await self.supervisor.stop(timeout_s=timeout_s)

    async def __aenter__(self) -> "AgentPlatformApp":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

