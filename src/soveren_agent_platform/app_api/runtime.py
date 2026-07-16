"""Runtime container and worker supervisor for platform apps."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.worker import run_actions_worker
from soveren_agent_platform.agent.contracts import AgentHandler
from soveren_agent_platform.agent.worker import run_agent_worker
from soveren_agent_platform.batching.worker import run_batching_worker
from soveren_agent_platform.cron.contracts import CronHandler
from soveren_agent_platform.cron.worker import run_cron_worker
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.worker import run_outbound_worker
from soveren_agent_platform.sessions.indexer_worker import run_session_indexer_worker
from soveren_agent_platform.sessions.inspector_registry import SessionInspectorMapping
from soveren_agent_platform.sessions.mailbox_worker import run_session_mailbox_worker
from soveren_agent_platform.sessions.registry import (
    SessionBackendMapping,
    SessionBackendRegistry,
    normalize_session_backends,
)
from soveren_agent_platform.storage.bootstrap import bootstrap_platform_storage

WorkerFactory = Callable[[asyncio.Event], Coroutine[Any, Any, None]]


@runtime_checkable
class RuntimeResource(Protocol):
    async def shutdown(self) -> None: ...


@dataclass(slots=True)
class WorkerSpec:
    name: str
    factory: WorkerFactory


class WorkerSupervisor:
    """Start, stop, and monitor a set of cooperative async workers."""

    def __init__(self, specs: Iterable[WorkerSpec] | None = None) -> None:
        self._specs: list[WorkerSpec] = []
        self._stop_event: asyncio.Event | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()
        for spec in specs or ():
            self.add(spec)

    @property
    def worker_names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self._specs)

    @property
    def stop_event(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        return self._stop_event

    def add(self, spec: WorkerSpec) -> None:
        if self._closed:
            raise RuntimeError("cannot add workers after supervisor has stopped")
        if self._tasks:
            raise RuntimeError("cannot add workers after supervisor has started")
        if not isinstance(spec.name, str) or not spec.name.strip():
            raise ValueError("worker name must be a non-empty string")
        if spec.name in {existing.name for existing in self._specs}:
            raise ValueError(f"worker already registered: {spec.name!r}")
        self._specs.append(spec)

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("worker supervisor cannot be restarted after stop")
            if self._tasks:
                return
            stop_event = self.stop_event
            for spec in self._specs:
                self._tasks[spec.name] = asyncio.create_task(
                    spec.factory(stop_event),
                    name=f"soveren-agent-platform:{spec.name}",
                )

    async def wait(self) -> None:
        """Wait until a worker exits or the supervisor is stopped.

        If any worker exits with an exception, all workers are stopped and that
        exception is re-raised.
        """
        await self.start()
        tasks = tuple(self._tasks.values())
        if not tasks:
            return
        done, _ = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                await self.stop()
                raise exc
        if not self.stop_event.is_set():
            await self.stop()

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        async with self._lifecycle_lock:
            self._closed = True
            if self._stop_event is not None:
                self._stop_event.set()
            if not self._tasks:
                return
            tasks = list(self._tasks.values())
            errors: list[BaseException] = []
            try:
                done, pending = await asyncio.wait(tasks, timeout=timeout_s)
                for task in pending:
                    task.cancel()
                if pending:
                    results = await asyncio.gather(*pending, return_exceptions=True)
                    errors.extend(
                        result
                        for result in results
                        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
                    )
                for task in done:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is not None:
                        errors.append(exc)
            except BaseException:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            finally:
                self._tasks.clear()
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("workers failed during shutdown", errors)


class AgentPlatformApp:
    """Composition helper for the standard platform worker set."""

    def __init__(self, *, db_path: Path, bootstrap_storage: bool = True) -> None:
        self.db_path = db_path
        self.bootstrap_storage = bootstrap_storage
        self.supervisor = WorkerSupervisor()
        self._storage_bootstrapped = False
        self._resources: list[RuntimeResource] = []
        self._session_backend_registries: list[SessionBackendRegistry] = []
        self._shutdown_resources: list[RuntimeResource] = []
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def worker_names(self) -> tuple[str, ...]:
        return self.supervisor.worker_names

    def add_worker(self, name: str, factory: WorkerFactory) -> "AgentPlatformApp":
        self.supervisor.add(WorkerSpec(name=name, factory=factory))
        return self

    def manage_resource(self, resource: RuntimeResource) -> "AgentPlatformApp":
        if self._closed:
            raise RuntimeError("cannot manage resources after AgentPlatformApp has stopped")
        if not any(existing is resource for existing in self._resources):
            self._resources.append(resource)
        return self

    def _manage_session_backend_registry(self, registry: SessionBackendRegistry) -> None:
        if not any(existing is registry for existing in self._session_backend_registries):
            self._session_backend_registries.append(registry)

    def _pending_runtime_resources(self) -> list[RuntimeResource]:
        candidates = list(self._resources)
        for registry in self._session_backend_registries:
            candidates.extend(
                backend for backend in registry.as_dict().values() if isinstance(backend, RuntimeResource)
            )

        pending: list[RuntimeResource] = []
        for resource in candidates:
            if any(shutdown is resource for shutdown in self._shutdown_resources):
                continue
            if not any(existing is resource for existing in pending):
                pending.append(resource)
        return pending

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
        tenant_id: str | None = None,
    ) -> "AgentPlatformApp":
        for channel in channels:
            self.add_worker(
                f"outbound:{channel}",
                self._outbound_worker_factory(
                    registry=registry,
                    channel=channel,
                    tenant_id=tenant_id,
                ),
            )
        return self

    def _outbound_worker_factory(
        self,
        *,
        registry: OutboundRegistry,
        channel: str,
        tenant_id: str | None,
    ) -> WorkerFactory:
        async def worker(stop_event: asyncio.Event) -> None:
            await run_outbound_worker(
                self.db_path,
                stop_event,
                registry=registry,
                channel=channel,
                tenant_id=tenant_id,
            )

        return worker

    def use_cron(
        self,
        *,
        handler: CronHandler,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        return self.add_worker(
            "cron" if tenant_id is None else f"cron:{tenant_id}",
            lambda stop_event: run_cron_worker(
                self.db_path,
                stop_event,
                handler=handler,
                tenant_id=tenant_id,
                **kwargs,
            ),
        )

    def use_session_mailbox(
        self,
        *,
        tenant_id: str,
        session_backends: SessionBackendMapping,
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        if isinstance(session_backends, SessionBackendRegistry):
            self._manage_session_backend_registry(session_backends)
        else:
            for backend in normalize_session_backends(session_backends).values():
                if isinstance(backend, RuntimeResource):
                    self.manage_resource(backend)
        return self.add_worker(
            f"session_mailbox:{tenant_id}",
            lambda stop_event: run_session_mailbox_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                session_backends=session_backends,
                **kwargs,
            ),
        )

    def use_session_indexer(
        self,
        *,
        tenant_id: str,
        session_inspectors: SessionInspectorMapping,
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        return self.add_worker(
            f"session_indexer:{tenant_id}",
            lambda stop_event: run_session_indexer_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                session_inspectors=session_inspectors,
                **kwargs,
            ),
        )

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("AgentPlatformApp cannot be restarted after stop")
            if self.bootstrap_storage and not self._storage_bootstrapped:
                await bootstrap_platform_storage(self.db_path)
                self._storage_bootstrapped = True
            await self.supervisor.start()

    async def wait(self) -> None:
        await self.supervisor.wait()

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        async with self._lifecycle_lock:
            self._closed = True
            errors: list[BaseException] = []
            try:
                await self.supervisor.stop(timeout_s=timeout_s)
            except BaseException as exc:
                errors.append(exc)
            for resource in reversed(self._pending_runtime_resources()):
                try:
                    await resource.shutdown()
                except BaseException as exc:
                    errors.append(exc)
                else:
                    self._shutdown_resources.append(resource)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("platform shutdown failed", errors)

    async def __aenter__(self) -> "AgentPlatformApp":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()
