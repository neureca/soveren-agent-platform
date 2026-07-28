"""Runtime container and worker supervisor for platform apps."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.worker import run_actions_worker
from soveren_agent_platform.agent.contracts import AgentHandler
from soveren_agent_platform.agent.worker import run_agent_worker
from soveren_agent_platform.batching.worker import run_batching_worker
from soveren_agent_platform.cron.worker import run_cron_worker
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.worker import run_outbound_worker
from soveren_agent_platform.runtime.worker_loop import DEFAULT_MAX_CONSECUTIVE_FAILURES
from soveren_agent_platform.sandbox.contracts import DEFAULT_MAX_ACTIVE_SANDBOXES
from soveren_agent_platform.sessions.indexer_worker import run_session_indexer_worker
from soveren_agent_platform.sessions.inspector_registry import SessionInspectorMapping
from soveren_agent_platform.sessions.mailbox_worker import (
    CAPTURE_PENDING_TIMEOUT_S,
    STALE_SENDING_S,
    run_session_mailbox_worker,
)
from soveren_agent_platform.sessions.registry import (
    SessionBackendMapping,
    SessionBackendRegistry,
    normalize_session_backends,
)
from soveren_agent_platform.storage.bootstrap import bootstrap_platform_storage

if TYPE_CHECKING:
    from soveren_agent_platform.json_types import JsonObject
    from soveren_agent_platform.llm import (
        ConversationToolRegistryFactory,
        SandboxedCodexRuntime,
        TenantCodexCredentialResolver,
    )
    from soveren_agent_platform.llm.backends.sandboxed_codex import (
        _ManagedSandboxedCodexRuntime,
    )
    from soveren_agent_platform.sessions import (
        CodexCollaborationMode,
    )

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
            workers: list[tuple[WorkerSpec, Coroutine[Any, Any, None]]] = []
            scheduled_count = 0
            try:
                # Construct every worker before scheduling any of them so a
                # synchronous factory failure cannot leave a partial runtime.
                for spec in self._specs:
                    workers.append((spec, spec.factory(stop_event)))
                for spec, worker in workers:
                    self._tasks[spec.name] = asyncio.create_task(
                        worker,
                        name=f"soveren-agent-platform:{spec.name}",
                    )
                    scheduled_count += 1
            except BaseException as start_error:
                cleanup_errors = await self._rollback_failed_start(
                    worker for _, worker in workers[scheduled_count:]
                )
                for cleanup_error in cleanup_errors:
                    start_error.add_note(f"worker startup rollback also failed: {cleanup_error!r}")
                raise

    async def _rollback_failed_start(
        self,
        unscheduled_workers: Iterable[Coroutine[Any, Any, None]],
    ) -> list[BaseException]:
        self._closed = True
        self.stop_event.set()
        cleanup_errors: list[BaseException] = []
        for worker in unscheduled_workers:
            try:
                worker.close()
            except BaseException as exc:
                cleanup_errors.append(exc)

        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            self._tasks.clear()
        cleanup_errors.extend(
            result
            for result in results
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
        )
        return cleanup_errors

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

    def __init__(
        self,
        *,
        db_path: Path,
        bootstrap_storage: bool = True,
        agent_recipient: str = "agent",
    ) -> None:
        if not isinstance(agent_recipient, str) or not agent_recipient.strip():
            raise ValueError("agent_recipient must be a non-empty string")
        self.db_path = db_path
        self.bootstrap_storage = bootstrap_storage
        self.agent_recipient = agent_recipient
        self.supervisor = WorkerSupervisor()
        self._storage_bootstrapped = False
        self._resources: list[RuntimeResource] = []
        self._session_backend_registries: list[SessionBackendRegistry] = []
        self._shutdown_resources: list[RuntimeResource] = []
        self._sandboxed_codex_runtime: _ManagedSandboxedCodexRuntime | None = None
        self._codex_mailbox_tenants: set[str] = set()
        self._session_mailbox_prefixes: dict[str, set[str | None]] = {}
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

    def configure_sandboxed_codex(
        self,
        *,
        credentials_for_tenant: TenantCodexCredentialResolver,
        model: str,
        resources: str = "small",
        max_active_sandboxes: int = DEFAULT_MAX_ACTIVE_SANDBOXES,
        developer_instructions: str | None = None,
        tool_registry_factory: ConversationToolRegistryFactory | None = None,
        output_schema: JsonObject | None = None,
        collaboration_mode: CodexCollaborationMode | None = None,
        idle_stop_after_s: float | None = 300.0,
    ) -> SandboxedCodexRuntime:
        """Configure the process-owned Codex runtime and manage its lifecycle."""
        if self._closed:
            raise RuntimeError("cannot configure Codex after AgentPlatformApp has stopped")
        if self._sandboxed_codex_runtime is not None:
            raise RuntimeError("sandboxed Codex runtime is already configured")
        from soveren_agent_platform.llm.backends.sandboxed_codex import (
            _create_sandboxed_codex_runtime,
        )

        runtime = _create_sandboxed_codex_runtime(
            db_path=self.db_path,
            credentials_for_tenant=credentials_for_tenant,
            model=model,
            resources=resources,
            max_active_sandboxes=max_active_sandboxes,
            developer_instructions=developer_instructions,
            tool_registry_factory=tool_registry_factory,
            output_schema=output_schema,
            collaboration_mode=collaboration_mode,
            idle_stop_after_s=idle_stop_after_s,
        )
        self._sandboxed_codex_runtime = runtime
        self.manage_resource(runtime)
        return runtime

    def use_codex_session_mailbox(
        self,
        *,
        tenant_id: str,
        stale_sending_s: int = STALE_SENDING_S,
        capture_pending_timeout_s: int = CAPTURE_PENDING_TIMEOUT_S,
        max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ) -> "AgentPlatformApp":
        """Run the durable mailbox against the configured Codex runtime."""
        if not tenant_id.strip():
            raise ValueError("tenant_id must be non-empty")
        if (
            isinstance(stale_sending_s, bool)
            or not isinstance(stale_sending_s, int)
            or stale_sending_s < 0
        ):
            raise ValueError("stale_sending_s must be a non-negative integer")
        if (
            isinstance(capture_pending_timeout_s, bool)
            or not isinstance(capture_pending_timeout_s, int)
            or capture_pending_timeout_s < 1
        ):
            raise ValueError(
                "capture_pending_timeout_s must be a positive integer"
            )
        if (
            isinstance(max_consecutive_failures, bool)
            or not isinstance(max_consecutive_failures, int)
            or max_consecutive_failures < 1
        ):
            raise ValueError(
                "max_consecutive_failures must be a positive integer"
            )
        runtime = self._sandboxed_codex_runtime
        if runtime is None:
            raise RuntimeError(
                "configure_sandboxed_codex must be called before "
                "use_codex_session_mailbox"
            )
        backend_prefix = runtime._mailbox_backend_prefix()
        self._ensure_session_mailbox_prefix_available(
            tenant_id=tenant_id,
            backend_prefix=backend_prefix,
        )
        session_backends = runtime._mailbox_backends()
        self.add_worker(
            f"session_mailbox:codex:{tenant_id}",
            lambda stop_event: run_session_mailbox_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                session_backends=session_backends,
                stale_sending_s=stale_sending_s,
                capture_pending_timeout_s=capture_pending_timeout_s,
                max_consecutive_failures=max_consecutive_failures,
                backend_prefix=backend_prefix,
            ),
        )
        self._record_session_mailbox_prefix(
            tenant_id=tenant_id,
            backend_prefix=backend_prefix,
        )
        self._codex_mailbox_tenants.add(tenant_id)
        return self

    def _manage_session_backend_registry(self, registry: SessionBackendRegistry) -> None:
        if not any(existing is registry for existing in self._session_backend_registries):
            self._session_backend_registries.append(registry)

    def _ensure_session_mailbox_prefix_available(
        self,
        *,
        tenant_id: str,
        backend_prefix: str | None,
    ) -> None:
        for existing in self._session_mailbox_prefixes.get(tenant_id, ()):
            if (
                existing is None
                or backend_prefix is None
                or existing.startswith(backend_prefix)
                or backend_prefix.startswith(existing)
            ):
                raise ValueError(
                    "session mailbox backend ownership overlaps another worker "
                    f"for tenant {tenant_id!r}"
                )

    def _record_session_mailbox_prefix(
        self,
        *,
        tenant_id: str,
        backend_prefix: str | None,
    ) -> None:
        self._session_mailbox_prefixes.setdefault(tenant_id, set()).add(
            backend_prefix
        )

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
        self._reject_owned_routing_argument(kwargs, "output_recipient")
        return self.add_worker(
            "batching",
            lambda stop_event: run_batching_worker(
                self.db_path,
                stop_event,
                output_recipient=self.agent_recipient,
                **kwargs,
            ),
        )

    def use_agent(self, *, handler: AgentHandler, **kwargs: Any) -> "AgentPlatformApp":
        self._reject_owned_routing_argument(kwargs, "recipient")
        return self.add_worker(
            "agent",
            lambda stop_event: run_agent_worker(
                self.db_path,
                stop_event,
                handler=handler,
                recipient=self.agent_recipient,
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
        max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ) -> "AgentPlatformApp":
        for channel in channels:
            self.add_worker(
                f"outbound:{channel}",
                self._outbound_worker_factory(
                    registry=registry,
                    channel=channel,
                    tenant_id=tenant_id,
                    max_consecutive_failures=max_consecutive_failures,
                ),
            )
        return self

    def _outbound_worker_factory(
        self,
        *,
        registry: OutboundRegistry,
        channel: str,
        tenant_id: str | None,
        max_consecutive_failures: int,
    ) -> WorkerFactory:
        async def worker(stop_event: asyncio.Event) -> None:
            await run_outbound_worker(
                self.db_path,
                stop_event,
                registry=registry,
                channel=channel,
                tenant_id=tenant_id,
                max_consecutive_failures=max_consecutive_failures,
            )

        return worker

    def use_cron(
        self,
        *,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        self._reject_owned_routing_argument(kwargs, "recipient")
        return self.add_worker(
            "cron" if tenant_id is None else f"cron:{tenant_id}",
            lambda stop_event: run_cron_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                recipient=self.agent_recipient,
                **kwargs,
            ),
        )

    @staticmethod
    def _reject_owned_routing_argument(
        kwargs: dict[str, Any],
        argument: str,
    ) -> None:
        if argument in kwargs:
            raise ValueError(
                f"AgentPlatformApp owns {argument}; configure agent_recipient "
                "on the application instead"
            )

    def use_session_mailbox(
        self,
        *,
        tenant_id: str,
        session_backends: SessionBackendMapping,
        backend_prefix: str | None = None,
        **kwargs: Any,
    ) -> "AgentPlatformApp":
        if backend_prefix is not None and (
            not isinstance(backend_prefix, str) or not backend_prefix
        ):
            raise ValueError("backend_prefix must be a non-empty string or None")
        self._ensure_session_mailbox_prefix_available(
            tenant_id=tenant_id,
            backend_prefix=backend_prefix,
        )
        if isinstance(session_backends, SessionBackendRegistry):
            self._manage_session_backend_registry(session_backends)
        else:
            for backend in normalize_session_backends(session_backends).values():
                if isinstance(backend, RuntimeResource):
                    self.manage_resource(backend)
        worker_name = (
            f"session_mailbox:{tenant_id}"
            if backend_prefix is None
            else f"session_mailbox:custom:{tenant_id}:{backend_prefix}"
        )
        self.add_worker(
            worker_name,
            lambda stop_event: run_session_mailbox_worker(
                self.db_path,
                stop_event,
                tenant_id=tenant_id,
                session_backends=session_backends,
                backend_prefix=backend_prefix,
                **kwargs,
            ),
        )
        self._record_session_mailbox_prefix(
            tenant_id=tenant_id,
            backend_prefix=backend_prefix,
        )
        return self

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
            try:
                if self.bootstrap_storage and not self._storage_bootstrapped:
                    await bootstrap_platform_storage(self.db_path)
                    self._storage_bootstrapped = True
                if self._sandboxed_codex_runtime is not None:
                    for tenant_id in sorted(self._codex_mailbox_tenants):
                        await self._sandboxed_codex_runtime._restore_sessions(
                            tenant_id
                        )
                await self.supervisor.start()
            except BaseException as start_error:
                self._closed = True
                rollback_errors = await self._shutdown_locked(timeout_s=5.0)
                if rollback_errors:
                    raise BaseExceptionGroup(
                        "platform startup failed and rollback was incomplete",
                        [start_error, *rollback_errors],
                    ) from start_error
                raise

    async def wait(self) -> None:
        await self.supervisor.wait()

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        async with self._lifecycle_lock:
            self._closed = True
            errors = await self._shutdown_locked(timeout_s=timeout_s)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("platform shutdown failed", errors)

    async def _shutdown_locked(self, *, timeout_s: float) -> list[BaseException]:
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
        return errors

    async def __aenter__(self) -> "AgentPlatformApp":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()
