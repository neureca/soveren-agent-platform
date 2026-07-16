"""Codex app-server backend running inside a managed sandbox."""

from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from soveren_agent_platform.sandbox import SandboxHandle, SandboxManager, SandboxSpec
from soveren_agent_platform.sessions.backend import (
    CaptureResult,
    OpenResult,
    OpenSpec,
    SendReceipt,
    ensure_conversation_scope,
)
from soveren_agent_platform.sessions.backends.codex_app_server import (
    CodexAppServerBackend,
    CodexCollaborationMode,
    CodexJsonRpcClient,
)
from soveren_agent_platform.sessions.backends.codex_tools import DynamicToolRegistry, DynamicToolSpec
from soveren_agent_platform.sessions.codex_credentials import (
    CodexCredentialProvider,
    CodexCredentialProvisioning,
)

logger = logging.getLogger(__name__)


class SandboxedCodexAppServerBackend:
    """SessionBackend that keeps one Codex app-server inside one conversation sandbox."""

    @property
    def tenant_id(self) -> str:
        return self.sandbox_spec.tenant_id

    @property
    def source_id(self) -> str:
        return self.sandbox_spec.conversation_id

    def __init__(
        self,
        *,
        sandbox_manager: SandboxManager,
        sandbox_spec: SandboxSpec,
        name: str = "codex",
        codex_command: list[str] | None = None,
        sandbox_cwd: str | None = None,
        model: str | None = None,
        sandbox: str = "workspace-write",
        approval_policy: str = "never",
        developer_instructions: str | None = None,
        dynamic_tools: DynamicToolRegistry | list[DynamicToolSpec | dict[str, Any]] | None = None,
        credentials: CodexCredentialProvider | None = None,
        output_schema: dict[str, Any] | None = None,
        collaboration_mode: CodexCollaborationMode | None = None,
        request_timeout_s: float = 15.0,
        turn_timeout_s: float = 180.0,
        idle_stop_after_s: float | None = 300.0,
        stop_sandbox_on_shutdown: bool = True,
        destroy_sandbox_on_shutdown: bool = False,
        client: CodexJsonRpcClient | None = None,
    ) -> None:
        if not name:
            raise ValueError("sandboxed backend name must be non-empty")
        self.name = name
        self.sandbox_manager = sandbox_manager
        self.sandbox_spec = sandbox_spec
        self.codex_command = codex_command or ["codex", "app-server", "--listen", "stdio://"]
        self.sandbox_cwd = sandbox_cwd or sandbox_spec.workspace_root
        self.model = model
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.developer_instructions = developer_instructions
        if dynamic_tools is not None and not isinstance(dynamic_tools, DynamicToolRegistry) and client is None:
            raise ValueError("dynamic tool specs without handlers require an explicit custom Codex client")
        if isinstance(dynamic_tools, DynamicToolRegistry):
            dynamic_tools.bind_conversation(
                tenant_id=sandbox_spec.tenant_id,
                source_id=sandbox_spec.conversation_id,
            )
        self.dynamic_tools = dynamic_tools
        self.credentials = credentials
        self.output_schema = output_schema
        if collaboration_mode is not None and not isinstance(collaboration_mode, CodexCollaborationMode):
            raise TypeError("collaboration_mode must be a CodexCollaborationMode")
        self.collaboration_mode = collaboration_mode
        self.request_timeout_s = request_timeout_s
        self.turn_timeout_s = turn_timeout_s
        if idle_stop_after_s is not None and idle_stop_after_s < 0:
            raise ValueError("idle_stop_after_s must be non-negative")
        self.idle_stop_after_s = idle_stop_after_s
        self.stop_sandbox_on_shutdown = stop_sandbox_on_shutdown
        self.destroy_sandbox_on_shutdown = destroy_sandbox_on_shutdown
        self.client = client
        self._handle: SandboxHandle | None = None
        self._backend: CodexAppServerBackend | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._active_thread_ids: set[str] = set()
        self._inflight_operations = 0
        self._idle_stop_task: asyncio.Task[None] | None = None

    async def open(self, spec: OpenSpec) -> OpenResult:
        ensure_conversation_scope(
            self,
            spec.conversation_scope,
            resource_name=f"sandboxed Codex backend {self.name!r}",
        )
        async with self._track_operation():
            if spec.kind not in ("codex", "codex_cli"):
                raise ValueError(f"sandboxed Codex backend cannot open kind={spec.kind!r}")
            backend = await self._activate_backend()
            handle = self._require_handle()
            cwd = _sandbox_cwd(handle.workspace_root, self.sandbox_cwd, spec.metadata)
            await self.sandbox_manager.ensure_directory(handle, cwd)
            opened = await backend.open(replace(spec, cwd=cwd))
            self._active_thread_ids.add(opened.backend_session_id)
            metadata = {
                **(opened.metadata or {}),
                "runtime": self.name,
                "isolation": handle.metadata.get("runtime", "unknown"),
                "sandbox_cwd": cwd,
            }
            return OpenResult(
                backend_session_id=opened.backend_session_id,
                session_handle=opened.session_handle,
                metadata=metadata,
            )

    async def send(self, backend_session_id: str, prompt: str) -> SendReceipt | None:
        async with self._track_operation():
            backend = await self._activate_backend()
            receipt = await backend.send(backend_session_id, prompt)
            self._active_thread_ids.add(backend_session_id)
            return receipt

    async def capture(self, backend_session_id: str) -> CaptureResult:
        async with self._track_operation():
            backend = await self._activate_backend()
            return await backend.capture(backend_session_id)

    async def capture_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> CaptureResult:
        async with self._track_operation():
            backend = await self._activate_backend()
            return await backend.capture_delivery(backend_session_id, receipt)

    async def abort_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> None:
        async with self._track_operation():
            try:
                backend = await self._activate_backend()
                await backend.abort_delivery(backend_session_id, receipt)
            finally:
                self._active_thread_ids.discard(backend_session_id)

    async def capture_thread_history(self, backend_session_id: str) -> CaptureResult:
        async with self._track_operation():
            backend = await self._activate_backend()
            return await backend.capture_thread_history(backend_session_id)

    async def close(self, backend_session_id: str) -> None:
        async with self._track_operation():
            backend = await self._activate_backend()
            await backend.close(backend_session_id)
            self._active_thread_ids.discard(backend_session_id)

    async def shutdown(self) -> None:
        idle_task = self._cancel_idle_stop()
        if idle_task is not None and idle_task is not asyncio.current_task():
            await asyncio.gather(idle_task, return_exceptions=True)
        async with self._lifecycle_lock:
            errors: list[BaseException] = []
            try:
                await self._shutdown_backend_locked()
            except BaseException as exc:
                errors.append(exc)
            try:
                if self._handle is not None and self.destroy_sandbox_on_shutdown:
                    await self.sandbox_manager.destroy(self._handle)
                    self._handle = None
                elif self._handle is not None and self.stop_sandbox_on_shutdown:
                    await self.sandbox_manager.stop(self._handle)
            except BaseException as exc:
                errors.append(exc)
            self._active_thread_ids.clear()
            if errors:
                raise BaseExceptionGroup("sandboxed Codex shutdown failed", errors)

    async def destroy_sandbox(self) -> None:
        idle_task = self._cancel_idle_stop()
        if idle_task is not None and idle_task is not asyncio.current_task():
            await asyncio.gather(idle_task, return_exceptions=True)
        async with self._lifecycle_lock:
            errors: list[BaseException] = []
            try:
                await self._shutdown_backend_locked()
            except BaseException as exc:
                errors.append(exc)
            try:
                if self._handle is not None:
                    await self.sandbox_manager.destroy(self._handle)
                    self._handle = None
            except BaseException as exc:
                errors.append(exc)
            self._active_thread_ids.clear()
            if errors:
                raise BaseExceptionGroup("sandboxed Codex destroy failed", errors)

    async def _ensure_backend(self) -> CodexAppServerBackend:
        if self._backend is not None:
            return self._backend
        async with self._lifecycle_lock:
            if self._backend is not None:
                return self._backend
            handle = await self.sandbox_manager.acquire(self.sandbox_spec)
            self._handle = handle
            provisioning = CodexCredentialProvisioning()
            try:
                await self.sandbox_manager.ensure_directory(handle, handle.workspace_root)
                await self.sandbox_manager.ensure_directory(handle, handle.codex_home)
                if self.credentials is not None:
                    provisioning = await self.credentials.provision(self.sandbox_manager, handle)
                    handle = replace(
                        handle,
                        metadata={**handle.metadata, **dict(provisioning.sandbox_metadata)},
                    )
                    self._handle = handle
            except BaseException as setup_error:
                try:
                    await self.sandbox_manager.stop(handle)
                    self._handle = None
                except BaseException as cleanup_error:
                    raise BaseExceptionGroup(
                        "sandboxed Codex setup and cleanup failed",
                        [setup_error, cleanup_error],
                    ) from setup_error
                raise
            codex_command = list(self.codex_command)
            for override in provisioning.config_overrides:
                codex_command.extend(["-c", override])
            launch_env = dict(provisioning.launch_env)
            launch_env["CODEX_HOME"] = handle.codex_home
            command = self.sandbox_manager.exec_command(
                handle,
                codex_command,
                env=launch_env,
                workdir=handle.workspace_root,
            )
            self._backend = CodexAppServerBackend(
                command=command,
                model=self.model,
                sandbox=self.sandbox,
                approval_policy=self.approval_policy,
                developer_instructions=self.developer_instructions,
                dynamic_tools=self.dynamic_tools,
                output_schema=self.output_schema,
                collaboration_mode=self.collaboration_mode,
                create_cwd=False,
                request_timeout_s=self.request_timeout_s,
                turn_timeout_s=self.turn_timeout_s,
                client=self.client,
            )
            return self._backend

    async def _activate_backend(self) -> CodexAppServerBackend:
        idle_task = self._cancel_idle_stop()
        if idle_task is not None and idle_task is not asyncio.current_task():
            await asyncio.gather(idle_task, return_exceptions=True)
        return await self._ensure_backend()

    async def _shutdown_backend_locked(self) -> None:
        backend = self._backend
        if backend is not None:
            try:
                await backend.shutdown()
            finally:
                self._backend = None

    def _cancel_idle_stop(self) -> asyncio.Task[None] | None:
        task = self._idle_stop_task
        self._idle_stop_task = None
        if task is not None and not task.done():
            task.cancel()
        return task

    @asynccontextmanager
    async def _track_operation(self) -> AsyncIterator[None]:
        self._inflight_operations += 1
        try:
            yield
        finally:
            self._inflight_operations -= 1
            self._schedule_idle_stop()

    def _schedule_idle_stop(self) -> None:
        if (
            self.idle_stop_after_s is None
            or self._active_thread_ids
            or self._inflight_operations
            or self._backend is None
        ):
            return
        self._cancel_idle_stop()
        self._idle_stop_task = asyncio.create_task(
            self._stop_after_idle(),
            name=(
                "soveren-sandbox-idle-stop:"
                f"{self.sandbox_spec.tenant_id}:{self.sandbox_spec.conversation_id}"
            ),
        )

    async def _stop_after_idle(self) -> None:
        try:
            await asyncio.sleep(self.idle_stop_after_s or 0)
            async with self._lifecycle_lock:
                if self._active_thread_ids or self._inflight_operations or self._backend is None:
                    return
                cleanup_task = asyncio.create_task(self._stop_idle_backend_locked())
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError as cancellation:
                    while not cleanup_task.done():
                        try:
                            await asyncio.shield(cleanup_task)
                        except asyncio.CancelledError:
                            continue
                    cleanup_task.result()
                    raise cancellation
        finally:
            if self._idle_stop_task is asyncio.current_task():
                self._idle_stop_task = None

    async def _stop_idle_backend_locked(self) -> None:
        errors: list[BaseException] = []
        try:
            await self._shutdown_backend_locked()
        except BaseException as exc:
            errors.append(exc)
        if self._handle is not None:
            try:
                await self.sandbox_manager.stop(self._handle)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            logger.error(
                "sandboxed Codex idle stop failed",
                exc_info=BaseExceptionGroup("sandboxed Codex idle stop failed", errors),
            )

    def _require_handle(self) -> SandboxHandle:
        if self._handle is None:
            raise RuntimeError("sandboxed Codex backend has no sandbox handle")
        return self._handle


def _sandbox_cwd(workspace_root: str, default_cwd: str, metadata: Mapping[str, Any] | None) -> str:
    root = _normalize_container_path(workspace_root, allow_root=False)
    value = (metadata or {}).get("sandbox_cwd")
    if isinstance(value, str) and value:
        path = value
    else:
        path = default_cwd
    normalized = _normalize_container_path(path, allow_root=True)
    if normalized != root and not normalized.startswith(f"{root}/"):
        raise ValueError("sandbox_cwd must stay inside the sandbox workspace root")
    return normalized


def _normalize_container_path(path: str, *, allow_root: bool) -> str:
    if not path.startswith("/"):
        raise ValueError("sandbox_cwd must be an absolute container path")
    normalized = posixpath.normpath(path)
    if normalized == "." or (normalized == "/" and not allow_root):
        raise ValueError("sandbox_cwd must be an absolute container path")
    return normalized
