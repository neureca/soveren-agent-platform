"""Codex app-server backend running inside a sandbox runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import PurePosixPath
from typing import Any

from soveren_agent_platform.sandbox import SandboxHandle, SandboxRuntime, SandboxSpec
from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec
from soveren_agent_platform.sessions.backends.codex_app_server import (
    CodexAppServerBackend,
    CodexJsonRpcClient,
)
from soveren_agent_platform.sessions.backends.codex_tools import DynamicToolRegistry, DynamicToolSpec


class SandboxedCodexAppServerBackend:
    """SessionBackend that keeps one Codex app-server inside one tenant sandbox."""

    name = "sandboxed_codex_app_server"

    def __init__(
        self,
        *,
        sandbox_runtime: SandboxRuntime,
        sandbox_spec: SandboxSpec,
        codex_command: list[str] | None = None,
        sandbox_cwd: str | None = None,
        model: str | None = None,
        sandbox: str = "workspace-write",
        approval_policy: str = "never",
        developer_instructions: str | None = None,
        dynamic_tools: DynamicToolRegistry | list[DynamicToolSpec | dict[str, Any]] | None = None,
        output_schema: dict[str, Any] | None = None,
        collaboration_mode: str | None = None,
        request_timeout_s: float = 15.0,
        turn_timeout_s: float = 180.0,
        destroy_sandbox_on_shutdown: bool = False,
        client: CodexJsonRpcClient | None = None,
    ) -> None:
        self.sandbox_runtime = sandbox_runtime
        self.sandbox_spec = sandbox_spec
        self.codex_command = codex_command or ["codex", "app-server", "--listen", "stdio://"]
        self.sandbox_cwd = sandbox_cwd or sandbox_spec.workspace_root
        self.model = model
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.developer_instructions = developer_instructions
        self.dynamic_tools = dynamic_tools
        self.output_schema = output_schema
        self.collaboration_mode = collaboration_mode
        self.request_timeout_s = request_timeout_s
        self.turn_timeout_s = turn_timeout_s
        self.destroy_sandbox_on_shutdown = destroy_sandbox_on_shutdown
        self.client = client
        self._handle: SandboxHandle | None = None
        self._backend: CodexAppServerBackend | None = None

    async def open(self, spec: OpenSpec) -> OpenResult:
        if spec.kind not in ("codex", "codex_cli"):
            raise ValueError(f"sandboxed Codex backend cannot open kind={spec.kind!r}")
        backend = await self._ensure_backend()
        cwd = _sandbox_cwd(self.sandbox_cwd, spec.metadata)
        await self.sandbox_runtime.ensure_directory(self._require_handle(), cwd)
        opened = await backend.open(replace(spec, cwd=cwd))
        metadata = {
            **(opened.metadata or {}),
            "runtime": self.name,
            "sandbox_runtime": self._require_handle().metadata.get("runtime", "unknown"),
            "sandbox_name": self._require_handle().name,
            "sandbox_tenant_key": self._require_handle().metadata.get("tenant_key", ""),
            "sandbox_cwd": cwd,
        }
        return OpenResult(
            backend_session_id=opened.backend_session_id,
            session_handle=opened.session_handle,
            metadata=metadata,
        )

    async def send(self, backend_session_id: str, prompt: str) -> None:
        await self._require_backend().send(backend_session_id, prompt)

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return await self._require_backend().capture(backend_session_id)

    async def close(self, backend_session_id: str) -> None:
        await self._require_backend().close(backend_session_id)

    async def shutdown(self) -> None:
        if self._backend is not None:
            await self._backend.shutdown()
        self._backend = None
        if self._handle is not None and self.destroy_sandbox_on_shutdown:
            await self.sandbox_runtime.destroy(self._handle)
            self._handle = None

    async def destroy_sandbox(self) -> None:
        handle = self._handle
        destroy_on_shutdown = self.destroy_sandbox_on_shutdown
        self.destroy_sandbox_on_shutdown = False
        await self.shutdown()
        self.destroy_sandbox_on_shutdown = destroy_on_shutdown
        if handle is not None:
            await self.sandbox_runtime.destroy(handle)
            self._handle = None

    async def _ensure_backend(self) -> CodexAppServerBackend:
        if self._backend is not None:
            return self._backend
        handle = await self.sandbox_runtime.acquire(self.sandbox_spec)
        self._handle = handle
        await self.sandbox_runtime.ensure_directory(handle, handle.workspace_root)
        await self.sandbox_runtime.ensure_directory(handle, handle.codex_home)
        command = self.sandbox_runtime.exec_command(
            handle,
            self.codex_command,
            env={"CODEX_HOME": handle.codex_home},
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

    def _require_backend(self) -> CodexAppServerBackend:
        if self._backend is None:
            raise RuntimeError("sandboxed Codex backend has not been opened")
        return self._backend

    def _require_handle(self) -> SandboxHandle:
        if self._handle is None:
            raise RuntimeError("sandboxed Codex backend has no sandbox handle")
        return self._handle


def _sandbox_cwd(default_cwd: str, metadata: Mapping[str, Any] | None) -> str:
    value = (metadata or {}).get("sandbox_cwd")
    if isinstance(value, str) and value:
        path = value
    else:
        path = default_cwd
    if not path.startswith("/"):
        raise ValueError("sandbox_cwd must be an absolute container path")
    return str(PurePosixPath(path))
