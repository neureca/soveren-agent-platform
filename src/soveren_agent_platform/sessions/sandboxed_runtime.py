"""Business-facing composition for sandboxed Codex execution."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.sandbox import (
    DockerEgressSpec,
    DockerSandboxRuntime,
    SandboxRuntime,
    SandboxSpec,
    resolve_sandbox_resource_profile,
)
from soveren_agent_platform.sessions.backends.codex_tools import DynamicToolRegistry
from soveren_agent_platform.sessions.backends.sandboxed_codex import SandboxedCodexAppServerBackend
from soveren_agent_platform.sessions.codex_credentials import CodexCredentialProvider
from soveren_agent_platform.sessions.registry import SessionBackendRegistry

DEFAULT_SANDBOX_IMAGE = "ghcr.io/neureca/soveren-codex-sandbox:0.2.8"
DEFAULT_EGRESS_IMAGE = "ghcr.io/neureca/soveren-sandbox-egress:0.2.8"
DEFAULT_SANDBOX_NETWORK = "soveren-sandbox-egress"
DEFAULT_EGRESS_PROXY = "http://soveren-sandbox-egress:3128"


def create_sandbox_pool(*, max_active_sandboxes: int = 1) -> DockerSandboxRuntime:
    """Create one process-local sandbox capacity pool with managed egress."""
    return DockerSandboxRuntime(
        max_active_sandboxes=max_active_sandboxes,
        egress=DockerEgressSpec(image=DEFAULT_EGRESS_IMAGE),
        recover_orphaned_sandboxes=True,
    )


def create_sandboxed_codex_backend(
    *,
    tenant_id: str,
    credentials: CodexCredentialProvider,
    resources: str = "small",
    session_backends: SessionBackendRegistry | None = None,
    sandbox_runtime: SandboxRuntime | None = None,
    model: str | None = None,
    developer_instructions: str | None = None,
    dynamic_tools: DynamicToolRegistry | None = None,
    output_schema: dict[str, Any] | None = None,
    collaboration_mode: str | None = None,
    idle_stop_after_s: float | None = 300.0,
) -> SandboxedCodexAppServerBackend:
    """Create the supported Docker-backed Codex backend for one tenant boundary."""
    profile = resolve_sandbox_resource_profile(resources)
    runtime = sandbox_runtime or create_sandbox_pool()
    env = {
        "HTTP_PROXY": DEFAULT_EGRESS_PROXY,
        "HTTPS_PROXY": DEFAULT_EGRESS_PROXY,
        "http_proxy": DEFAULT_EGRESS_PROXY,
        "https_proxy": DEFAULT_EGRESS_PROXY,
        "NO_PROXY": "",
        "no_proxy": "",
    }
    backend = SandboxedCodexAppServerBackend(
        sandbox_runtime=runtime,
        sandbox_spec=SandboxSpec(
            tenant_id=tenant_id,
            image=DEFAULT_SANDBOX_IMAGE,
            memory=profile.memory,
            cpus=profile.cpus,
            pids_limit=profile.pids_limit,
            disk_limit=profile.disk_limit,
            tmpfs_size=profile.tmpfs_size,
            network=DEFAULT_SANDBOX_NETWORK,
            env=env,
        ),
        credentials=credentials,
        model=model,
        developer_instructions=developer_instructions,
        dynamic_tools=dynamic_tools,
        output_schema=output_schema,
        collaboration_mode=collaboration_mode,
        idle_stop_after_s=idle_stop_after_s,
    )
    if session_backends is not None:
        session_backends.register(backend.name, backend)
    return backend
