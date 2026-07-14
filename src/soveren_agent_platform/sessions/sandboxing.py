"""Business-facing composition for sandboxed Codex execution."""

from __future__ import annotations

import hashlib
from typing import Any

from soveren_agent_platform import __version__
from soveren_agent_platform.sandbox import (
    DockerCredentialBrokerSpec,
    DockerEgressSpec,
    DockerSandboxManager,
    SandboxManager,
    SandboxSpec,
    resolve_sandbox_resource_profile,
)
from soveren_agent_platform.sessions.backends.codex_tools import DynamicToolRegistry
from soveren_agent_platform.sessions.backends.sandboxed_codex import SandboxedCodexAppServerBackend
from soveren_agent_platform.sessions.codex_credentials import CodexCredentialProvider
from soveren_agent_platform.sessions.registry import SessionBackendRegistry

DEFAULT_SANDBOX_IMAGE = f"ghcr.io/neureca/soveren-codex-sandbox:{__version__}"
DEFAULT_EGRESS_IMAGE = f"ghcr.io/neureca/soveren-sandbox-egress:{__version__}"
DEFAULT_CREDENTIAL_BROKER_IMAGE = f"ghcr.io/neureca/soveren-credential-broker:{__version__}"
DEFAULT_SANDBOX_NETWORK = "soveren-sandbox-egress"
DEFAULT_EGRESS_PROXY = "http://soveren-sandbox-egress:3128"


def create_sandbox_manager(*, max_active_sandboxes: int = 1) -> DockerSandboxManager:
    """Create the process-owned manager shared by every conversation sandbox."""
    return DockerSandboxManager(
        max_active_sandboxes=max_active_sandboxes,
        egress=DockerEgressSpec(image=DEFAULT_EGRESS_IMAGE),
        credential_broker=DockerCredentialBrokerSpec(image=DEFAULT_CREDENTIAL_BROKER_IMAGE),
        recover_orphaned_sandboxes=True,
    )


def create_sandboxed_codex_backend(
    *,
    tenant_id: str,
    source_id: str,
    credentials: CodexCredentialProvider,
    sandbox_manager: SandboxManager,
    session_backends: SessionBackendRegistry,
    resources: str = "small",
    model: str | None = None,
    developer_instructions: str | None = None,
    dynamic_tools: DynamicToolRegistry | None = None,
    output_schema: dict[str, Any] | None = None,
    collaboration_mode: str | None = None,
    idle_stop_after_s: float | None = 300.0,
) -> SandboxedCodexAppServerBackend:
    """Create the supported Docker-backed Codex backend for one private conversation."""
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    profile = resolve_sandbox_resource_profile(resources)
    env = {
        "HTTP_PROXY": DEFAULT_EGRESS_PROXY,
        "HTTPS_PROXY": DEFAULT_EGRESS_PROXY,
        "http_proxy": DEFAULT_EGRESS_PROXY,
        "https_proxy": DEFAULT_EGRESS_PROXY,
        "NO_PROXY": "",
        "no_proxy": "",
    }
    backend = SandboxedCodexAppServerBackend(
        sandbox_manager=sandbox_manager,
        name=_conversation_backend_name(tenant_id, source_id),
        sandbox_spec=SandboxSpec(
            tenant_id=tenant_id,
            conversation_id=source_id,
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
    session_backends.register(backend.name, backend)
    return backend


def _conversation_backend_name(tenant_id: str, source_id: str) -> str:
    digest = hashlib.sha256(f"{tenant_id}\0{source_id}".encode("utf-8")).hexdigest()[:24]
    return f"codex:{digest}"
