"""Sandbox lifecycle contracts.

Sandboxes are execution-plane boundaries. They are not product tenants,
authorization policy, or app-owned business state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class SandboxResourceProfile:
    memory: str
    cpus: str
    pids_limit: int
    disk_limit: str
    tmpfs_size: str


SANDBOX_RESOURCE_PROFILES: Mapping[str, SandboxResourceProfile] = {
    "small": SandboxResourceProfile(
        memory="512m",
        cpus="0.5",
        pids_limit=128,
        disk_limit="1g",
        tmpfs_size="64m",
    ),
    "medium": SandboxResourceProfile(
        memory="1g",
        cpus="1.0",
        pids_limit=256,
        disk_limit="2g",
        tmpfs_size="128m",
    ),
}


def resolve_sandbox_resource_profile(name: str) -> SandboxResourceProfile:
    try:
        return SANDBOX_RESOURCE_PROFILES[name]
    except KeyError as exc:
        available = ", ".join(sorted(SANDBOX_RESOURCE_PROFILES))
        raise ValueError(f"unknown sandbox resource profile {name!r}; expected one of: {available}") from exc


@dataclass(frozen=True, slots=True)
class SandboxSpec:
    tenant_id: str
    conversation_id: str
    image: str
    memory: str = "512m"
    cpus: str = "0.5"
    pids_limit: int = 128
    disk_limit: str | None = "1g"
    tmpfs_size: str = "64m"
    network: str = "none"
    user: str = "10001:10001"
    workspace_root: str = "/workspace"
    codex_home: str = "/codex-home"
    command: tuple[str, ...] = ("sleep", "infinity")
    name: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    labels: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SandboxHandle:
    id: str
    name: str
    tenant_id: str
    conversation_id: str
    workspace_root: str
    codex_home: str
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CredentialBrokerPolicy:
    """Tenant-wide limits enforced before requests reach the model provider."""

    max_concurrent_requests: int = 2
    requests_per_minute: int = 120
    max_request_bytes: int = 32 * 1024 * 1024
    queue_timeout_s: float = 5.0
    allowed_models: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.max_concurrent_requests < 1:
            raise ValueError("credential broker concurrency must be positive")
        if self.requests_per_minute < 1:
            raise ValueError("credential broker request rate must be positive")
        if self.max_request_bytes < 1:
            raise ValueError("credential broker request size must be positive")
        if self.queue_timeout_s <= 0:
            raise ValueError("credential broker queue timeout must be positive")
        normalized = tuple(model.strip() for model in self.allowed_models)
        if any(not model for model in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("credential broker allowed models must be unique and non-empty")
        object.__setattr__(self, "allowed_models", normalized)


@dataclass(frozen=True, slots=True)
class CredentialBrokerEndpoint:
    """Conversation-network endpoint with no provider credential material."""

    base_url: str
    network_ip: str


@runtime_checkable
class CredentialBrokerProvisioner(Protocol):
    async def provision_credential_broker(
        self,
        handle: SandboxHandle,
        *,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        """Bind a tenant broker to the conversation without exposing the API key."""
        ...


class SandboxManager(Protocol):
    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        """Return a running sandbox for this spec, creating one if necessary."""
        ...

    async def destroy(self, handle: SandboxHandle) -> None:
        """Stop and remove a sandbox owned by this manager."""
        ...

    async def stop(self, handle: SandboxHandle) -> None:
        """Stop a sandbox without deleting its persistent state."""
        ...

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        """Ensure a directory exists inside the sandbox."""
        ...

    async def run_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        input_data: bytes | None = None,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
    ) -> None:
        """Run a bounded infrastructure command inside the sandbox."""
        ...

    def exec_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
        interactive: bool = True,
    ) -> list[str]:
        """Build a host command that executes inside the sandbox."""
        ...
