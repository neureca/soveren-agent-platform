"""Sandbox lifecycle contracts.

Sandboxes are execution-plane boundaries. They are not product tenants,
authorization policy, or app-owned business state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol


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
    workspace_root: str
    codex_home: str
    metadata: Mapping[str, str] = field(default_factory=dict)


class SandboxRuntime(Protocol):
    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        """Return a running sandbox for this spec, creating one if necessary."""
        ...

    async def destroy(self, handle: SandboxHandle) -> None:
        """Stop and remove a sandbox owned by this runtime."""
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
