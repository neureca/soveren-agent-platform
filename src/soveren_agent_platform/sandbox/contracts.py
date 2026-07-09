"""Sandbox lifecycle contracts.

Sandboxes are execution-plane boundaries. They are not product tenants,
authorization policy, or app-owned business state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol


@dataclass(frozen=True, slots=True)
class SandboxSpec:
    tenant_id: str
    image: str
    memory: str = "512m"
    cpus: str = "0.5"
    pids_limit: int = 128
    network: str = "bridge"
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

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        """Ensure a directory exists inside the sandbox."""
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
