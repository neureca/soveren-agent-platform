"""Credential provisioning for sandboxed Codex runtimes."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from soveren_agent_platform.sandbox import SandboxHandle, SandboxRuntime

MAX_AUTH_FILE_BYTES = 1024 * 1024


class CodexCredentialProvider(Protocol):
    async def provision(self, runtime: SandboxRuntime, handle: SandboxHandle) -> None:
        ...


@dataclass(frozen=True, slots=True)
class ExistingCodexCredentials:
    """Use credentials already persisted in the tenant CODEX_HOME."""

    async def provision(self, runtime: SandboxRuntime, handle: SandboxHandle) -> None:
        return None


@dataclass(frozen=True, slots=True)
class CodexApiKeyCredentials:
    """Provision API-key login without placing the key in Docker metadata."""

    api_key: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("Codex API key must not be empty")

    async def provision(self, runtime: SandboxRuntime, handle: SandboxHandle) -> None:
        await runtime.run_command(
            handle,
            ["codex", "login", "--with-api-key"],
            input_data=(self.api_key.strip() + "\n").encode(),
            env={"CODEX_HOME": handle.codex_home},
            workdir=handle.workspace_root,
        )


@dataclass(frozen=True, slots=True)
class CodexAuthFileCredentials:
    """Provision a trusted local Codex auth cache into the tenant CODEX_HOME."""

    path: Path

    async def provision(self, runtime: SandboxRuntime, handle: SandboxHandle) -> None:
        data = await asyncio.to_thread(self.path.read_bytes)
        if not data or len(data) > MAX_AUTH_FILE_BYTES:
            raise ValueError("Codex auth file must be non-empty and at most 1 MiB")
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError("Codex auth file must contain valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Codex auth file must contain a JSON object")
        await runtime.run_command(
            handle,
            ["sh", "-c", 'umask 077; test -s "$CODEX_HOME/auth.json" || cat > "$CODEX_HOME/auth.json"'],
            input_data=data,
            env={"CODEX_HOME": handle.codex_home},
            workdir=handle.workspace_root,
        )
