"""Docker-backed sandbox runtime.

This runtime creates sibling containers through the host Docker daemon. It must
run only in a trusted runner process/container; tenant sandboxes must never get
the Docker socket.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Mapping, Protocol

from soveren_agent_platform.sandbox.contracts import SandboxHandle, SandboxSpec

MANAGED_LABEL = "soveren.managed"
TENANT_KEY_LABEL = "soveren.tenant_key"
RUNTIME_LABEL = "soveren.runtime"


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class DockerCommandRunner(Protocol):
    async def run(self, args: list[str]) -> CommandResult:
        ...


class SubprocessDockerCommandRunner:
    async def run(self, args: list[str]) -> CommandResult:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        returncode = proc.returncode if proc.returncode is not None else -1
        return CommandResult(
            returncode=returncode,
            stdout=stdout.decode("utf-8", "replace"),
            stderr=stderr.decode("utf-8", "replace"),
        )


class DockerSandboxRuntime:
    """Minimal Docker CLI sandbox runtime for single-host compose deployments."""

    def __init__(
        self,
        *,
        docker_command: tuple[str, ...] = ("docker",),
        runner: DockerCommandRunner | None = None,
        name_prefix: str = "soveren-sandbox",
    ) -> None:
        self.docker_command = docker_command
        self.runner = runner or SubprocessDockerCommandRunner()
        self.name_prefix = _safe_name_component(name_prefix)

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        _validate_spec(spec)
        tenant_key = _tenant_key(spec.tenant_id)
        name = _safe_name_component(spec.name) if spec.name else f"{self.name_prefix}-{tenant_key[:12]}"
        existing = await self._find_container_id(tenant_key)
        if existing:
            running = await self._is_running(existing)
            if not running:
                await self._run_checked([*self.docker_command, "start", existing])
            return _handle(
                container_id=existing,
                name=name,
                spec=spec,
                tenant_key=tenant_key,
            )

        args = [
            *self.docker_command,
            "run",
            "-d",
            "--name",
            name,
            "--label",
            f"{MANAGED_LABEL}=true",
            "--label",
            f"{RUNTIME_LABEL}=docker",
            "--label",
            f"{TENANT_KEY_LABEL}={tenant_key}",
            "--memory",
            spec.memory,
            "--cpus",
            spec.cpus,
            "--pids-limit",
            str(spec.pids_limit),
            "--network",
            spec.network,
        ]
        for key, value in sorted(spec.labels.items()):
            if key in {MANAGED_LABEL, RUNTIME_LABEL, TENANT_KEY_LABEL}:
                raise ValueError(f"reserved sandbox label: {key}")
            args.extend(["--label", f"{key}={value}"])
        for key, value in sorted(spec.env.items()):
            if not key or "=" in key:
                raise ValueError(f"invalid sandbox env key: {key!r}")
            args.extend(["-e", f"{key}={value}"])
        args.extend([spec.image, *spec.command])

        result = await self._run_checked(args)
        container_id = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else name
        return _handle(
            container_id=container_id,
            name=name,
            spec=spec,
            tenant_key=tenant_key,
        )

    async def destroy(self, handle: SandboxHandle) -> None:
        await self._run_checked([*self.docker_command, "rm", "-f", handle.id])

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        _validate_container_path(path)
        await self._run_checked([*self.docker_command, "exec", handle.id, "mkdir", "-p", path])

    def exec_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
        interactive: bool = True,
    ) -> list[str]:
        if not command:
            raise ValueError("exec command must not be empty")
        args = [*self.docker_command, "exec"]
        if interactive:
            args.append("-i")
        if workdir is not None:
            _validate_container_path(workdir)
            args.extend(["-w", workdir])
        for key, value in sorted((env or {}).items()):
            if not key or "=" in key:
                raise ValueError(f"invalid sandbox env key: {key!r}")
            args.extend(["-e", f"{key}={value}"])
        args.extend([handle.id, *command])
        return args

    async def _find_container_id(self, tenant_key: str) -> str | None:
        result = await self._run_checked(
            [
                *self.docker_command,
                "ps",
                "-aq",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={RUNTIME_LABEL}=docker",
                "--filter",
                f"label={TENANT_KEY_LABEL}={tenant_key}",
            ]
        )
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return ids[0] if ids else None

    async def _is_running(self, container_id: str) -> bool:
        result = await self._run_checked(
            [*self.docker_command, "inspect", "-f", "{{.State.Running}}", container_id]
        )
        return result.stdout.strip().lower() == "true"

    async def _run_checked(self, args: list[str]) -> CommandResult:
        result = await self.runner.run(args)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
            raise RuntimeError(f"docker sandbox command failed: {detail}")
        return result


def _handle(*, container_id: str, name: str, spec: SandboxSpec, tenant_key: str) -> SandboxHandle:
    return SandboxHandle(
        id=container_id,
        name=name,
        tenant_id=spec.tenant_id,
        workspace_root=spec.workspace_root,
        codex_home=spec.codex_home,
        metadata={
            "runtime": "docker",
            "tenant_key": tenant_key,
            "image": spec.image,
            "memory": spec.memory,
            "cpus": spec.cpus,
            "network": spec.network,
        },
    )


def _tenant_key(tenant_id: str) -> str:
    return hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()


def _safe_name_component(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-")
    if not safe:
        raise ValueError("docker sandbox name component must not be empty")
    return safe[:64]


def _validate_spec(spec: SandboxSpec) -> None:
    if not spec.tenant_id:
        raise ValueError("sandbox tenant_id must not be empty")
    if not spec.image:
        raise ValueError("sandbox image must not be empty")
    if not spec.memory:
        raise ValueError("sandbox memory must not be empty")
    if not spec.cpus:
        raise ValueError("sandbox cpus must not be empty")
    if spec.pids_limit < 1:
        raise ValueError("sandbox pids_limit must be positive")
    if spec.network == "host" or spec.network.startswith("container:"):
        raise ValueError("sandbox network must not be host or another container namespace")
    _validate_container_path(spec.workspace_root)
    _validate_container_path(spec.codex_home)
    if not spec.command:
        raise ValueError("sandbox command must not be empty")


def _validate_container_path(path: str) -> None:
    if not path.startswith("/"):
        raise ValueError("sandbox paths must be absolute container paths")
