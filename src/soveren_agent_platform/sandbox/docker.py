"""Docker-backed sandbox runtime.

This runtime creates sibling containers through the host Docker daemon. It must
run only in a trusted runner process/container; tenant sandboxes must never get
the Docker socket.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Mapping, Protocol

from soveren_agent_platform.sandbox.contracts import SandboxHandle, SandboxSpec

MANAGED_LABEL = "soveren.managed"
TENANT_KEY_LABEL = "soveren.tenant_key"
RUNTIME_LABEL = "soveren.runtime"
SPEC_HASH_LABEL = "soveren.spec_hash"
EGRESS_LABEL = "soveren.egress"
DOCKER_SANDBOX_POLICY_VERSION = "1"


@dataclass(frozen=True, slots=True)
class DockerEgressSpec:
    image: str
    container_name: str = "soveren-sandbox-egress"
    internal_network: str = "soveren-sandbox-egress"
    public_network: str = "soveren-sandbox-public-egress"
    memory: str = "64m"
    cpus: str = "0.25"
    pids_limit: int = 64


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class DockerCommandRunner(Protocol):
    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        ...


class SubprocessDockerCommandRunner:
    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if input_data is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(input_data)
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
        allowed_networks: frozenset[str] | None = None,
        max_active_sandboxes: int = 1,
        egress: DockerEgressSpec | None = None,
        recover_orphaned_sandboxes: bool = False,
    ) -> None:
        if max_active_sandboxes < 1:
            raise ValueError("max_active_sandboxes must be positive")
        self.docker_command = docker_command
        self.runner = runner or SubprocessDockerCommandRunner()
        self.name_prefix = _safe_name_component(name_prefix)
        default_networks = {"none", "soveren-sandbox-egress"}
        if egress is not None:
            default_networks.add(egress.internal_network)
        self.allowed_networks = allowed_networks if allowed_networks is not None else frozenset(default_networks)
        self.max_active_sandboxes = max_active_sandboxes
        self.egress = egress
        self.recover_orphaned_sandboxes = recover_orphaned_sandboxes
        self._tenant_locks: dict[str, asyncio.Lock] = {}
        self._egress_lock = asyncio.Lock()
        self._recovery_lock = asyncio.Lock()
        self._orphan_recovery_complete = False
        self._capacity_condition = asyncio.Condition()
        self._active_tenant_keys: set[str] = set()

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        _validate_spec(spec)
        if spec.network not in self.allowed_networks:
            allowed = ", ".join(sorted(self.allowed_networks))
            raise ValueError(f"sandbox network {spec.network!r} is not allowed; expected one of: {allowed}")
        if self.recover_orphaned_sandboxes:
            await self._recover_orphaned_sandboxes_once()
        if self.egress is not None and spec.network == self.egress.internal_network:
            await self._ensure_egress()
        tenant_key = _tenant_key(spec.tenant_id)
        lock = self._tenant_locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            reserved = await self._reserve_capacity(tenant_key)
            try:
                return await self._acquire_locked(spec, tenant_key=tenant_key)
            except BaseException:
                if reserved:
                    await self._release_capacity(tenant_key)
                raise

    async def _acquire_locked(self, spec: SandboxSpec, *, tenant_key: str) -> SandboxHandle:
        name = _safe_name_component(spec.name) if spec.name else f"{self.name_prefix}-{tenant_key[:12]}"
        spec_hash = _spec_hash(spec)
        existing = await self._find_container_id(tenant_key)
        if existing:
            return await self._reuse_existing(
                existing,
                name=name,
                spec=spec,
                tenant_key=tenant_key,
                spec_hash=spec_hash,
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
            "--label",
            f"{SPEC_HASH_LABEL}={spec_hash}",
            "--memory",
            spec.memory,
            "--cpus",
            spec.cpus,
            "--pids-limit",
            str(spec.pids_limit),
            "--tmpfs",
            f"/tmp:rw,nosuid,nodev,size={spec.tmpfs_size}",
            "--user",
            spec.user,
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--init",
            "--network",
            spec.network,
        ]
        if spec.disk_limit is not None:
            args.extend(["--storage-opt", f"size={spec.disk_limit}"])
        for key, value in sorted(spec.labels.items()):
            if key in {MANAGED_LABEL, RUNTIME_LABEL, TENANT_KEY_LABEL, SPEC_HASH_LABEL}:
                raise ValueError(f"reserved sandbox label: {key}")
            args.extend(["--label", f"{key}={value}"])
        for key, value in sorted(spec.env.items()):
            if not key or "=" in key:
                raise ValueError(f"invalid sandbox env key: {key!r}")
            args.extend(["-e", f"{key}={value}"])
        args.extend([spec.image, *spec.command])

        result = await self.runner.run(args)
        if result.returncode != 0:
            existing = await self._find_container_id(tenant_key)
            if existing:
                return await self._reuse_existing(
                    existing,
                    name=name,
                    spec=spec,
                    tenant_key=tenant_key,
                    spec_hash=spec_hash,
                )
            self._raise_command_error(result)
        container_id = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else name
        return _handle(
            container_id=container_id,
            name=name,
            spec=spec,
            tenant_key=tenant_key,
            spec_hash=spec_hash,
        )

    async def destroy(self, handle: SandboxHandle) -> None:
        await self._run_checked([*self.docker_command, "rm", "-f", handle.id])
        await self._release_capacity(_handle_tenant_key(handle))

    async def stop(self, handle: SandboxHandle) -> None:
        await self._run_checked([*self.docker_command, "stop", handle.id])
        await self._release_capacity(_handle_tenant_key(handle))

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        _validate_container_path(path)
        await self._run_checked([*self.docker_command, "exec", handle.id, "mkdir", "-p", path])

    async def run_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        input_data: bytes | None = None,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
    ) -> None:
        args = self.exec_command(
            handle,
            command,
            env=env,
            workdir=workdir,
            interactive=input_data is not None,
        )
        result = await self.runner.run(args, input_data=input_data)
        if result.returncode != 0:
            self._raise_command_error(result)

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

    async def _recover_orphaned_sandboxes_once(self) -> None:
        if self._orphan_recovery_complete:
            return
        async with self._recovery_lock:
            if self._orphan_recovery_complete:
                return
            result = await self._run_checked([
                *self.docker_command,
                "ps",
                "-q",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={RUNTIME_LABEL}=docker",
            ])
            container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            for container_id in container_ids:
                await self._run_checked([*self.docker_command, "stop", container_id])
            self._orphan_recovery_complete = True

    async def _ensure_egress(self) -> None:
        assert self.egress is not None
        async with self._egress_lock:
            await self._ensure_network(self.egress.internal_network, internal=True)
            await self._ensure_network(self.egress.public_network, internal=False)
            container_id = await self._find_egress_container_id()
            if container_id is None:
                container_id = await self._create_egress_container()
            await self._validate_egress_container(container_id)
            if not await self._is_running(container_id):
                await self._run_checked([*self.docker_command, "start", container_id])
            await self._connect_egress_network(container_id)
            await self._wait_for_egress_health(container_id)

    async def _ensure_network(self, name: str, *, internal: bool) -> None:
        inspect_args = [*self.docker_command, "network", "inspect", "-f", "{{.Internal}}", name]
        result = await self.runner.run(inspect_args)
        if result.returncode == 0:
            self._validate_network_mode(name, result.stdout, internal=internal)
            return
        create_args = [*self.docker_command, "network", "create"]
        if internal:
            create_args.append("--internal")
        create_args.append(name)
        created = await self.runner.run(create_args)
        if created.returncode == 0:
            return
        raced = await self.runner.run(inspect_args)
        if raced.returncode != 0:
            self._raise_command_error(created)
        self._validate_network_mode(name, raced.stdout, internal=internal)

    async def _find_egress_container_id(self) -> str | None:
        result = await self._run_checked([
            *self.docker_command,
            "ps",
            "-aq",
            "--filter",
            f"label={MANAGED_LABEL}=true",
            "--filter",
            f"label={EGRESS_LABEL}=true",
        ])
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return ids[0] if ids else None

    async def _create_egress_container(self) -> str:
        assert self.egress is not None
        args = [
            *self.docker_command,
            "run",
            "-d",
            "--name",
            self.egress.container_name,
            "--label",
            f"{MANAGED_LABEL}=true",
            "--label",
            f"{EGRESS_LABEL}=true",
            "--restart",
            "unless-stopped",
            "--memory",
            self.egress.memory,
            "--cpus",
            self.egress.cpus,
            "--pids-limit",
            str(self.egress.pids_limit),
            "--read-only",
            "--tmpfs",
            "/run:rw,nosuid,nodev,size=8m",
            "--tmpfs",
            "/var/log/squid:rw,nosuid,nodev,size=8m,mode=0777",
            "--tmpfs",
            "/var/spool/squid:rw,nosuid,nodev,size=8m,mode=0777",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "CHOWN",
            "--cap-add",
            "SETGID",
            "--cap-add",
            "SETUID",
            "--security-opt",
            "no-new-privileges:true",
            "--network",
            self.egress.public_network,
            self.egress.image,
        ]
        result = await self.runner.run(args)
        if result.returncode == 0:
            return result.stdout.strip().splitlines()[-1] if result.stdout.strip() else self.egress.container_name
        existing = await self._find_egress_container_id()
        if existing is not None:
            return existing
        self._raise_command_error(result)
        raise AssertionError("unreachable")

    async def _validate_egress_container(self, container_id: str) -> None:
        assert self.egress is not None
        result = await self._run_checked(
            [*self.docker_command, "inspect", "-f", "{{.Config.Image}}", container_id]
        )
        if result.stdout.strip() != self.egress.image:
            raise RuntimeError(
                "managed Docker egress image changed; remove the existing egress container before retrying"
            )

    async def _connect_egress_network(self, container_id: str) -> None:
        assert self.egress is not None
        inspect_args = [
            *self.docker_command,
            "inspect",
            "-f",
            "{{json .NetworkSettings.Networks}}",
            container_id,
        ]
        result = await self._run_checked(inspect_args)
        try:
            networks = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid egress network metadata") from exc
        if self.egress.internal_network in networks:
            return
        connected = await self.runner.run([
            *self.docker_command,
            "network",
            "connect",
            "--alias",
            self.egress.container_name,
            self.egress.internal_network,
            container_id,
        ])
        if connected.returncode == 0:
            return
        raced = await self._run_checked(inspect_args)
        try:
            raced_networks = json.loads(raced.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid egress network metadata") from exc
        if self.egress.internal_network not in raced_networks:
            self._raise_command_error(connected)

    async def _wait_for_egress_health(self, container_id: str) -> None:
        for _ in range(30):
            result = await self._run_checked([
                *self.docker_command,
                "inspect",
                "-f",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}",
                container_id,
            ])
            status = result.stdout.strip().lower()
            if status == "healthy":
                return
            if status in {"unhealthy", "missing"}:
                raise RuntimeError(f"managed Docker egress is {status}")
            await asyncio.sleep(1.0)
        raise RuntimeError("managed Docker egress did not become healthy")

    async def _is_running(self, container_id: str) -> bool:
        result = await self._run_checked(
            [*self.docker_command, "inspect", "-f", "{{.State.Running}}", container_id]
        )
        return result.stdout.strip().lower() == "true"

    async def _inspect_label(self, container_id: str, label: str) -> str | None:
        result = await self._run_checked(
            [*self.docker_command, "inspect", "-f", f"{{{{ index .Config.Labels {json.dumps(label)} }}}}", container_id]
        )
        value = result.stdout.strip()
        return value or None

    async def _reuse_existing(
        self,
        container_id: str,
        *,
        name: str,
        spec: SandboxSpec,
        tenant_key: str,
        spec_hash: str,
    ) -> SandboxHandle:
        existing_spec_hash = await self._inspect_label(container_id, SPEC_HASH_LABEL)
        if existing_spec_hash != spec_hash:
            raise RuntimeError(
                "docker sandbox config changed for existing container; "
                "destroy or migrate the sandbox before acquiring it again"
            )
        running = await self._is_running(container_id)
        if not running:
            await self._run_checked([*self.docker_command, "start", container_id])
        return _handle(
            container_id=container_id,
            name=name,
            spec=spec,
            tenant_key=tenant_key,
            spec_hash=spec_hash,
        )

    async def _run_checked(self, args: list[str]) -> CommandResult:
        result = await self.runner.run(args)
        if result.returncode != 0:
            self._raise_command_error(result)
        return result

    async def _reserve_capacity(self, tenant_key: str) -> bool:
        async with self._capacity_condition:
            if tenant_key in self._active_tenant_keys:
                return False
            await self._capacity_condition.wait_for(
                lambda: len(self._active_tenant_keys) < self.max_active_sandboxes
            )
            self._active_tenant_keys.add(tenant_key)
            return True

    async def _release_capacity(self, tenant_key: str) -> None:
        async with self._capacity_condition:
            self._active_tenant_keys.discard(tenant_key)
            self._capacity_condition.notify_all()

    @staticmethod
    def _raise_command_error(result: CommandResult) -> None:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        if "storage-opt" in detail.lower():
            raise RuntimeError(
                "docker sandbox disk quota is unavailable; configure a quota-capable Docker "
                "storage driver (overlay2 requires XFS with pquota)"
            )
        raise RuntimeError(f"docker sandbox command failed: {detail}")

    @staticmethod
    def _validate_network_mode(name: str, value: str, *, internal: bool) -> None:
        actual = value.strip().lower() == "true"
        if actual != internal:
            expected = "internal" if internal else "public"
            raise RuntimeError(f"Docker network {name!r} exists but is not {expected}")


def _handle(
    *,
    container_id: str,
    name: str,
    spec: SandboxSpec,
    tenant_key: str,
    spec_hash: str,
) -> SandboxHandle:
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
            "spec_hash": spec_hash,
        },
    )


def _tenant_key(tenant_id: str) -> str:
    return hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()


def _spec_hash(spec: SandboxSpec) -> str:
    payload = {
        "policy_version": DOCKER_SANDBOX_POLICY_VERSION,
        "image": spec.image,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
        "disk_limit": spec.disk_limit,
        "tmpfs_size": spec.tmpfs_size,
        "network": spec.network,
        "user": spec.user,
        "workspace_root": spec.workspace_root,
        "codex_home": spec.codex_home,
        "command": list(spec.command),
        "env": dict(sorted(spec.env.items())),
        "labels": dict(sorted(spec.labels.items())),
        "name": spec.name,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
    if spec.disk_limit is not None and not spec.disk_limit:
        raise ValueError("sandbox disk_limit must not be empty")
    if not spec.tmpfs_size:
        raise ValueError("sandbox tmpfs_size must not be empty")
    if not spec.user:
        raise ValueError("sandbox user must not be empty")
    if spec.network == "host" or spec.network.startswith("container:"):
        raise ValueError("sandbox network must not be host or another container namespace")
    _validate_container_path(spec.workspace_root)
    _validate_container_path(spec.codex_home)
    if not spec.command:
        raise ValueError("sandbox command must not be empty")


def _validate_container_path(path: str) -> None:
    if not path.startswith("/"):
        raise ValueError("sandbox paths must be absolute container paths")


def _handle_tenant_key(handle: SandboxHandle) -> str:
    tenant_key = handle.metadata.get("tenant_key")
    return tenant_key or _tenant_key(handle.tenant_id)
