"""Docker-backed sandbox management.

This manager creates sibling containers through the host Docker daemon. It must
run only in a trusted runner process/container; tenant sandboxes must never get
the Docker socket.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
from dataclasses import dataclass, replace
from typing import Mapping

from soveren_agent_platform.sandbox.contracts import (
    CredentialBindingScope,
    CredentialBrokerCapability,
    CredentialBrokerEndpoint,
    CredentialBrokerPolicy,
    HttpCredentialBinding,
    SandboxHandle,
    SandboxSpec,
)
from soveren_agent_platform.sandbox.docker_broker import (
    DockerCredentialBrokerManager,
    DockerCredentialBrokerNetworkPreparation,
    DockerCredentialBrokerSpec,
)
from soveren_agent_platform.sandbox.docker_commands import (
    CommandResult,
    DockerCommandRunner,
    SubprocessDockerCommandRunner,
)
from soveren_agent_platform.sandbox.docker_labels import (
    CONVERSATION_KEY_LABEL,
    MANAGED_LABEL,
    RUNTIME_LABEL,
    TENANT_KEY_LABEL,
)

SPEC_HASH_LABEL = "soveren.spec_hash"
EGRESS_LABEL = "soveren.egress"
EGRESS_POLICY_LABEL = "soveren.egress_policy"
EGRESS_POLICY_VERSION = "1"
DOCKER_SANDBOX_POLICY_VERSION = "5"


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
class _DockerEgressConfiguration:
    image: str
    policy_version: str | None


@dataclass(frozen=True, slots=True)
class _DockerNetworkPolicy:
    network: str
    source: str
    destination: str

    @property
    def proxy_ip(self) -> str:
        return self.destination.removesuffix("/32")

    def proxy_response_rule(self) -> list[str]:
        return [
            "DOCKER-USER",
            "-s",
            self.destination,
            "-d",
            self.source,
            "-p",
            "tcp",
            "--sport",
            "3128",
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ]

    def legacy_proxy_egress_rule(self) -> list[str]:
        return ["DOCKER-USER", "-s", self.destination, "-j", "ACCEPT"]

    def allow_rule(self) -> list[str]:
        return [
            "DOCKER-USER",
            "-s",
            self.source,
            "-d",
            self.destination,
            "-p",
            "tcp",
            "--dport",
            "3128",
            "-j",
            "ACCEPT",
        ]

    def drop_rule(self) -> list[str]:
        return ["DOCKER-USER", "-s", self.source, "-j", "DROP"]

    def host_input_drop_rule(self) -> list[str]:
        return ["INPUT", "-s", self.source, "-j", "DROP"]


class DockerSandboxManager:
    """Manage isolated Docker sandboxes for one control-plane process."""

    def __init__(
        self,
        *,
        docker_command: tuple[str, ...] = ("docker",),
        runner: DockerCommandRunner | None = None,
        name_prefix: str = "soveren-sandbox",
        allowed_networks: frozenset[str] | None = None,
        max_active_sandboxes: int = 1,
        egress: DockerEgressSpec | None = None,
        credential_broker: DockerCredentialBrokerSpec | None = None,
        recover_orphaned_sandboxes: bool = False,
    ) -> None:
        if max_active_sandboxes < 1:
            raise ValueError("max_active_sandboxes must be positive")
        if credential_broker is not None:
            if egress is None:
                raise ValueError("Docker credential broker requires managed egress")
        self.docker_command = docker_command
        self.runner = runner or SubprocessDockerCommandRunner()
        self.name_prefix = _safe_name_component(name_prefix)
        default_networks = {"none", "soveren-sandbox-egress"}
        if egress is not None:
            default_networks.add(egress.internal_network)
        self.allowed_networks = allowed_networks if allowed_networks is not None else frozenset(default_networks)
        self.max_active_sandboxes = max_active_sandboxes
        self.egress = egress
        self.credential_broker = credential_broker
        self.recover_orphaned_sandboxes = recover_orphaned_sandboxes
        self._sandbox_locks: dict[str, asyncio.Lock] = {}
        self._tenant_lifecycle_locks: dict[str, asyncio.Lock] = {}
        self._credential_broker_manager = (
            DockerCredentialBrokerManager(host=self, spec=credential_broker) if credential_broker is not None else None
        )
        self._egress_lock = asyncio.Lock()
        self._recovery_lock = asyncio.Lock()
        self._orphan_recovery_complete = False
        self._capacity_condition = asyncio.Condition()
        self._active_conversation_keys: set[str] = set()

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        _validate_spec(spec)
        tenant_key = _tenant_key(spec.tenant_id)
        conversation_key = _conversation_key(spec.tenant_id, spec.conversation_id)
        managed_conversation_network = self._managed_conversation_network(
            spec,
            conversation_key=conversation_key,
        )
        if managed_conversation_network is not None:
            spec = replace(spec, network=managed_conversation_network)
        elif spec.network not in self.allowed_networks:
            allowed = ", ".join(sorted(self.allowed_networks))
            raise ValueError(f"sandbox network {spec.network!r} is not allowed; expected one of: {allowed}")
        if self.recover_orphaned_sandboxes:
            await self._recover_orphaned_sandboxes_once()
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock:
            reserved = await self._reserve_capacity(conversation_key)
            async with tenant_lifecycle_lock:
                network_policy: _DockerNetworkPolicy | None = None
                broker_preparation: DockerCredentialBrokerNetworkPreparation | None = None
                try:
                    if managed_conversation_network is not None:
                        network_policy = await self._ensure_egress(
                            internal_network=managed_conversation_network,
                            tenant_key=tenant_key,
                            conversation_key=conversation_key,
                        )
                    if self._credential_broker_manager is not None:
                        if managed_conversation_network is None:
                            await self._credential_broker_manager.remove_unowned(tenant_key)
                        else:
                            broker_preparation = await self._credential_broker_manager.prepare_tenant_network(
                                tenant_key,
                                managed_conversation_network,
                            )
                    handle = await self._acquire_locked(
                        spec,
                        tenant_key=tenant_key,
                        conversation_key=conversation_key,
                    )
                    handle = _with_network_policy(handle, network_policy)
                    if self.credential_broker is not None:
                        handle = replace(
                            handle,
                            metadata={
                                **handle.metadata,
                                "credential_broker_host": self.credential_broker.network_alias,
                            },
                        )
                    return handle
                except BaseException as acquire_error:
                    cleanup_errors: list[BaseException] = []
                    if (
                        network_policy is not None
                        and await self._find_container_id(tenant_key, conversation_key) is None
                    ):
                        if broker_preparation is not None and self._credential_broker_manager is not None:
                            try:
                                await self._credential_broker_manager.rollback_prepared_network(
                                    preparation=broker_preparation,
                                    network_subnet=network_policy.source,
                                )
                            except BaseException as exc:
                                cleanup_errors.append(exc)
                        try:
                            await self._cleanup_network_policy(network_policy)
                        except BaseException as exc:
                            cleanup_errors.append(exc)
                    if reserved:
                        await self._release_capacity(conversation_key)
                    if cleanup_errors:
                        raise BaseExceptionGroup(
                            "docker sandbox acquisition and network cleanup failed",
                            [acquire_error, *cleanup_errors],
                        ) from acquire_error
                    raise

    async def _acquire_locked(
        self,
        spec: SandboxSpec,
        *,
        tenant_key: str,
        conversation_key: str,
    ) -> SandboxHandle:
        name = _safe_name_component(spec.name) if spec.name else f"{self.name_prefix}-{conversation_key[:12]}"
        spec_hash = _spec_hash(spec)
        existing = await self._find_container_id(tenant_key, conversation_key)
        if existing:
            return await self._reuse_existing(
                existing,
                name=name,
                spec=spec,
                tenant_key=tenant_key,
                conversation_key=conversation_key,
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
            f"{CONVERSATION_KEY_LABEL}={conversation_key}",
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
            if key in {
                MANAGED_LABEL,
                RUNTIME_LABEL,
                TENANT_KEY_LABEL,
                CONVERSATION_KEY_LABEL,
                SPEC_HASH_LABEL,
            }:
                raise ValueError(f"reserved sandbox label: {key}")
            args.extend(["--label", f"{key}={value}"])
        for key, value in sorted(spec.env.items()):
            if not key or "=" in key:
                raise ValueError(f"invalid sandbox env key: {key!r}")
            args.extend(["-e", f"{key}={value}"])
        args.extend([spec.image, *spec.command])

        result = await self.runner.run(args)
        if result.returncode != 0:
            existing = await self._find_container_id(tenant_key, conversation_key)
            if existing:
                return await self._reuse_existing(
                    existing,
                    name=name,
                    spec=spec,
                    tenant_key=tenant_key,
                    conversation_key=conversation_key,
                    spec_hash=spec_hash,
                )
            self._raise_command_error(result)
        container_id = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else name
        return _handle(
            container_id=container_id,
            name=name,
            spec=spec,
            tenant_key=tenant_key,
            conversation_key=conversation_key,
            spec_hash=spec_hash,
        )

    async def destroy(self, handle: SandboxHandle) -> None:
        tenant_key = _tenant_key(handle.tenant_id)
        conversation_key = _handle_conversation_key(handle)
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock, tenant_lifecycle_lock:
            removed = await self.runner.run([*self.docker_command, "rm", "-f", handle.id])
            if removed.returncode != 0 and not self._is_missing_container_result(removed):
                self._raise_command_error(removed)
            try:
                await self._cleanup_tenant_network(handle)
            finally:
                await self._release_capacity(conversation_key)

    async def stop(self, handle: SandboxHandle) -> None:
        tenant_key = _tenant_key(handle.tenant_id)
        conversation_key = _handle_conversation_key(handle)
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock, tenant_lifecycle_lock:
            stopped = await self.runner.run([*self.docker_command, "stop", handle.id])
            if stopped.returncode != 0 and not self._is_missing_container_result(stopped):
                self._raise_command_error(stopped)
            try:
                if self._credential_broker_manager is not None:
                    await self._credential_broker_manager.remove_inactive(tenant_key)
            finally:
                await self._release_capacity(conversation_key)

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

    async def provision_credential_broker(
        self,
        handle: SandboxHandle,
        *,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        """Provision one in-memory API credential broker per tenant."""
        manager = self._credential_broker_manager
        if manager is None:
            raise RuntimeError("Docker credential broker is not configured")
        tenant_key = _tenant_key(handle.tenant_id)
        conversation_key = _conversation_key(handle.tenant_id, handle.conversation_id)
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock, tenant_lifecycle_lock:
            return await manager.provision(
                handle,
                tenant_key=tenant_key,
                conversation_key=conversation_key,
                api_key=api_key,
                policy=policy,
            )

    async def provision_http_credential(
        self,
        handle: SandboxHandle,
        *,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        """Provision or rotate one policy-bound HTTP credential."""
        manager = self._credential_broker_manager
        if manager is None:
            raise RuntimeError("Docker credential broker is not configured")
        tenant_key = _tenant_key(handle.tenant_id)
        conversation_key = _conversation_key(handle.tenant_id, handle.conversation_id)
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock, tenant_lifecycle_lock:
            return await manager.provision_http(
                handle,
                tenant_key=tenant_key,
                conversation_key=conversation_key,
                credential=credential,
                binding=binding,
            )

    async def revoke_http_credential(
        self,
        handle: SandboxHandle,
        *,
        name: str,
        scope: CredentialBindingScope = "conversation",
    ) -> None:
        """Revoke one policy-bound HTTP credential."""
        manager = self._credential_broker_manager
        if manager is None:
            raise RuntimeError("Docker credential broker is not configured")
        tenant_key = _tenant_key(handle.tenant_id)
        conversation_key = _conversation_key(handle.tenant_id, handle.conversation_id)
        conversation_lock = self._sandbox_locks.setdefault(conversation_key, asyncio.Lock())
        tenant_lifecycle_lock = self._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        async with conversation_lock, tenant_lifecycle_lock:
            await manager.revoke_http(
                handle,
                tenant_key=tenant_key,
                conversation_key=conversation_key,
                name=name,
                scope=scope,
            )

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

    async def _find_container_id(self, tenant_key: str, conversation_key: str) -> str | None:
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
                "--filter",
                f"label={CONVERSATION_KEY_LABEL}={conversation_key}",
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
            result = await self._run_checked(
                [
                    *self.docker_command,
                    "ps",
                    "-q",
                    "--filter",
                    f"label={MANAGED_LABEL}=true",
                    "--filter",
                    f"label={RUNTIME_LABEL}=docker",
                ]
            )
            container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            for container_id in container_ids:
                await self._run_checked([*self.docker_command, "stop", container_id])
            self._orphan_recovery_complete = True

    def _managed_conversation_network(
        self,
        spec: SandboxSpec,
        *,
        conversation_key: str,
    ) -> str | None:
        if self.egress is None or spec.network != self.egress.internal_network:
            return None
        return f"{self.egress.internal_network}-{conversation_key[:12]}"

    async def _ensure_egress(
        self,
        *,
        internal_network: str,
        tenant_key: str,
        conversation_key: str,
    ) -> _DockerNetworkPolicy:
        assert self.egress is not None
        async with self._egress_lock:
            network_created = False
            egress_connected = False
            try:
                network_created = await self._ensure_network(
                    internal_network,
                    internal=True,
                    labels={
                        MANAGED_LABEL: "true",
                        TENANT_KEY_LABEL: tenant_key,
                        CONVERSATION_KEY_LABEL: conversation_key,
                    },
                )
                network_subnet = await self._tenant_network_subnet(internal_network)
                await self._ensure_network(self.egress.public_network, internal=False)
                container_id = await self._ensure_current_egress_container()
                if not await self._is_running(container_id):
                    await self._run_checked([*self.docker_command, "start", container_id])
                egress_connected = await self._connect_egress_network(
                    container_id,
                    internal_network=internal_network,
                )
                await self._wait_for_egress_health(container_id)
                policy = await self._inspect_network_policy(
                    internal_network=internal_network,
                    network_subnet=network_subnet,
                    egress_container_id=container_id,
                )
                await self._ensure_network_policy(policy=policy)
                return policy
            except BaseException as setup_error:
                cleanup_errors: list[BaseException] = []
                if egress_connected:
                    try:
                        await self._disconnect_current_egress(internal_network)
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if network_created:
                    try:
                        await self._run_checked([*self.docker_command, "network", "rm", internal_network])
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                if cleanup_errors:
                    raise BaseExceptionGroup(
                        "docker egress setup and tenant network cleanup failed",
                        [setup_error, *cleanup_errors],
                    ) from setup_error
                raise

    async def _ensure_network(
        self,
        name: str,
        *,
        internal: bool,
        labels: Mapping[str, str] | None = None,
    ) -> bool:
        inspect_args = [*self.docker_command, "network", "inspect", "-f", "{{.Internal}}", name]
        result = await self.runner.run(inspect_args)
        if result.returncode == 0:
            self._validate_network_mode(name, result.stdout, internal=internal)
            if labels:
                await self._validate_network_labels(name, labels)
            return False
        create_args = [*self.docker_command, "network", "create"]
        if internal:
            create_args.append("--internal")
        for key, value in sorted((labels or {}).items()):
            create_args.extend(["--label", f"{key}={value}"])
        create_args.append(name)
        created = await self.runner.run(create_args)
        if created.returncode == 0:
            return True
        raced = await self.runner.run(inspect_args)
        if raced.returncode != 0:
            self._raise_command_error(created)
        self._validate_network_mode(name, raced.stdout, internal=internal)
        if labels:
            await self._validate_network_labels(name, labels)
        return False

    async def _validate_network_labels(
        self,
        name: str,
        expected: Mapping[str, str],
    ) -> None:
        result = await self._run_checked(
            [
                *self.docker_command,
                "network",
                "inspect",
                "-f",
                "{{json .Labels}}",
                name,
            ]
        )
        try:
            labels = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid managed network labels") from exc
        if not isinstance(labels, dict) or any(labels.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"Docker network {name!r} is not owned by the requested tenant conversation")

    async def _find_egress_container_id(self) -> str | None:
        result = await self._run_checked(
            [
                *self.docker_command,
                "ps",
                "-aq",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={EGRESS_LABEL}=true",
            ]
        )
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return ids[0] if ids else None

    async def _ensure_current_egress_container(self) -> str:
        container_id = await self._find_egress_container_id()
        if container_id is None:
            container_id = await self._create_egress_container()
        configuration = await self._inspect_egress_container(container_id)
        assert self.egress is not None
        if configuration.image == self.egress.image and configuration.policy_version == EGRESS_POLICY_VERSION:
            return container_id
        if configuration.policy_version != EGRESS_POLICY_VERSION:
            raise RuntimeError(
                "managed Docker egress firewall policy changed; apply the matching policy migration before retrying"
            )
        container_id = await self._replace_outdated_egress_image(container_id)
        replacement = await self._inspect_egress_container(container_id)
        if replacement.image != self.egress.image or replacement.policy_version != EGRESS_POLICY_VERSION:
            raise RuntimeError("managed Docker egress replacement has an unexpected configuration")
        return container_id

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
            "--label",
            f"{EGRESS_POLICY_LABEL}={EGRESS_POLICY_VERSION}",
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

    async def _inspect_egress_container(self, container_id: str) -> _DockerEgressConfiguration:
        result = await self._run_checked([*self.docker_command, "inspect", "-f", "{{json .Config}}", container_id])
        try:
            config = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid managed egress configuration") from exc
        if not isinstance(config, dict):
            raise RuntimeError("Docker returned invalid managed egress configuration")
        image = config.get("Image")
        labels = config.get("Labels") or {}
        if not isinstance(image, str) or not image or not isinstance(labels, dict):
            raise RuntimeError("Docker returned invalid managed egress configuration")
        policy_version = labels.get(EGRESS_POLICY_LABEL)
        if policy_version is not None and not isinstance(policy_version, str):
            raise RuntimeError("Docker returned invalid managed egress configuration")
        return _DockerEgressConfiguration(
            image=image,
            policy_version=policy_version,
        )

    async def _replace_outdated_egress_image(self, container_id: str) -> str:
        running_sandboxes = await self._running_managed_sandbox_ids()
        if running_sandboxes:
            raise RuntimeError(
                "managed Docker egress image changed while conversation sandboxes are running; "
                "stop the sandboxes before retrying"
            )
        policies = await self._attached_egress_network_policies(container_id)
        for policy in policies:
            await self._remove_iptables_rule(policy.proxy_response_rule())
            await self._remove_iptables_rule(policy.legacy_proxy_egress_rule())
            await self._remove_iptables_rule(policy.allow_rule())
        removed = await self.runner.run([*self.docker_command, "rm", "-f", container_id])
        if removed.returncode != 0 and not self._is_missing_container_result(removed):
            self._raise_command_error(removed)
        return await self._create_egress_container()

    async def _running_managed_sandbox_ids(self) -> list[str]:
        result = await self._run_checked(
            [
                *self.docker_command,
                "ps",
                "-q",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={RUNTIME_LABEL}=docker",
            ]
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    async def _attached_egress_network_policies(
        self,
        container_id: str,
    ) -> list[_DockerNetworkPolicy]:
        assert self.egress is not None
        networks = await self._egress_networks(container_id)
        if networks is None:
            return []
        managed_prefix = f"{self.egress.internal_network}-"
        policies: list[_DockerNetworkPolicy] = []
        for network in sorted(networks):
            if not network.startswith(managed_prefix):
                continue
            subnet = await self._tenant_network_subnet(network)
            policies.append(
                await self._inspect_network_policy(
                    internal_network=network,
                    network_subnet=subnet,
                    egress_container_id=container_id,
                )
            )
        return policies

    async def _connect_egress_network(
        self,
        container_id: str,
        *,
        internal_network: str,
    ) -> bool:
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
        if internal_network in networks:
            return False
        connected = await self.runner.run(
            [
                *self.docker_command,
                "network",
                "connect",
                "--alias",
                self.egress.container_name,
                internal_network,
                container_id,
            ]
        )
        if connected.returncode == 0:
            return True
        raced = await self._run_checked(inspect_args)
        try:
            raced_networks = json.loads(raced.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid egress network metadata") from exc
        if internal_network not in raced_networks:
            self._raise_command_error(connected)
        return False

    async def _tenant_network_subnet(self, internal_network: str) -> str:
        ipv6_result = await self._run_checked(
            [
                *self.docker_command,
                "network",
                "inspect",
                "-f",
                "{{.EnableIPv6}}",
                internal_network,
            ]
        )
        if ipv6_result.stdout.strip().lower() != "false":
            raise RuntimeError(
                "docker sandbox tenant networks must have IPv6 disabled until dual-stack firewall policy is supported"
            )
        subnet_result = await self._run_checked(
            [
                *self.docker_command,
                "network",
                "inspect",
                "-f",
                "{{(index .IPAM.Config 0).Subnet}}",
                internal_network,
            ]
        )
        try:
            subnet = ipaddress.ip_network(subnet_result.stdout.strip(), strict=False)
        except ValueError as exc:
            raise RuntimeError("Docker returned invalid tenant network policy metadata") from exc
        if subnet.version != 4:
            raise RuntimeError("Docker tenant network policy requires an IPv4 subnet")
        return str(subnet)

    async def _inspect_network_policy(
        self,
        *,
        internal_network: str,
        network_subnet: str,
        egress_container_id: str,
    ) -> _DockerNetworkPolicy:
        subnet = ipaddress.ip_network(network_subnet, strict=False)
        proxy_result = await self._run_checked(
            [
                *self.docker_command,
                "inspect",
                "-f",
                f"{{{{with index .NetworkSettings.Networks {json.dumps(internal_network)}}}}}"
                "{{.IPAddress}}{{end}}",
                egress_container_id,
            ]
        )
        try:
            proxy_ip = ipaddress.ip_address(proxy_result.stdout.strip())
        except ValueError as exc:
            raise RuntimeError("Docker returned invalid tenant network policy metadata") from exc
        if proxy_ip.version != 4 or proxy_ip not in subnet:
            raise RuntimeError("Docker egress proxy is not using an address inside its tenant network")
        return _DockerNetworkPolicy(
            network=internal_network,
            source=str(subnet),
            destination=f"{proxy_ip}/32",
        )

    async def _ensure_network_policy(
        self,
        *,
        policy: _DockerNetworkPolicy,
    ) -> None:
        created_rules: list[list[str]] = []
        try:
            for rule, force_first in (
                (policy.drop_rule(), False),
                (policy.host_input_drop_rule(), False),
                (policy.allow_rule(), True),
                (policy.proxy_response_rule(), True),
            ):
                if await self._ensure_iptables_rule(rule, force_first=force_first):
                    created_rules.append(rule)
            await self._remove_iptables_rule(policy.legacy_proxy_egress_rule())
        except BaseException as setup_error:
            cleanup_errors: list[BaseException] = []
            for rule in reversed(created_rules):
                try:
                    await self._remove_iptables_rule(rule)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if cleanup_errors:
                raise BaseExceptionGroup(
                    "docker firewall setup and partial-rule cleanup failed",
                    [setup_error, *cleanup_errors],
                ) from setup_error
            raise

    async def _ensure_iptables_rule(self, rule: list[str], *, force_first: bool = False) -> bool:
        checked = await self.runner.run(self._iptables_helper_command(["-C", *rule]))
        if checked.returncode == 0 and not force_first:
            return False
        if checked.returncode != 1:
            if checked.returncode != 0:
                raise RuntimeError(
                    "docker sandbox host firewall is unavailable; the DOCKER-USER iptables chain is required"
                )
        if checked.returncode == 0:
            deleted = await self.runner.run(self._iptables_helper_command(["-D", *rule]))
            if deleted.returncode != 0:
                raise RuntimeError("docker sandbox host firewall policy could not be reordered safely")
        inserted = await self.runner.run(self._iptables_helper_command(["-I", rule[0], "1", *rule[1:]]))
        if inserted.returncode != 0:
            install_error = RuntimeError(
                "docker sandbox host firewall policy could not be installed; refusing unconfined networking"
            )
            if checked.returncode == 0:
                restored = await self.runner.run(self._iptables_helper_command(["-A", *rule]))
                if restored.returncode != 0:
                    raise BaseExceptionGroup(
                        "docker firewall rule installation and restoration failed",
                        [
                            install_error,
                            RuntimeError("existing docker sandbox host firewall rule could not be restored"),
                        ],
                    ) from install_error
            raise install_error
        return checked.returncode != 0

    async def _cleanup_tenant_network(self, handle: SandboxHandle) -> None:
        if self.egress is None:
            return
        network = handle.metadata.get("network")
        managed_prefix = f"{self.egress.internal_network}-"
        if not network or not network.startswith(managed_prefix):
            return
        source = handle.metadata.get("network_subnet")
        proxy_ip = handle.metadata.get("egress_proxy_ip")
        if source and proxy_ip:
            policy = _DockerNetworkPolicy(
                network=network,
                source=str(ipaddress.ip_network(source, strict=False)),
                destination=f"{ipaddress.ip_address(proxy_ip)}/32",
            )
        else:
            egress_container_id = await self._find_egress_container_id()
            if egress_container_id is None:
                raise RuntimeError("managed Docker egress disappeared before legacy tenant network cleanup")
            source = await self._tenant_network_subnet(network)
            policy = await self._inspect_network_policy(
                internal_network=network,
                network_subnet=source,
                egress_container_id=egress_container_id,
            )
        manager = self._credential_broker_manager
        tenant_key = _tenant_key(handle.tenant_id)
        if manager is not None:
            await manager.cleanup_network(
                handle,
                tenant_key=tenant_key,
                network=network,
                network_subnet=policy.source,
            )
        await self._cleanup_network_policy(policy)
        if manager is not None:
            await manager.remove_unused(tenant_key)

    async def _cleanup_network_policy(self, policy: _DockerNetworkPolicy) -> None:
        policies = [policy]
        current_policy = await self._current_network_policy(policy.network, network_subnet=policy.source)
        if current_policy is not None and current_policy.destination != policy.destination:
            policies.append(current_policy)
        for active_policy in policies:
            await self._remove_iptables_rule(active_policy.proxy_response_rule())
            await self._remove_iptables_rule(active_policy.legacy_proxy_egress_rule())
            await self._remove_iptables_rule(active_policy.allow_rule())
        await self._disconnect_current_egress(policy.network)
        removed = await self.runner.run([*self.docker_command, "network", "rm", policy.network])
        if removed.returncode != 0 and "not found" not in (removed.stderr + removed.stdout).lower():
            self._raise_command_error(removed)
        await self._remove_iptables_rule(policy.drop_rule())
        await self._remove_iptables_rule(policy.host_input_drop_rule())

    async def _current_network_policy(
        self,
        network: str,
        *,
        network_subnet: str,
    ) -> _DockerNetworkPolicy | None:
        egress_container_id = await self._find_egress_container_id()
        if egress_container_id is None:
            return None
        running = await self._is_running_or_missing(egress_container_id)
        if running is not True:
            return None
        networks = await self._egress_networks(egress_container_id)
        if networks is None or network not in networks:
            return None
        try:
            return await self._inspect_network_policy(
                internal_network=network,
                network_subnet=network_subnet,
                egress_container_id=egress_container_id,
            )
        except RuntimeError:
            if await self._is_running_or_missing(egress_container_id) is not True:
                return None
            raise

    async def _disconnect_current_egress(self, network: str) -> None:
        egress_container_id = await self._find_egress_container_id()
        if egress_container_id is None:
            return
        networks = await self._egress_networks(egress_container_id)
        if networks is None or network not in networks:
            return
        disconnected = await self.runner.run(
            [
                *self.docker_command,
                "network",
                "disconnect",
                "-f",
                network,
                egress_container_id,
            ]
        )
        if disconnected.returncode != 0 and not self._is_missing_container_result(disconnected):
            self._raise_command_error(disconnected)

    async def _egress_networks(self, egress_container_id: str) -> dict[str, object] | None:
        inspect = await self.runner.run(
            [
                *self.docker_command,
                "inspect",
                "-f",
                "{{json .NetworkSettings.Networks}}",
                egress_container_id,
            ]
        )
        if inspect.returncode != 0:
            if self._is_missing_container_result(inspect):
                return None
            self._raise_command_error(inspect)
        try:
            networks = json.loads(inspect.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid egress network metadata") from exc
        if not isinstance(networks, dict):
            raise RuntimeError("Docker returned invalid egress network metadata")
        return networks

    async def _remove_iptables_rule(self, rule: list[str]) -> None:
        checked = await self.runner.run(self._iptables_helper_command(["-C", *rule]))
        if checked.returncode == 1:
            return
        if checked.returncode != 0:
            raise RuntimeError("docker sandbox host firewall policy could not be inspected during cleanup")
        deleted = await self.runner.run(self._iptables_helper_command(["-D", *rule]))
        if deleted.returncode != 0:
            raise RuntimeError("docker sandbox host firewall policy could not be removed")

    def _iptables_helper_command(self, args: list[str]) -> list[str]:
        assert self.egress is not None
        return [
            *self.docker_command,
            "run",
            "--rm",
            "--network",
            "host",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "NET_ADMIN",
            "--security-opt",
            "no-new-privileges:true",
            "--entrypoint",
            "iptables",
            self.egress.image,
            *args,
        ]

    async def _wait_for_egress_health(self, container_id: str) -> None:
        for _ in range(30):
            result = await self._run_checked(
                [
                    *self.docker_command,
                    "inspect",
                    "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}",
                    container_id,
                ]
            )
            status = result.stdout.strip().lower()
            if status == "healthy":
                return
            if status in {"unhealthy", "missing"}:
                raise RuntimeError(f"managed Docker egress is {status}")
            await asyncio.sleep(1.0)
        raise RuntimeError("managed Docker egress did not become healthy")

    async def _is_running(self, container_id: str) -> bool:
        result = await self._run_checked([*self.docker_command, "inspect", "-f", "{{.State.Running}}", container_id])
        return result.stdout.strip().lower() == "true"

    async def _is_running_or_missing(self, container_id: str) -> bool | None:
        result = await self.runner.run([*self.docker_command, "inspect", "-f", "{{.State.Running}}", container_id])
        if result.returncode != 0:
            if self._is_missing_container_result(result):
                return None
            self._raise_command_error(result)
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
        conversation_key: str,
        spec_hash: str,
    ) -> SandboxHandle:
        existing_spec_hash = await self._inspect_label(container_id, SPEC_HASH_LABEL)
        actual_spec = spec
        if existing_spec_hash != spec_hash:
            existing_image = await self._inspect_container_image(container_id)
            retained_spec = replace(spec, image=existing_image)
            if _spec_hash(retained_spec) != existing_spec_hash:
                raise RuntimeError(
                    "docker sandbox config changed for existing container; "
                    "destroy or migrate the sandbox before acquiring it again"
                )
            actual_spec = retained_spec
        running = await self._is_running(container_id)
        if not running:
            await self._run_checked([*self.docker_command, "start", container_id])
        return _handle(
            container_id=container_id,
            name=name,
            spec=actual_spec,
            tenant_key=tenant_key,
            conversation_key=conversation_key,
            spec_hash=existing_spec_hash or spec_hash,
            configured_image=spec.image,
        )

    async def _inspect_container_image(self, container_id: str) -> str:
        result = await self._run_checked([*self.docker_command, "inspect", "-f", "{{.Config.Image}}", container_id])
        image = result.stdout.strip()
        if not image:
            raise RuntimeError("Docker returned an empty sandbox image")
        return image

    async def _run_checked(self, args: list[str]) -> CommandResult:
        result = await self._run_docker(args)
        if result.returncode != 0:
            self._raise_command_error(result)
        return result

    async def _run_docker(
        self,
        args: list[str],
        *,
        input_data: bytes | None = None,
    ) -> CommandResult:
        return await self.runner.run(args, input_data=input_data)

    async def _reserve_capacity(self, conversation_key: str) -> bool:
        async with self._capacity_condition:
            if conversation_key in self._active_conversation_keys:
                return False
            await self._capacity_condition.wait_for(
                lambda: len(self._active_conversation_keys) < self.max_active_sandboxes
            )
            self._active_conversation_keys.add(conversation_key)
            return True

    async def _release_capacity(self, conversation_key: str) -> None:
        async with self._capacity_condition:
            self._active_conversation_keys.discard(conversation_key)
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
    def _is_missing_container_result(result: CommandResult) -> bool:
        detail = (result.stderr + result.stdout).lower()
        return "no such container" in detail or "no such object" in detail

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
    conversation_key: str,
    spec_hash: str,
    configured_image: str | None = None,
) -> SandboxHandle:
    metadata = {
        "runtime": "docker",
        "tenant_key": tenant_key,
        "conversation_key": conversation_key,
        "image": spec.image,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "network": spec.network,
        "spec_hash": spec_hash,
    }
    if configured_image is not None and configured_image != spec.image:
        metadata["configured_image"] = configured_image
        metadata["image_update_state"] = "deferred_until_destroy"
    return SandboxHandle(
        id=container_id,
        name=name,
        tenant_id=spec.tenant_id,
        conversation_id=spec.conversation_id,
        workspace_root=spec.workspace_root,
        codex_home=spec.codex_home,
        metadata=metadata,
    )


def _with_network_policy(
    handle: SandboxHandle,
    policy: _DockerNetworkPolicy | None,
) -> SandboxHandle:
    if policy is None:
        return handle
    return replace(
        handle,
        metadata={
            **handle.metadata,
            "network": policy.network,
            "network_subnet": policy.source,
            "egress_proxy_ip": policy.proxy_ip,
        },
    )


def _tenant_key(tenant_id: str) -> str:
    return hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()


def _conversation_key(tenant_id: str, conversation_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}\0{conversation_id}".encode("utf-8")).hexdigest()


def _spec_hash(spec: SandboxSpec) -> str:
    payload = {
        "policy_version": DOCKER_SANDBOX_POLICY_VERSION,
        "conversation_id": spec.conversation_id,
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
    if not spec.conversation_id:
        raise ValueError("sandbox conversation_id must not be empty")
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


def _handle_conversation_key(handle: SandboxHandle) -> str:
    conversation_key = handle.metadata.get("conversation_key")
    return conversation_key or _conversation_key(handle.tenant_id, handle.conversation_id)
