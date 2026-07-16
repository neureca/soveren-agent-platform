"""Tenant credential-broker lifecycle for the bundled Docker sandbox manager."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import re
import secrets
from dataclasses import dataclass, field, replace
from typing import Literal, Protocol

from soveren_agent_platform.sandbox.contracts import (
    CredentialBindingScope,
    CredentialBrokerCapability,
    CredentialBrokerEndpoint,
    CredentialBrokerPolicy,
    CredentialUsagePolicy,
    HttpCredentialBinding,
    SandboxHandle,
)
from soveren_agent_platform.sandbox.docker_commands import CommandResult
from soveren_agent_platform.sandbox.docker_labels import (
    CONVERSATION_KEY_LABEL,
    MANAGED_LABEL,
    RUNTIME_LABEL,
    TENANT_KEY_LABEL,
)

CREDENTIAL_BROKER_LABEL = "soveren.credential_broker"
CREDENTIAL_BROKER_POLICY_LABEL = "soveren.credential_broker_policy"
CREDENTIAL_BROKER_SPEC_HASH_LABEL = "soveren.credential_broker_spec_hash"
CREDENTIAL_BROKER_POLICY_VERSION = "2"
CREDENTIAL_BROKER_REGISTRY_VERSION = 1
MAX_CREDENTIAL_BINDINGS = 256
MAX_CREDENTIAL_REGISTRY_BYTES = 1024 * 1024

_BINDING_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_NETWORK_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


@dataclass(frozen=True, slots=True)
class DockerCredentialBrokerSpec:
    image: str
    container_name_prefix: str = "soveren-credential-broker"
    network_alias: str = "soveren-credential-broker"
    port: int = 8080
    memory: str = "128m"
    cpus: str = "0.25"
    pids_limit: int = 64

    def __post_init__(self) -> None:
        if not self.image.strip():
            raise ValueError("Docker credential broker image must not be empty")
        if not self.container_name_prefix.strip() or not self.network_alias.strip():
            raise ValueError("Docker credential broker names must not be empty")
        if not _NETWORK_ALIAS_RE.fullmatch(self.network_alias):
            raise ValueError("Docker credential broker network alias is invalid")
        if self.port != 8080:
            raise ValueError("the bundled Docker credential broker listens on port 8080")
        if not self.memory.strip() or not self.cpus.strip():
            raise ValueError("Docker credential broker resource limits must not be empty")
        if self.pids_limit < 1:
            raise ValueError("Docker credential broker pids_limit must be positive")


class _DockerEgressConfig(Protocol):
    @property
    def container_name(self) -> str: ...

    @property
    def internal_network(self) -> str: ...


class _DockerCredentialBrokerHost(Protocol):
    docker_command: tuple[str, ...]

    @property
    def egress(self) -> _DockerEgressConfig | None: ...

    async def _run_docker(
        self,
        args: list[str],
        *,
        input_data: bytes | None = None,
    ) -> CommandResult: ...

    async def _run_checked(self, args: list[str]) -> CommandResult: ...

    async def _is_running(self, container_id: str) -> bool: ...

    async def _inspect_label(self, container_id: str, label: str) -> str | None: ...

    async def _tenant_network_subnet(self, internal_network: str) -> str: ...

    async def _ensure_iptables_rule(self, rule: list[str], *, force_first: bool = False) -> bool: ...

    async def _remove_iptables_rule(self, rule: list[str]) -> None: ...

    def _raise_command_error(self, result: CommandResult) -> None: ...

    def _is_missing_container_result(self, result: CommandResult) -> bool: ...


@dataclass(frozen=True, slots=True)
class _DockerBrokerNetworkPolicy:
    network: str
    source: str
    destination: str
    port: int

    @property
    def broker_ip(self) -> str:
        return self.destination.removesuffix("/32")

    def response_rule(self) -> list[str]:
        return [
            "DOCKER-USER",
            "-s",
            self.destination,
            "-d",
            self.source,
            "-p",
            "tcp",
            "--sport",
            str(self.port),
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ]

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
            str(self.port),
            "-j",
            "ACCEPT",
        ]


@dataclass(frozen=True, slots=True)
class _ManagedBinding:
    binding_id: str
    kind: Literal["http", "openai_responses"]
    secret: bytes = field(repr=False)
    scope: CredentialBindingScope
    conversation_key: str | None
    networks: frozenset[str]
    usage_policy: CredentialUsagePolicy
    capability: str | None
    http: HttpCredentialBinding | None
    allowed_models: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DockerCredentialBrokerNetworkPreparation:
    """State needed to compensate one broker-network preparation exactly."""

    tenant_key: str
    network: str
    previous_bindings: dict[str, _ManagedBinding] | None = field(repr=False)


class DockerCredentialBrokerManager:
    """Own one memory-only, multi-binding credential broker per active tenant."""

    def __init__(
        self,
        *,
        host: _DockerCredentialBrokerHost,
        spec: DockerCredentialBrokerSpec,
    ) -> None:
        if host.egress is None:
            raise ValueError("Docker credential broker requires managed egress")
        self.host = host
        self.spec = spec
        self._locks: dict[str, asyncio.Lock] = {}
        self._bindings: dict[str, dict[str, _ManagedBinding]] = {}
        self._container_ids: dict[str, str] = {}
        self._network_ips: dict[str, str] = {}

    async def provision(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        conversation_key: str,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        network = self._validate_handle(
            handle,
            tenant_key=tenant_key,
            conversation_key=conversation_key,
        )
        _validate_credential(api_key)
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            tenant_networks = await self._verified_tenant_networks(tenant_key, required=network)
            binding = _ManagedBinding(
                binding_id=_openai_binding_id(tenant_key),
                kind="openai_responses",
                secret=bytes(api_key),
                scope="tenant",
                conversation_key=None,
                networks=frozenset(tenant_networks),
                usage_policy=policy,
                capability=None,
                http=None,
                allowed_models=policy.allowed_models,
            )
            network_policy = await self._provision_binding_locked(
                tenant_key=tenant_key,
                required_network=network,
                tenant_networks=tenant_networks,
                binding=binding,
            )
            return CredentialBrokerEndpoint(
                base_url=f"http://{self.spec.network_alias}:{self.spec.port}/v1",
                network_ip=network_policy.broker_ip,
            )

    async def provision_http(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        conversation_key: str,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        network = self._validate_handle(
            handle,
            tenant_key=tenant_key,
            conversation_key=conversation_key,
        )
        _validate_credential(credential)
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            tenant_networks = await self._verified_tenant_networks(tenant_key, required=network)
            binding_id = _http_binding_id(
                tenant_key,
                conversation_key=conversation_key,
                name=binding.name,
                scope=binding.scope,
            )
            existing = self._bindings.get(tenant_key, {}).get(binding_id)
            capability = (
                existing.capability
                if existing is not None and existing.kind == "http" and existing.capability is not None
                else secrets.token_urlsafe(32)
            )
            networks = tenant_networks if binding.scope == "tenant" else {network}
            managed = _ManagedBinding(
                binding_id=binding_id,
                kind="http",
                secret=bytes(credential),
                scope=binding.scope,
                conversation_key=conversation_key if binding.scope == "conversation" else None,
                networks=frozenset(networks),
                usage_policy=binding.usage_policy,
                capability=capability,
                http=binding,
                allowed_models=(),
            )
            network_policy = await self._provision_binding_locked(
                tenant_key=tenant_key,
                required_network=network,
                tenant_networks=tenant_networks,
                binding=managed,
            )
            return CredentialBrokerCapability(
                base_url=(f"http://{self.spec.network_alias}:{self.spec.port}/bindings/{capability}"),
                network_ip=network_policy.broker_ip,
            )

    async def revoke_http(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        conversation_key: str,
        name: str,
        scope: CredentialBindingScope,
    ) -> None:
        if scope not in {"conversation", "tenant"}:
            raise ValueError("HTTP credential binding scope must be 'conversation' or 'tenant'")
        self._validate_handle(
            handle,
            tenant_key=tenant_key,
            conversation_key=conversation_key,
        )
        _validate_binding_name(name)
        binding_id = _http_binding_id(
            tenant_key,
            conversation_key=conversation_key,
            name=name,
            scope=scope,
        )
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            current = self._bindings.get(tenant_key)
            if current is None or binding_id not in current:
                await self._apply_revocation_locked(tenant_key, current or {})
                return
            desired = dict(current)
            del desired[binding_id]
            self._set_bindings(tenant_key, desired)
            await self._apply_revocation_locked(tenant_key, desired)

    async def remove_unowned(self, tenant_key: str) -> None:
        """Discard a broker whose memory registry belongs to another control-plane process."""
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            await self._remove_unowned_container_locked(tenant_key)

    async def prepare_tenant_network(
        self,
        tenant_key: str,
        network: str,
    ) -> DockerCredentialBrokerNetworkPreparation:
        """Restore active bindings and extend tenant grants before a sandbox starts."""
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            current = self._bindings.get(tenant_key)
            preparation = DockerCredentialBrokerNetworkPreparation(
                tenant_key=tenant_key,
                network=network,
                previous_bindings=dict(current) if current is not None else None,
            )
            if not current:
                await self._remove_unowned_container_locked(tenant_key)
                return preparation

            tenant_networks = await self._verified_tenant_networks(tenant_key, required=network)
            desired = _refresh_binding_networks(current, tenant_networks)
            authorized_networks = _binding_networks(desired)
            _validate_registry_size(desired)
            self._set_bindings(tenant_key, desired)
            container_id: str | None = None
            try:
                existing = await self._find_container_id(tenant_key)
                if not authorized_networks:
                    if existing is not None:
                        await self._remove_broker_container(tenant_key, existing)
                    return preparation

                tracked_before = self._container_ids.get(tenant_key)
                if _binding_networks(current) - authorized_networks and existing is not None:
                    await self._remove_broker_container(tenant_key, existing)
                    existing = None

                container_id = await self._ensure_container(
                    tenant_key=tenant_key,
                    initial_network=min(authorized_networks),
                )
                registry_changed = desired != current or container_id != tracked_before
                for authorized_network in sorted(authorized_networks):
                    previous_ip = self._network_ips.get(authorized_network)
                    policy = await self._ensure_network_access(
                        container_id,
                        internal_network=authorized_network,
                    )
                    if policy.broker_ip != previous_ip:
                        registry_changed = True
                if registry_changed:
                    await self._sync_registry(container_id, desired)
                return preparation
            except BaseException as setup_error:
                self._bindings[tenant_key] = current
                cleanup_error: BaseException | None = None
                if container_id is not None:
                    try:
                        await self._remove_broker_container(tenant_key, container_id)
                    except BaseException as exc:
                        cleanup_error = exc
                if cleanup_error is not None:
                    raise BaseExceptionGroup(
                        "credential broker restore and fail-closed cleanup failed",
                        [setup_error, cleanup_error],
                    ) from setup_error
                raise

    async def _remove_unowned_container_locked(self, tenant_key: str) -> None:
        container_id = await self._find_container_id(tenant_key)
        if container_id is not None and self._container_ids.get(tenant_key) != container_id:
            await self._remove_broker_container(tenant_key, container_id)

    async def _provision_binding_locked(
        self,
        *,
        tenant_key: str,
        required_network: str,
        tenant_networks: set[str],
        binding: _ManagedBinding,
    ) -> _DockerBrokerNetworkPolicy:
        previous = self._bindings.get(tenant_key)
        desired = _refresh_binding_networks(previous or {}, tenant_networks)
        desired[binding.binding_id] = binding
        if len(desired) > MAX_CREDENTIAL_BINDINGS:
            raise RuntimeError("tenant credential binding limit exceeded")
        _validate_registry_size(desired)
        self._set_bindings(tenant_key, desired)
        container_id: str | None = None
        try:
            container_id = await self._ensure_container(
                tenant_key=tenant_key,
                initial_network=required_network,
            )
            policies: dict[str, _DockerBrokerNetworkPolicy] = {}
            for network in sorted(_binding_networks(desired)):
                policies[network] = await self._ensure_network_access(
                    container_id,
                    internal_network=network,
                )
            await self._sync_registry(container_id, desired)
            try:
                return policies[required_network]
            except KeyError as exc:
                raise RuntimeError("credential broker was not attached to the conversation network") from exc
        except BaseException as setup_error:
            if previous is None:
                self._bindings.pop(tenant_key, None)
            else:
                self._bindings[tenant_key] = previous
            cleanup_error: BaseException | None = None
            if container_id is not None:
                try:
                    await self._remove_broker_container(tenant_key, container_id)
                except BaseException as exc:
                    cleanup_error = exc
            if cleanup_error is not None:
                raise BaseExceptionGroup(
                    "credential broker provisioning and fail-closed cleanup failed",
                    [setup_error, cleanup_error],
                ) from setup_error
            raise

    async def _apply_revocation_locked(
        self,
        tenant_key: str,
        desired: dict[str, _ManagedBinding],
    ) -> None:
        container_id = await self._find_container_id(tenant_key)
        if container_id is None:
            await self._remove_known_network_rules_locked(tenant_key)
            self._container_ids.pop(tenant_key, None)
            return
        if self._container_ids.get(tenant_key) != container_id or not desired:
            await self._remove_broker_container(tenant_key, container_id)
            return
        try:
            await self._sync_registry(container_id, desired)
        except BaseException as update_error:
            try:
                await self._remove_broker_container(tenant_key, container_id)
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "credential revocation and fail-closed cleanup failed",
                    [update_error, cleanup_error],
                ) from update_error
            if isinstance(update_error, asyncio.CancelledError):
                raise update_error

    async def cleanup_network(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        network: str,
        network_subnet: str,
    ) -> None:
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            desired = _bindings_without_network(
                self._bindings.get(tenant_key, {}),
                network,
            )
            self._set_bindings(tenant_key, desired)
            await self._deauthorize_network_locked(
                tenant_key=tenant_key,
                network=network,
                network_subnet=network_subnet,
                retained_broker_ip=handle.metadata.get("credential_broker_ip"),
                desired=desired,
            )

    async def rollback_prepared_network(
        self,
        *,
        preparation: DockerCredentialBrokerNetworkPreparation,
        network_subnet: str,
    ) -> None:
        """Undo one prepare without discarding the retained credential registry."""
        tenant_key = preparation.tenant_key
        network = preparation.network
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            previous = preparation.previous_bindings
            serving = _bindings_without_network(previous or {}, network)
            try:
                await self._deauthorize_network_locked(
                    tenant_key=tenant_key,
                    network=network,
                    network_subnet=network_subnet,
                    retained_broker_ip=None,
                    desired=serving,
                )
            finally:
                if previous is None:
                    self._bindings.pop(tenant_key, None)
                else:
                    self._bindings[tenant_key] = previous

    async def _deauthorize_network_locked(
        self,
        *,
        tenant_key: str,
        network: str,
        network_subnet: str,
        retained_broker_ip: str | None,
        desired: dict[str, _ManagedBinding],
    ) -> None:
        candidates: list[_DockerBrokerNetworkPolicy] = []
        for value in (
            retained_broker_ip,
            self._network_ips.get(network),
        ):
            if not value:
                continue
            candidate = _DockerBrokerNetworkPolicy(
                network=network,
                source=str(ipaddress.ip_network(network_subnet, strict=False)),
                destination=f"{ipaddress.ip_address(value)}/32",
                port=self.spec.port,
            )
            if all(existing.destination != candidate.destination for existing in candidates):
                candidates.append(candidate)

        container_id = await self._find_container_id(tenant_key)
        cancelled_update: asyncio.CancelledError | None = None
        if container_id is not None:
            attached_networks = await self._container_networks(container_id)
            if network in attached_networks:
                inspected = await self._inspect_network_policy(
                    container_id,
                    internal_network=network,
                    network_subnet=network_subnet,
                )
                if all(existing.destination != inspected.destination for existing in candidates):
                    candidates.append(inspected)

            if self._container_ids.get(tenant_key) != container_id or not desired:
                await self._remove_broker_container(tenant_key, container_id)
                container_id = None
            else:
                try:
                    await self._sync_registry(container_id, desired)
                except BaseException as update_error:
                    try:
                        await self._remove_broker_container(tenant_key, container_id)
                    except BaseException as cleanup_error:
                        raise BaseExceptionGroup(
                            "credential network revocation and fail-closed cleanup failed",
                            [update_error, cleanup_error],
                        ) from update_error
                    if isinstance(update_error, asyncio.CancelledError):
                        cancelled_update = update_error
                    container_id = None

        for candidate in candidates:
            await self.host._remove_iptables_rule(candidate.response_rule())
            await self.host._remove_iptables_rule(candidate.allow_rule())
        if container_id is not None:
            await self._disconnect_network(container_id, network)
        self._network_ips.pop(network, None)
        if cancelled_update is not None:
            raise cancelled_update

    async def remove_unused(self, tenant_key: str) -> None:
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            existing_networks = set(await self._tenant_conversation_networks(tenant_key))
            current = self._bindings.get(tenant_key, {})
            desired = _refresh_binding_networks(current, existing_networks)
            self._set_bindings(tenant_key, desired)
            if desired:
                return
            container_id = await self._find_container_id(tenant_key)
            if container_id is not None:
                await self._remove_broker_container(tenant_key, container_id)

    async def remove_inactive(self, tenant_key: str) -> None:
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            running = await self.host._run_checked(
                [
                    *self.host.docker_command,
                    "ps",
                    "-q",
                    "--filter",
                    f"label={MANAGED_LABEL}=true",
                    "--filter",
                    f"label={RUNTIME_LABEL}=docker",
                    "--filter",
                    f"label={TENANT_KEY_LABEL}={tenant_key}",
                ]
            )
            if running.stdout.strip():
                return
            container_id = await self._find_container_id(tenant_key)
            if container_id is not None:
                await self._remove_broker_container(tenant_key, container_id)

    def _validate_handle(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        conversation_key: str,
    ) -> str:
        egress = self._require_egress()
        network = handle.metadata.get("network", "")
        if (
            handle.metadata.get("runtime") != "docker"
            or handle.metadata.get("tenant_key") != tenant_key
            or handle.metadata.get("conversation_key") != conversation_key
            or not network.startswith(f"{egress.internal_network}-")
        ):
            raise ValueError("credential broker requires a managed conversation sandbox handle")
        return network

    async def _verified_tenant_networks(self, tenant_key: str, *, required: str) -> set[str]:
        networks = set(await self._tenant_conversation_networks(tenant_key))
        if required not in networks:
            raise RuntimeError("managed conversation network ownership changed during broker setup")
        return networks

    async def _ensure_container(self, *, tenant_key: str, initial_network: str) -> str:
        spec_hash = _spec_hash(self.spec)
        container_id = await self._find_container_id(tenant_key)
        tracked = self._container_ids.get(tenant_key)
        if container_id is not None and tracked != container_id:
            await self._remove_broker_container(tenant_key, container_id)
            container_id = None
        if container_id is not None:
            recreate = await self.host._inspect_label(
                container_id, CREDENTIAL_BROKER_SPEC_HASH_LABEL
            ) != spec_hash or not await self.host._is_running(container_id)
            if recreate:
                await self._remove_broker_container(tenant_key, container_id)
                container_id = None
        if container_id is not None:
            try:
                await self._wait_for_health(container_id)
            except RuntimeError:
                await self._remove_broker_container(tenant_key, container_id)
                container_id = None
        if container_id is None:
            container_id = await self._create_container(
                tenant_key=tenant_key,
                spec_hash=spec_hash,
                initial_network=initial_network,
            )
            self._container_ids[tenant_key] = container_id
            try:
                await self._wait_for_health(container_id)
            except BaseException:
                await self._remove_container(container_id)
                self._container_ids.pop(tenant_key, None)
                raise
        return container_id

    async def _find_container_id(self, tenant_key: str) -> str | None:
        result = await self.host._run_checked(
            [
                *self.host.docker_command,
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={CREDENTIAL_BROKER_LABEL}=true",
                "--filter",
                f"label={TENANT_KEY_LABEL}={tenant_key}",
            ]
        )
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if len(ids) > 1:
            raise RuntimeError("multiple managed credential brokers exist for one tenant")
        return ids[0] if ids else None

    async def _create_container(
        self,
        *,
        tenant_key: str,
        spec_hash: str,
        initial_network: str,
    ) -> str:
        egress = self._require_egress()
        name = f"{_safe_name_component(self.spec.container_name_prefix)}-{tenant_key[:12]}"
        args = [
            *self.host.docker_command,
            "run",
            "-d",
            "--name",
            name,
            "--label",
            f"{MANAGED_LABEL}=true",
            "--label",
            f"{CREDENTIAL_BROKER_LABEL}=true",
            "--label",
            f"{CREDENTIAL_BROKER_POLICY_LABEL}={CREDENTIAL_BROKER_POLICY_VERSION}",
            "--label",
            f"{CREDENTIAL_BROKER_SPEC_HASH_LABEL}={spec_hash}",
            "--label",
            f"{TENANT_KEY_LABEL}={tenant_key}",
            "--memory",
            self.spec.memory,
            "--cpus",
            self.spec.cpus,
            "--pids-limit",
            str(self.spec.pids_limit),
            "--read-only",
            "--tmpfs",
            "/run/soveren:rw,nosuid,nodev,noexec,size=1m,mode=0700,uid=10001,gid=10001",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=8m,mode=0700,uid=10001,gid=10001",
            "--user",
            "10001:10001",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--init",
            "--network",
            initial_network,
            "--network-alias",
            self.spec.network_alias,
            "-e",
            f"SOVEREN_BROKER_EGRESS_PROXY=http://{egress.container_name}:3128",
            self.spec.image,
        ]
        result = await self.host._run_docker(args)
        if result.returncode != 0:
            self.host._raise_command_error(result)
        return result.stdout.strip().splitlines()[-1] if result.stdout.strip() else name

    async def _sync_registry(
        self,
        container_id: str,
        bindings: dict[str, _ManagedBinding],
    ) -> None:
        payload = _registry_payload(bindings, self._network_ips)
        result = await self.host._run_docker(
            [
                *self.host.docker_command,
                "exec",
                "-i",
                container_id,
                "python",
                "/opt/soveren/credential_broker.py",
                "admin",
            ],
            input_data=payload,
        )
        if result.returncode != 0:
            self.host._raise_command_error(result)

    async def _remove_container(self, container_id: str) -> None:
        result = await self.host._run_docker([*self.host.docker_command, "rm", "-f", container_id])
        if result.returncode != 0 and not self.host._is_missing_container_result(result):
            self.host._raise_command_error(result)

    async def _remove_broker_container(self, tenant_key: str, container_id: str) -> None:
        await self._decommission(container_id)
        if self._container_ids.get(tenant_key) == container_id:
            self._container_ids.pop(tenant_key, None)

    async def _decommission(self, container_id: str) -> None:
        egress = self._require_egress()
        policies: list[_DockerBrokerNetworkPolicy] = []
        discovery_error: BaseException | None = None
        try:
            networks = await self._container_networks(container_id)
            for network in sorted(networks):
                if not network.startswith(f"{egress.internal_network}-"):
                    continue
                subnet = await self.host._tenant_network_subnet(network)
                broker_ip = self._network_ips.get(network)
                if broker_ip is None:
                    policy = await self._inspect_network_policy(
                        container_id,
                        internal_network=network,
                        network_subnet=subnet,
                    )
                else:
                    policy = _DockerBrokerNetworkPolicy(
                        network=network,
                        source=str(ipaddress.ip_network(subnet, strict=False)),
                        destination=f"{ipaddress.ip_address(broker_ip)}/32",
                        port=self.spec.port,
                    )
                policies.append(policy)
        except BaseException as exc:
            discovery_error = exc

        try:
            await self._remove_container(container_id)
        except BaseException as removal_error:
            if discovery_error is not None:
                raise BaseExceptionGroup(
                    "credential broker policy discovery and fail-closed removal failed",
                    [discovery_error, removal_error],
                ) from discovery_error
            raise
        if discovery_error is not None:
            raise discovery_error

        for policy in policies:
            await self.host._remove_iptables_rule(policy.response_rule())
            await self.host._remove_iptables_rule(policy.allow_rule())
            self._network_ips.pop(policy.network, None)

    async def _remove_known_network_rules_locked(self, tenant_key: str) -> None:
        tenant_networks = await self._tenant_conversation_networks(tenant_key)
        for network in tenant_networks:
            broker_ip = self._network_ips.get(network)
            if broker_ip is None:
                continue
            subnet = await self.host._tenant_network_subnet(network)
            policy = _DockerBrokerNetworkPolicy(
                network=network,
                source=str(ipaddress.ip_network(subnet, strict=False)),
                destination=f"{ipaddress.ip_address(broker_ip)}/32",
                port=self.spec.port,
            )
            await self.host._remove_iptables_rule(policy.response_rule())
            await self.host._remove_iptables_rule(policy.allow_rule())
            self._network_ips.pop(network, None)

    async def _tenant_conversation_networks(self, tenant_key: str) -> list[str]:
        egress = self._require_egress()
        result = await self.host._run_checked(
            [
                *self.host.docker_command,
                "network",
                "ls",
                "--format",
                "{{.Name}}",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={TENANT_KEY_LABEL}={tenant_key}",
                "--filter",
                f"label={CONVERSATION_KEY_LABEL}",
            ]
        )
        prefix = f"{egress.internal_network}-"
        return sorted({line.strip() for line in result.stdout.splitlines() if line.strip().startswith(prefix)})

    async def _ensure_network_access(
        self,
        container_id: str,
        *,
        internal_network: str,
    ) -> _DockerBrokerNetworkPolicy:
        connected = await self._connect_network(container_id, internal_network=internal_network)
        created_rules: list[list[str]] = []
        try:
            network_subnet = await self.host._tenant_network_subnet(internal_network)
            policy = await self._inspect_network_policy(
                container_id,
                internal_network=internal_network,
                network_subnet=network_subnet,
            )
            for rule in (policy.allow_rule(), policy.response_rule()):
                if await self.host._ensure_iptables_rule(rule, force_first=True):
                    created_rules.append(rule)
            self._network_ips[internal_network] = policy.broker_ip
            return policy
        except BaseException as setup_error:
            cleanup_errors: list[BaseException] = []
            for rule in reversed(created_rules):
                try:
                    await self.host._remove_iptables_rule(rule)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if connected:
                try:
                    await self._disconnect_network(container_id, internal_network)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if cleanup_errors:
                raise BaseExceptionGroup(
                    "credential broker network setup and cleanup failed",
                    [setup_error, *cleanup_errors],
                ) from setup_error
            raise

    async def _connect_network(self, container_id: str, *, internal_network: str) -> bool:
        networks = await self._container_networks(container_id)
        if internal_network in networks:
            return False
        connected = await self.host._run_docker(
            [
                *self.host.docker_command,
                "network",
                "connect",
                "--alias",
                self.spec.network_alias,
                internal_network,
                container_id,
            ]
        )
        if connected.returncode == 0:
            return True
        raced_networks = await self._container_networks(container_id)
        if internal_network not in raced_networks:
            self.host._raise_command_error(connected)
        return False

    async def _inspect_network_policy(
        self,
        container_id: str,
        *,
        internal_network: str,
        network_subnet: str,
    ) -> _DockerBrokerNetworkPolicy:
        subnet = ipaddress.ip_network(network_subnet, strict=False)
        result = await self.host._run_checked(
            [
                *self.host.docker_command,
                "inspect",
                "-f",
                f"{{{{with index .NetworkSettings.Networks {json.dumps(internal_network)}}}}}"
                "{{.IPAddress}}{{end}}",
                container_id,
            ]
        )
        try:
            broker_ip = ipaddress.ip_address(result.stdout.strip())
        except ValueError as exc:
            raise RuntimeError("Docker returned invalid credential broker network metadata") from exc
        if broker_ip.version != 4 or broker_ip not in subnet:
            raise RuntimeError("credential broker is not using an address inside the conversation network")
        return _DockerBrokerNetworkPolicy(
            network=internal_network,
            source=str(subnet),
            destination=f"{broker_ip}/32",
            port=self.spec.port,
        )

    async def _disconnect_network(self, container_id: str, network: str) -> None:
        result = await self.host._run_docker(
            [
                *self.host.docker_command,
                "network",
                "disconnect",
                "-f",
                network,
                container_id,
            ]
        )
        if result.returncode != 0 and not self.host._is_missing_container_result(result):
            detail = (result.stderr + result.stdout).lower()
            if "not connected" not in detail and "network not found" not in detail:
                self.host._raise_command_error(result)

    async def _container_networks(self, container_id: str) -> dict[str, object]:
        result = await self.host._run_checked(
            [
                *self.host.docker_command,
                "inspect",
                "-f",
                "{{json .NetworkSettings.Networks}}",
                container_id,
            ]
        )
        try:
            networks = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid credential broker network metadata") from exc
        if not isinstance(networks, dict):
            raise RuntimeError("Docker returned invalid credential broker network metadata")
        return networks

    async def _wait_for_health(self, container_id: str) -> None:
        for _ in range(30):
            result = await self.host._run_checked(
                [
                    *self.host.docker_command,
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
                raise RuntimeError(f"managed credential broker is {status}")
            await asyncio.sleep(1.0)
        raise RuntimeError("managed credential broker did not become healthy")

    def _set_bindings(self, tenant_key: str, bindings: dict[str, _ManagedBinding]) -> None:
        if bindings:
            self._bindings[tenant_key] = bindings
        else:
            self._bindings.pop(tenant_key, None)

    def _require_egress(self) -> _DockerEgressConfig:
        egress = self.host.egress
        if egress is None:
            raise RuntimeError("Docker credential broker requires managed egress")
        return egress


def _refresh_binding_networks(
    bindings: dict[str, _ManagedBinding],
    tenant_networks: set[str],
) -> dict[str, _ManagedBinding]:
    refreshed: dict[str, _ManagedBinding] = {}
    for binding_id, binding in bindings.items():
        networks = tenant_networks if binding.scope == "tenant" else binding.networks & tenant_networks
        if networks:
            refreshed[binding_id] = replace(binding, networks=frozenset(networks))
    return refreshed


def _bindings_without_network(
    bindings: dict[str, _ManagedBinding],
    network: str,
) -> dict[str, _ManagedBinding]:
    retained: dict[str, _ManagedBinding] = {}
    for binding_id, binding in bindings.items():
        if binding.scope == "conversation" and network in binding.networks:
            continue
        networks = binding.networks - {network}
        if networks:
            retained[binding_id] = replace(binding, networks=frozenset(networks))
    return retained


def _binding_networks(bindings: dict[str, _ManagedBinding]) -> set[str]:
    return {network for binding in bindings.values() for network in binding.networks}


def _registry_payload(
    bindings: dict[str, _ManagedBinding],
    network_ips: dict[str, str],
) -> bytes:
    serialized: list[dict[str, object]] = []
    for binding in sorted(bindings.values(), key=lambda item: item.binding_id):
        try:
            allowed_local_ips = sorted({network_ips[network] for network in binding.networks})
        except KeyError as exc:
            raise RuntimeError("credential broker network address is unavailable") from exc
        value: dict[str, object] = {
            "binding_id": binding.binding_id,
            "kind": binding.kind,
            "secret": base64.b64encode(binding.secret).decode("ascii"),
            "allowed_local_ips": allowed_local_ips,
            "limits": _usage_policy_payload(binding.usage_policy),
        }
        if binding.kind == "openai_responses":
            value["allowed_models"] = list(binding.allowed_models)
        else:
            assert binding.http is not None and binding.capability is not None
            value.update(
                {
                    "capability": binding.capability,
                    "target_origin": binding.http.target_origin,
                    "credential_header": binding.http.credential_header,
                    "credential_prefix": binding.http.credential_prefix,
                    "allowed_methods": list(binding.http.allowed_methods),
                    "allowed_path_prefixes": list(binding.http.allowed_path_prefixes),
                    "allowed_request_headers": list(binding.http.allowed_request_headers),
                }
            )
        serialized.append(value)
    payload = json.dumps(
        {"version": CREDENTIAL_BROKER_REGISTRY_VERSION, "bindings": serialized},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if len(payload) > MAX_CREDENTIAL_REGISTRY_BYTES:
        raise RuntimeError("tenant credential registry exceeds the 1 MiB broker update limit")
    return payload


def _validate_registry_size(bindings: dict[str, _ManagedBinding]) -> None:
    placeholder_ips = {network: "255.255.255.255" for network in _binding_networks(bindings)}
    _registry_payload(bindings, placeholder_ips)


def _usage_policy_payload(policy: CredentialUsagePolicy) -> dict[str, int | float]:
    return {
        "max_concurrent_requests": policy.max_concurrent_requests,
        "requests_per_minute": policy.requests_per_minute,
        "max_request_bytes": policy.max_request_bytes,
        "queue_timeout_s": policy.queue_timeout_s,
        "request_read_timeout_s": policy.request_read_timeout_s,
    }


def _spec_hash(spec: DockerCredentialBrokerSpec) -> str:
    payload = {
        "policy_version": CREDENTIAL_BROKER_POLICY_VERSION,
        "registry_version": CREDENTIAL_BROKER_REGISTRY_VERSION,
        "image": spec.image,
        "container_name_prefix": spec.container_name_prefix,
        "network_alias": spec.network_alias,
        "port": spec.port,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _openai_binding_id(tenant_key: str) -> str:
    return hashlib.sha256(f"{tenant_key}\0openai_responses".encode()).hexdigest()


def _http_binding_id(
    tenant_key: str,
    *,
    conversation_key: str,
    name: str,
    scope: CredentialBindingScope,
) -> str:
    _validate_binding_name(name)
    if scope not in {"conversation", "tenant"}:
        raise ValueError("HTTP credential binding scope must be 'conversation' or 'tenant'")
    owner = tenant_key if scope == "tenant" else conversation_key
    return hashlib.sha256(f"{tenant_key}\0http\0{scope}\0{owner}\0{name}".encode()).hexdigest()


def _validate_binding_name(name: str) -> None:
    if not _BINDING_NAME_RE.fullmatch(name):
        raise ValueError("HTTP credential binding name is invalid")


def _validate_credential(credential: bytes) -> None:
    if not credential or len(credential) > 16 * 1024:
        raise ValueError("credential must be non-empty and at most 16 KiB")
    try:
        decoded = credential.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("credential must be ASCII") from exc
    if decoded != decoded.strip() or any(ord(character) < 33 or ord(character) > 126 for character in decoded):
        raise ValueError("credential contains invalid characters")


def _safe_name_component(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-")
    if not safe:
        raise ValueError("Docker credential broker name component must not be empty")
    return safe[:64]
