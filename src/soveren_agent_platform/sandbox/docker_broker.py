"""Tenant credential-broker lifecycle for the bundled Docker sandbox manager."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
from dataclasses import dataclass
from typing import Protocol

from soveren_agent_platform.sandbox.contracts import (
    CredentialBrokerEndpoint,
    CredentialBrokerPolicy,
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
CREDENTIAL_BROKER_POLICY_VERSION = "1"


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
        if self.port < 1 or self.port > 65535:
            raise ValueError("Docker credential broker port is invalid")
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


class DockerCredentialBrokerManager:
    """Own one in-memory provider-key broker per active tenant."""

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
        self._credential_fingerprints: dict[str, bytes] = {}
        self._network_ips: dict[str, str] = {}
        self._authorized_networks: dict[str, set[str]] = {}

    async def provision(
        self,
        handle: SandboxHandle,
        *,
        tenant_key: str,
        conversation_key: str,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        egress = self._require_egress()
        network = handle.metadata.get("network", "")
        if (
            handle.metadata.get("runtime") != "docker"
            or handle.metadata.get("tenant_key") != tenant_key
            or handle.metadata.get("conversation_key") != conversation_key
            or not network.startswith(f"{egress.internal_network}-")
        ):
            raise ValueError("credential broker requires a managed conversation sandbox handle")
        _validate_api_key(api_key)
        fingerprint = hashlib.sha256(api_key).digest()
        spec_hash = _spec_hash(self.spec, policy)
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            tenant_networks = set(await self._tenant_conversation_networks(tenant_key))
            if network not in tenant_networks:
                raise RuntimeError("managed conversation network ownership changed during broker setup")
            authorized_networks = self._authorized_networks.setdefault(tenant_key, set())
            authorized_networks.intersection_update(tenant_networks)
            container_id = await self._find_container_id(tenant_key)
            recreate = self._credential_fingerprints.get(tenant_key) != fingerprint
            if container_id is not None and not recreate:
                recreate = await self.host._inspect_label(
                    container_id,
                    CREDENTIAL_BROKER_SPEC_HASH_LABEL,
                ) != spec_hash or not await self.host._is_running(container_id)
            if container_id is not None and recreate:
                await self._decommission(container_id)
                container_id = None
                self._credential_fingerprints.pop(tenant_key, None)
            if container_id is not None:
                try:
                    await self._wait_for_health(container_id)
                except RuntimeError:
                    await self._decommission(container_id)
                    container_id = None
                    self._credential_fingerprints.pop(tenant_key, None)
            if container_id is None:
                container_id = await self._create_container(
                    tenant_key=tenant_key,
                    policy=policy,
                    spec_hash=spec_hash,
                    initial_network=network,
                )
                try:
                    await self._stream_api_key(container_id, api_key)
                    await self._wait_for_health(container_id)
                except BaseException:
                    await self._remove_container(container_id)
                    raise
                self._credential_fingerprints[tenant_key] = fingerprint

            was_authorized = network in authorized_networks
            authorized_networks.add(network)
            current_policy: _DockerBrokerNetworkPolicy | None = None
            try:
                for tenant_network in sorted(authorized_networks):
                    broker_policy = await self._ensure_network_access(
                        container_id,
                        internal_network=tenant_network,
                    )
                    if tenant_network == network:
                        current_policy = broker_policy
            except BaseException:
                if not was_authorized:
                    authorized_networks.discard(network)
                raise
            if current_policy is None:
                raise RuntimeError("credential broker was not attached to the conversation network")
            return CredentialBrokerEndpoint(
                base_url=f"http://{self.spec.network_alias}:{self.spec.port}/v1",
                network_ip=current_policy.broker_ip,
            )

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
            candidates: list[_DockerBrokerNetworkPolicy] = []
            for value in (
                handle.metadata.get("credential_broker_ip"),
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
            if container_id is not None:
                networks = await self._container_networks(container_id)
                if network in networks:
                    current = await self._inspect_network_policy(
                        container_id,
                        internal_network=network,
                        network_subnet=network_subnet,
                    )
                    if all(existing.destination != current.destination for existing in candidates):
                        candidates.append(current)

            for candidate in candidates:
                await self.host._remove_iptables_rule(candidate.response_rule())
                await self.host._remove_iptables_rule(candidate.allow_rule())
            if container_id is not None:
                await self._disconnect_network(container_id, network)
            self._network_ips.pop(network, None)
            authorized_networks = self._authorized_networks.get(tenant_key)
            if authorized_networks is not None:
                authorized_networks.discard(network)

    async def remove_unused(self, tenant_key: str) -> None:
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            existing_networks = set(await self._tenant_conversation_networks(tenant_key))
            authorized_networks = self._authorized_networks.setdefault(tenant_key, set())
            authorized_networks.intersection_update(existing_networks)
            if authorized_networks:
                return
            container_id = await self._find_container_id(tenant_key)
            if container_id is not None:
                await self._remove_container(container_id)
            self._credential_fingerprints.pop(tenant_key, None)
            self._authorized_networks.pop(tenant_key, None)

    async def remove_inactive(self, tenant_key: str) -> None:
        lock = self._locks.setdefault(tenant_key, asyncio.Lock())
        async with lock:
            running = await self.host._run_checked([
                *self.host.docker_command,
                "ps",
                "-q",
                "--filter",
                f"label={MANAGED_LABEL}=true",
                "--filter",
                f"label={RUNTIME_LABEL}=docker",
                "--filter",
                f"label={TENANT_KEY_LABEL}={tenant_key}",
            ])
            if running.stdout.strip():
                return
            container_id = await self._find_container_id(tenant_key)
            if container_id is not None:
                await self._decommission(container_id)
            self._credential_fingerprints.pop(tenant_key, None)
            self._authorized_networks.pop(tenant_key, None)

    async def _find_container_id(self, tenant_key: str) -> str | None:
        result = await self.host._run_checked([
            *self.host.docker_command,
            "ps",
            "-aq",
            "--filter",
            f"label={MANAGED_LABEL}=true",
            "--filter",
            f"label={CREDENTIAL_BROKER_LABEL}=true",
            "--filter",
            f"label={TENANT_KEY_LABEL}={tenant_key}",
        ])
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if len(ids) > 1:
            raise RuntimeError("multiple managed credential brokers exist for one tenant")
        return ids[0] if ids else None

    async def _create_container(
        self,
        *,
        tenant_key: str,
        policy: CredentialBrokerPolicy,
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
            f"SOVEREN_BROKER_MAX_CONCURRENT={policy.max_concurrent_requests}",
            "-e",
            f"SOVEREN_BROKER_REQUESTS_PER_MINUTE={policy.requests_per_minute}",
            "-e",
            f"SOVEREN_BROKER_MAX_REQUEST_BYTES={policy.max_request_bytes}",
            "-e",
            f"SOVEREN_BROKER_QUEUE_TIMEOUT_S={policy.queue_timeout_s}",
            "-e",
            f"SOVEREN_BROKER_REQUEST_READ_TIMEOUT_S={policy.request_read_timeout_s}",
            "-e",
            f"SOVEREN_BROKER_ALLOWED_MODELS={json.dumps(policy.allowed_models, separators=(',', ':'))}",
            "-e",
            f"SOVEREN_BROKER_EGRESS_PROXY=http://{egress.container_name}:3128",
            self.spec.image,
        ]
        result = await self.host._run_docker(args)
        if result.returncode != 0:
            self.host._raise_command_error(result)
        return result.stdout.strip().splitlines()[-1] if result.stdout.strip() else name

    async def _stream_api_key(self, container_id: str, api_key: bytes) -> None:
        result = await self.host._run_docker(
            [
                *self.host.docker_command,
                "exec",
                "-i",
                container_id,
                "sh",
                "-c",
                "umask 077; cat > /run/soveren/openai-api-key.tmp; "
                "mv /run/soveren/openai-api-key.tmp /run/soveren/openai-api-key",
            ],
            input_data=api_key,
        )
        if result.returncode != 0:
            self.host._raise_command_error(result)

    async def _remove_container(self, container_id: str) -> None:
        result = await self.host._run_docker([*self.host.docker_command, "rm", "-f", container_id])
        if result.returncode != 0 and not self.host._is_missing_container_result(result):
            self.host._raise_command_error(result)

    async def _decommission(self, container_id: str) -> None:
        egress = self._require_egress()
        networks = await self._container_networks(container_id)
        for network in sorted(networks):
            if not network.startswith(f"{egress.internal_network}-"):
                continue
            subnet = await self.host._tenant_network_subnet(network)
            policy = await self._inspect_network_policy(
                container_id,
                internal_network=network,
                network_subnet=subnet,
            )
            await self.host._remove_iptables_rule(policy.response_rule())
            await self.host._remove_iptables_rule(policy.allow_rule())
            self._network_ips.pop(network, None)
        await self._remove_container(container_id)

    async def _tenant_conversation_networks(self, tenant_key: str) -> list[str]:
        egress = self._require_egress()
        result = await self.host._run_checked([
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
        ])
        prefix = f"{egress.internal_network}-"
        return sorted(
            {
                line.strip()
                for line in result.stdout.splitlines()
                if line.strip().startswith(prefix)
            }
        )

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
        connected = await self.host._run_docker([
            *self.host.docker_command,
            "network",
            "connect",
            "--alias",
            self.spec.network_alias,
            internal_network,
            container_id,
        ])
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
        result = await self.host._run_checked([
            *self.host.docker_command,
            "inspect",
            "-f",
            f"{{{{with index .NetworkSettings.Networks {json.dumps(internal_network)}}}}}"
            "{{.IPAddress}}{{end}}",
            container_id,
        ])
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
        result = await self.host._run_docker([
            *self.host.docker_command,
            "network",
            "disconnect",
            "-f",
            network,
            container_id,
        ])
        if result.returncode != 0 and not self.host._is_missing_container_result(result):
            detail = (result.stderr + result.stdout).lower()
            if "not connected" not in detail and "network not found" not in detail:
                self.host._raise_command_error(result)

    async def _container_networks(self, container_id: str) -> dict[str, object]:
        result = await self.host._run_checked([
            *self.host.docker_command,
            "inspect",
            "-f",
            "{{json .NetworkSettings.Networks}}",
            container_id,
        ])
        try:
            networks = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned invalid credential broker network metadata") from exc
        if not isinstance(networks, dict):
            raise RuntimeError("Docker returned invalid credential broker network metadata")
        return networks

    async def _wait_for_health(self, container_id: str) -> None:
        for _ in range(30):
            result = await self.host._run_checked([
                *self.host.docker_command,
                "inspect",
                "-f",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}",
                container_id,
            ])
            status = result.stdout.strip().lower()
            if status == "healthy":
                return
            if status in {"unhealthy", "missing"}:
                raise RuntimeError(f"managed credential broker is {status}")
            await asyncio.sleep(1.0)
        raise RuntimeError("managed credential broker did not become healthy")

    def _require_egress(self) -> _DockerEgressConfig:
        egress = self.host.egress
        if egress is None:
            raise RuntimeError("Docker credential broker requires managed egress")
        return egress


def _spec_hash(spec: DockerCredentialBrokerSpec, policy: CredentialBrokerPolicy) -> str:
    payload = {
        "policy_version": CREDENTIAL_BROKER_POLICY_VERSION,
        "image": spec.image,
        "container_name_prefix": spec.container_name_prefix,
        "network_alias": spec.network_alias,
        "port": spec.port,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
        "max_concurrent_requests": policy.max_concurrent_requests,
        "requests_per_minute": policy.requests_per_minute,
        "max_request_bytes": policy.max_request_bytes,
        "queue_timeout_s": policy.queue_timeout_s,
        "request_read_timeout_s": policy.request_read_timeout_s,
        "allowed_models": list(policy.allowed_models),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_api_key(api_key: bytes) -> None:
    if not api_key or len(api_key) > 16 * 1024:
        raise ValueError("credential broker API key must be non-empty and at most 16 KiB")
    try:
        decoded = api_key.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("credential broker API key must be ASCII") from exc
    if decoded != decoded.strip() or any(ord(character) < 33 or ord(character) > 126 for character in decoded):
        raise ValueError("credential broker API key contains invalid characters")


def _safe_name_component(value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-")
    if not safe:
        raise ValueError("Docker credential broker name component must not be empty")
    return safe[:64]
