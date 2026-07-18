"""Credential provisioning for sandboxed Codex runtimes."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

from soveren_agent_platform.sandbox import (
    CredentialBrokerPolicy,
    CredentialBrokerProvisioner,
    SandboxHandle,
    SandboxManager,
)

MAX_AUTH_FILE_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class CodexCredentialProvisioning:
    """Non-secret launch configuration produced by a credential provider."""

    config_overrides: tuple[str, ...] = ()
    launch_env: tuple[tuple[str, str], ...] = ()
    sandbox_metadata: tuple[tuple[str, str], ...] = ()


class CodexCredentialProvider(Protocol):
    async def provision(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        ...


@dataclass(frozen=True, slots=True)
class ExistingCodexCredentials:
    """Use credentials already persisted in the tenant CODEX_HOME."""

    async def provision(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        return CodexCredentialProvisioning()


@dataclass(frozen=True, slots=True)
class CodexApiKeyCredentials:
    """Route Codex through a tenant-isolated broker without exposing its API key."""

    api_key: str = field(repr=False)
    policy: CredentialBrokerPolicy = field(default_factory=CredentialBrokerPolicy)

    def __post_init__(self) -> None:
        normalized = self.api_key.strip()
        if not normalized:
            raise ValueError("Codex API key must not be empty")
        try:
            encoded = normalized.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("Codex API key must be ASCII") from exc
        if len(encoded) > 16 * 1024 or any(ord(character) < 33 or ord(character) > 126 for character in normalized):
            raise ValueError("Codex API key contains invalid characters")
        object.__setattr__(self, "api_key", normalized)

    async def provision(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        if not isinstance(manager, CredentialBrokerProvisioner):
            raise TypeError("Codex API-key credentials require a credential-broker sandbox manager")
        endpoint = await manager.provision_credential_broker(
            handle,
            api_key=self.api_key.encode("ascii"),
            policy=self.policy,
        )
        _validate_broker_base_url(endpoint.base_url)
        broker_host = urlsplit(endpoint.base_url).hostname
        assert broker_host is not None
        no_proxy = ",".join(dict.fromkeys((broker_host, endpoint.network_ip)))
        provider_id = "soveren_credential_broker"
        quoted_base_url = json.dumps(endpoint.base_url)
        return CodexCredentialProvisioning(
            config_overrides=(
                f"model_provider={json.dumps(provider_id)}",
                f"model_providers.{provider_id}.name={json.dumps('Soveren Credential Broker')}",
                f"model_providers.{provider_id}.base_url={quoted_base_url}",
                f"model_providers.{provider_id}.wire_api={json.dumps('responses')}",
                f"model_providers.{provider_id}.requires_openai_auth=false",
                f"model_providers.{provider_id}.supports_websockets=false",
            ),
            launch_env=(("NO_PROXY", no_proxy), ("no_proxy", no_proxy)),
            sandbox_metadata=(("credential_broker_ip", endpoint.network_ip),),
        )


@dataclass(frozen=True, slots=True)
class CodexAuthFileCredentials:
    """Provision a trusted local Codex auth cache into the tenant CODEX_HOME."""

    path: Path

    async def provision(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        data = await asyncio.to_thread(self.path.read_bytes)
        if not data or len(data) > MAX_AUTH_FILE_BYTES:
            raise ValueError("Codex auth file must be non-empty and at most 1 MiB")
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError("Codex auth file must contain valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Codex auth file must contain a JSON object")
        await manager.run_command(
            handle,
            ["sh", "-c", 'umask 077; test -s "$CODEX_HOME/auth.json" || cat > "$CODEX_HOME/auth.json"'],
            input_data=data,
            env={"CODEX_HOME": handle.codex_home},
            workdir=handle.workspace_root,
        )
        return CodexCredentialProvisioning()


def _validate_broker_base_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") != "/v1"
    ):
        raise ValueError("credential broker returned an invalid Codex base URL")
