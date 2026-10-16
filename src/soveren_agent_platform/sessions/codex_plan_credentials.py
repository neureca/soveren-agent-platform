"""ChatGPT-plan access through the existing protected HTTP credential broker."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlsplit

from soveren_agent_platform.runtime.failures import NonRetryableEventError
from soveren_agent_platform.runtime.latency import measure_codex_lock, measure_codex_runtime
from soveren_agent_platform.sandbox import (
    CredentialBrokerCapability,
    HttpCredentialBinding,
    HttpCredentialBrokerProvisioner,
    SandboxHandle,
    SandboxManager,
)
from soveren_agent_platform.sessions.backend import TenantBoundaryError
from soveren_agent_platform.sessions.codex_credentials import CodexClientInfo, CodexCredentialProvisioning


class CodexCredentialUnavailable(NonRetryableEventError):
    """The application must repair/re-authorize its connection before another turn."""

    def __init__(
        self,
        reason: Literal[
            "reauthentication_required", "access_token_expired_or_expiring", "invalid_access_token"
        ] = "reauthentication_required",
    ) -> None:
        if reason not in {"reauthentication_required", "access_token_expired_or_expiring", "invalid_access_token"}:
            raise ValueError("unknown Codex credential failure reason")
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class ChatGPTPlanAccessToken:
    """An app-verified plan-use grant; never include an ID token or refresh token."""

    access_token: str = field(repr=False)
    expires_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.access_token, str)
            or not self.access_token
            or len(self.access_token) > 64 * 1024
            or any(ord(c) < 33 or ord(c) > 126 for c in self.access_token)
            or not isinstance(self.expires_at, datetime)
            or self.expires_at.tzinfo is None
            or self.expires_at.utcoffset() is None
        ):
            raise ValueError("ChatGPT plan grant requires an ASCII bearer and an aware expiration datetime")


@dataclass(frozen=True, slots=True)
class CodexChatGPTPlanCredentials:
    """Resolve fresh access tokens from one app-owned, durable OAuth session.

    The application verifies identity/scope and owns serialized, cancellation-safe
    token rotation and persistence. This adapter never invokes OAuth or reads CLI
    auth caches. Each turn updates the stable conversation broker binding.
    """

    tenant_id: str
    get_access_token: Callable[[], Awaitable[ChatGPTPlanAccessToken]] = field(repr=False)
    client_info: CodexClientInfo
    minimum_validity_s: float = 180.0
    _binding_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.tenant_id.strip() or not callable(self.get_access_token):
            raise ValueError("ChatGPT plan credentials require a tenant and an async access-token provider")
        if not isinstance(self.client_info, CodexClientInfo):
            raise TypeError("client_info must be CodexClientInfo")
        if (
            isinstance(self.minimum_validity_s, bool)
            or not isinstance(self.minimum_validity_s, (int, float))
            or not math.isfinite(self.minimum_validity_s)
            or self.minimum_validity_s < 0
        ):
            raise ValueError("minimum_validity_s must be finite and non-negative")

    async def provision(self, manager: SandboxManager, handle: SandboxHandle) -> CodexCredentialProvisioning:
        endpoint = await self._bind(manager, handle)
        return self._configuration(endpoint)

    def _configuration(self, endpoint: CredentialBrokerCapability) -> CodexCredentialProvisioning:
        host = urlsplit(endpoint.base_url).hostname
        assert host is not None
        provider = "soveren_chatgpt_plan"
        no_proxy = ",".join(dict.fromkeys((host, endpoint.network_ip)))
        return CodexCredentialProvisioning(
            config_overrides=(
                f'model_provider="{provider}"',
                f'model_providers.{provider}.name="ChatGPT plan"',
                f"model_providers.{provider}.base_url={json.dumps(endpoint.base_url.rstrip('/') + '/v1')}",
                f'model_providers.{provider}.wire_api="responses"',
                f"model_providers.{provider}.requires_openai_auth=false",
                f"model_providers.{provider}.supports_websockets=false",
                "features.apps=false",
                "features.plugins=false",
                "features.image_generation=false",
                "features.multi_agent=false",
                "features.browser_use=false",
                'web_search="disabled"',
            ),
            launch_env=(("NO_PROXY", no_proxy), ("no_proxy", no_proxy)),
            sandbox_metadata=(("credential_broker_ip", endpoint.network_ip),),
            client_info=self.client_info,
        )

    async def refresh_for_turn(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        return self._configuration(await self._bind(manager, handle))

    async def _bind(self, manager: SandboxManager, handle: SandboxHandle) -> CredentialBrokerCapability:
        with measure_codex_runtime("siwc_bind"):
            if handle.tenant_id != self.tenant_id:
                raise TenantBoundaryError("ChatGPT plan credentials cannot authorize another tenant")
            if not isinstance(manager, HttpCredentialBrokerProvisioner):
                raise TypeError("ChatGPT plan credentials require a protected HTTP credential broker")
            async with measure_codex_lock(self._binding_lock, "siwc_binding_lock_wait"):
                try:
                    # Do not read/remove an old native credential or mix refresh domains.
                    with measure_codex_runtime("siwc_auth_file_check"):
                        await manager.run_command(
                            handle,
                            [
                                "sh",
                                "-c",
                                'if test -e "$CODEX_HOME/auth.json" || test -L "$CODEX_HOME/auth.json"; then '
                                'printf "%s\\n" "SIWC requires a CODEX_HOME without native auth.json" >&2; exit 1; fi',
                            ],
                            env={"CODEX_HOME": handle.codex_home},
                            workdir=handle.workspace_root,
                        )
                    with measure_codex_runtime("siwc_get_access_token"):
                        token = await self.get_access_token()
                    with measure_codex_runtime("siwc_grant_validation"):
                        if not isinstance(token, ChatGPTPlanAccessToken):
                            raise CodexCredentialUnavailable("invalid_access_token")
                        if (token.expires_at - datetime.now(UTC)).total_seconds() <= self.minimum_validity_s:
                            raise CodexCredentialUnavailable("access_token_expired_or_expiring")
                    with measure_codex_runtime("siwc_provision_http_credential"):
                        endpoint = await manager.provision_http_credential(
                            handle,
                            credential=token.access_token.encode("ascii"),
                            binding=HttpCredentialBinding(
                                name="soveren-chatgpt-plan",
                                target_origin="https://api.openai.com",
                                allowed_methods=("POST",),
                                allowed_path_prefixes=("/v1/responses",),
                                allowed_request_headers=("accept", "content-type", "user-agent", "originator"),
                            ),
                        )
                    with measure_codex_runtime("siwc_endpoint_validation"):
                        parsed = urlsplit(endpoint.base_url)
                        if (
                            parsed.scheme != "http"
                            or not parsed.hostname
                            or parsed.username is not None
                            or parsed.password is not None
                            or parsed.query
                            or parsed.fragment
                            or not parsed.path.startswith("/bindings/")
                            or parsed.path.count("/") != 2
                            or not parsed.path.removeprefix("/bindings/")
                        ):
                            raise ValueError("credential broker returned an invalid ChatGPT plan endpoint")
                    return endpoint
                except BaseException as auth_error:
                    # Even retryable/unknown verification failures must not leave the
                    # previous bearer usable. The broker's own bounded revocation must
                    # finish before cancellation can release this binding owner.
                    with measure_codex_runtime("siwc_revocation"):
                        revoke = asyncio.create_task(
                            manager.revoke_http_credential(handle, name="soveren-chatgpt-plan"),
                        )
                        while not revoke.done():
                            try:
                                await asyncio.shield(revoke)
                            except asyncio.CancelledError:
                                continue
                            except BaseException:
                                break
                        try:
                            revoke.result()
                        except BaseException as cleanup_error:
                            raise BaseExceptionGroup(
                                "ChatGPT plan denial and revocation failed",
                                [auth_error, cleanup_error],
                            ) from None
                    raise
