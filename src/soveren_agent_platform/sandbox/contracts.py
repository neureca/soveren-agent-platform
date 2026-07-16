"""Sandbox lifecycle contracts.

Sandboxes are execution-plane boundaries. They are not product tenants,
authorization policy, or app-owned business state.
"""

from __future__ import annotations

import ipaddress
import math
import re
from dataclasses import dataclass, field
from typing import Literal, Mapping, Protocol, runtime_checkable
from urllib.parse import urlsplit


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
    conversation_id: str
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
    conversation_id: str
    workspace_root: str
    codex_home: str
    metadata: Mapping[str, str] = field(default_factory=dict)


CredentialBindingScope = Literal["conversation", "tenant"]

_HTTP_METHODS = frozenset({"DELETE", "GET", "HEAD", "PATCH", "POST", "PUT"})
_MAX_CREDENTIAL_REQUEST_BYTES = 64 * 1024 * 1024
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_BINDING_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_FORBIDDEN_CREDENTIAL_HEADERS = frozenset(
    {
        "connection",
        "content-length",
        "cookie",
        "forwarded",
        "host",
        "keep-alive",
        "openai-organization",
        "openai-project",
        "proxy-authenticate",
        "proxy-authorization",
        "set-cookie",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-forwarded-uri",
        "x-http-method",
        "x-http-method-override",
        "x-method-override",
        "x-original-url",
        "x-rewrite-url",
    }
)
_FORBIDDEN_REQUEST_HEADERS = _FORBIDDEN_CREDENTIAL_HEADERS | {
    "api-key",
    "authorization",
    "openai-organization",
    "openai-project",
    "x-api-key",
}


@dataclass(frozen=True, slots=True)
class CredentialUsagePolicy:
    """Per-binding limits enforced before an authenticated upstream request."""

    max_concurrent_requests: int = 2
    requests_per_minute: int = 120
    max_request_bytes: int = 32 * 1024 * 1024
    queue_timeout_s: float = 5.0
    request_read_timeout_s: float = 15.0

    def __post_init__(self) -> None:
        _require_positive_int(self.max_concurrent_requests, name="credential broker concurrency")
        _require_positive_int(self.requests_per_minute, name="credential broker request rate")
        _require_positive_int(self.max_request_bytes, name="credential broker request size")
        if self.max_request_bytes > _MAX_CREDENTIAL_REQUEST_BYTES:
            raise ValueError("credential broker request size must not exceed 64 MiB")
        queue_timeout = _require_positive_finite_number(
            self.queue_timeout_s,
            name="credential broker queue timeout",
        )
        read_timeout = _require_positive_finite_number(
            self.request_read_timeout_s,
            name="credential broker request read timeout",
        )
        object.__setattr__(self, "queue_timeout_s", queue_timeout)
        object.__setattr__(self, "request_read_timeout_s", read_timeout)


@dataclass(frozen=True, slots=True)
class CredentialBrokerPolicy(CredentialUsagePolicy):
    """Tenant-wide limits for the built-in OpenAI Responses binding."""

    allowed_models: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        CredentialUsagePolicy.__post_init__(self)
        if isinstance(self.allowed_models, (str, bytes)) or any(
            not isinstance(model, str) for model in self.allowed_models
        ):
            raise ValueError("credential broker allowed models must be strings")
        normalized = tuple(model.strip() for model in self.allowed_models)
        if any(not model for model in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("credential broker allowed models must be unique and non-empty")
        object.__setattr__(self, "allowed_models", normalized)


@dataclass(frozen=True, slots=True)
class HttpCredentialBinding:
    """A fixed HTTPS origin that receives one broker-injected header credential."""

    name: str
    target_origin: str
    allowed_methods: tuple[str, ...]
    allowed_path_prefixes: tuple[str, ...]
    scope: CredentialBindingScope = "conversation"
    credential_header: str = "Authorization"
    credential_prefix: str = "Bearer "
    allowed_request_headers: tuple[str, ...] = ("accept", "content-type", "user-agent")
    usage_policy: CredentialUsagePolicy = field(default_factory=CredentialUsagePolicy)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _BINDING_NAME_RE.fullmatch(self.name):
            raise ValueError(
                "HTTP credential binding name must use 1-128 letters, digits, dots, dashes, or underscores"
            )
        if self.scope not in {"conversation", "tenant"}:
            raise ValueError("HTTP credential binding scope must be 'conversation' or 'tenant'")
        if not isinstance(self.target_origin, str):
            raise ValueError("HTTP credential target must be a string")
        object.__setattr__(self, "target_origin", _normalize_https_origin(self.target_origin))

        if not isinstance(self.credential_header, str):
            raise ValueError("HTTP credential header must be a string")
        header = self.credential_header.strip()
        if (
            len(header) > 128
            or not _HEADER_NAME_RE.fullmatch(header)
            or header.lower() in _FORBIDDEN_CREDENTIAL_HEADERS
        ):
            raise ValueError("HTTP credential header is invalid or unsafe")
        object.__setattr__(self, "credential_header", header)
        if not isinstance(self.credential_prefix, str) or len(self.credential_prefix) > 128 or any(
            ord(character) < 32 or ord(character) > 126 for character in self.credential_prefix
        ):
            raise ValueError("HTTP credential prefix must contain only printable ASCII")

        if isinstance(self.allowed_methods, (str, bytes)) or any(
            not isinstance(method, str) for method in self.allowed_methods
        ):
            raise ValueError("HTTP credential methods must be strings")
        methods = tuple(dict.fromkeys(method.strip().upper() for method in self.allowed_methods))
        if not methods or any(method not in _HTTP_METHODS for method in methods):
            raise ValueError(
                "HTTP credential methods must be a non-empty subset of GET, HEAD, POST, PUT, PATCH, DELETE"
            )
        object.__setattr__(self, "allowed_methods", methods)

        if isinstance(self.allowed_path_prefixes, (str, bytes)) or any(
            not isinstance(prefix, str) for prefix in self.allowed_path_prefixes
        ):
            raise ValueError("HTTP credential path prefixes must be strings")
        prefixes = tuple(dict.fromkeys(_normalize_path_prefix(prefix) for prefix in self.allowed_path_prefixes))
        if not prefixes:
            raise ValueError("HTTP credential path prefixes must not be empty")
        object.__setattr__(self, "allowed_path_prefixes", prefixes)

        if isinstance(self.allowed_request_headers, (str, bytes)) or any(
            not isinstance(header, str) for header in self.allowed_request_headers
        ):
            raise ValueError("HTTP credential request headers must be strings")
        request_headers = tuple(dict.fromkeys(header.strip().lower() for header in self.allowed_request_headers))
        injected_header = self.credential_header.lower()
        if any(
            not _HEADER_NAME_RE.fullmatch(header)
            or len(header) > 128
            or header in _FORBIDDEN_REQUEST_HEADERS
            or header == injected_header
            for header in request_headers
        ):
            raise ValueError("HTTP credential request-header allowlist contains an unsafe header")
        object.__setattr__(self, "allowed_request_headers", request_headers)
        if not isinstance(self.usage_policy, CredentialUsagePolicy):
            raise TypeError("HTTP credential usage_policy must be CredentialUsagePolicy")


@dataclass(frozen=True, slots=True)
class CredentialBrokerEndpoint:
    """Conversation-network endpoint with no provider credential material."""

    base_url: str
    network_ip: str


@dataclass(frozen=True, slots=True)
class CredentialBrokerCapability:
    """Conversation-network capability for one protected HTTP credential binding."""

    base_url: str = field(repr=False)
    network_ip: str


@runtime_checkable
class CredentialBrokerProvisioner(Protocol):
    async def provision_credential_broker(
        self,
        handle: SandboxHandle,
        *,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        """Bind a tenant broker to the conversation without exposing the API key."""
        ...


@runtime_checkable
class HttpCredentialBrokerProvisioner(Protocol):
    async def provision_http_credential(
        self,
        handle: SandboxHandle,
        *,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        """Bind a protected static HTTP credential to a managed conversation."""
        ...

    async def revoke_http_credential(
        self,
        handle: SandboxHandle,
        *,
        name: str,
        scope: CredentialBindingScope = "conversation",
    ) -> None:
        """Revoke one protected HTTP credential binding."""
        ...


class SandboxManager(Protocol):
    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        """Return a running sandbox for this spec, creating one if necessary."""
        ...

    async def destroy(self, handle: SandboxHandle) -> None:
        """Stop and remove a sandbox owned by this manager."""
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


def _normalize_https_origin(value: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("HTTP credential target must be an HTTPS origin without credentials, path, query, or fragment")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("HTTP credential target port is invalid") from exc
    hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        labels = hostname.split(".")
        if len(hostname) > 253 or len(labels) < 2 or any(not _DOMAIN_LABEL_RE.fullmatch(label) for label in labels):
            raise ValueError("HTTP credential target hostname must be a normalized public DNS name")
    else:
        if not address.is_global:
            raise ValueError("HTTP credential target cannot be a private or non-global IP address")
        if address.version == 6:
            hostname = f"[{hostname}]"
    if port not in {None, 443}:
        raise ValueError("HTTP credential target must use HTTPS port 443")
    return f"https://{hostname}"


def _normalize_path_prefix(value: str) -> str:
    prefix = value.strip()
    if (
        not prefix.startswith("/")
        or "?" in prefix
        or "#" in prefix
        or "//" in prefix
        or "\\" in prefix
        or any(segment in {".", ".."} for segment in prefix.split("/"))
    ):
        raise ValueError("HTTP credential path prefix must be an absolute normalized path")
    if prefix != "/":
        prefix = prefix.rstrip("/")
    return prefix


def _require_positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_positive_finite_number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive finite number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return normalized
