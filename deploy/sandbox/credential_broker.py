"""Policy-bound HTTP credential broker for isolated tenant sandboxes."""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import math
import os
import re
import secrets
import struct
import sys
import time
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector, web
from multidict import CIMultiDict
from yarl import URL

ADMIN_SOCKET_PATH = Path("/run/soveren/credential-broker.sock")
MAX_ADMIN_PAYLOAD_BYTES = 1024 * 1024
MAX_BINDINGS_PER_TENANT = 256
MAX_TENANTS = 256
MAX_TOTAL_BINDINGS = 1024
MAX_REQUEST_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_BUFFERED_REQUEST_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_INFLIGHT_REQUESTS = 16
DEFAULT_MAX_BUFFERED_REQUEST_BYTES_PER_TENANT = 32 * 1024 * 1024
DEFAULT_MAX_INFLIGHT_REQUESTS_PER_TENANT = 8
LEGACY_REGISTRY_VERSION = 1
REGISTRY_VERSION = 2
OPENAI_UPSTREAM = {
    "/v1/responses": "https://api.openai.com/v1/responses",
    "/v1/responses/compact": "https://api.openai.com/v1/responses/compact",
}
HTTP_METHODS = frozenset({"DELETE", "GET", "HEAD", "PATCH", "POST", "PUT"})
HEADER_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
BINDING_ID_RE = re.compile(r"^[a-f0-9]{64}$")
TENANT_KEY_RE = re.compile(r"^[a-f0-9]{64}$")
CAPABILITY_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
LEGACY_TENANT_KEY = "0" * 64
CGROUP_MEMORY_LIMIT_PATHS = (
    Path("/sys/fs/cgroup/memory.max"),
    Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
)
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
STRIPPED_REQUEST_HEADERS = HOP_BY_HOP_HEADERS | {
    "authorization",
    "api-key",
    "content-length",
    "cookie",
    "forwarded",
    "host",
    "openai-organization",
    "openai-project",
    "set-cookie",
    "x-api-key",
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
STRIPPED_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {
    "content-length",
    "set-cookie",
}


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float(value: Any, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value <= 0:
        raise ValueError(f"{name} must be positive")
    return float(value)


def _env_positive_int(name: str, default: int) -> int:
    return _positive_int(int(os.environ.get(name, default)), name=name)


def _env_positive_float(name: str, default: float) -> float:
    return _positive_float(float(os.environ.get(name, default)), name=name)


def _max_buffered_request_bytes(configured: int | None = None) -> int:
    if configured is None:
        configured = _env_positive_int(
            "SOVEREN_BROKER_MAX_BUFFERED_REQUEST_BYTES",
            DEFAULT_MAX_BUFFERED_REQUEST_BYTES,
        )
    else:
        configured = _positive_int(
            configured,
            name="broker max buffered request bytes",
        )
    memory_limit = _cgroup_memory_limit_bytes()
    if memory_limit is None:
        return configured
    return min(configured, max(1, memory_limit // 2))


def _cgroup_memory_limit_bytes() -> int | None:
    for path in CGROUP_MEMORY_LIMIT_PATHS:
        try:
            raw = path.read_text(encoding="ascii").strip()
        except (FileNotFoundError, OSError, UnicodeError):
            continue
        if raw == "max":
            return None
        try:
            value = int(raw)
        except ValueError:
            continue
        if 0 < value < 1 << 60:
            return value
    return None


def _allowed_models_from_env() -> tuple[str, ...]:
    raw = json.loads(os.environ.get("SOVEREN_BROKER_ALLOWED_MODELS", "[]"))
    if not isinstance(raw, list) or any(not isinstance(model, str) or not model for model in raw):
        raise ValueError("SOVEREN_BROKER_ALLOWED_MODELS must be a JSON array of model names")
    return tuple(raw)


def _egress_proxy() -> str:
    value = os.environ.get("SOVEREN_BROKER_EGRESS_PROXY", "")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.port is None
    ):
        raise ValueError("SOVEREN_BROKER_EGRESS_PROXY must be an HTTP proxy URL")
    return value


class RateLimiter:
    def __init__(self) -> None:
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def allow(self, limit: int) -> bool:
        now = time.monotonic()
        async with self._lock:
            cutoff = now - 60.0
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= limit:
                return False
            self._timestamps.append(now)
            return True


class BindingRuntime:
    """Admission state that survives binding secret and policy replacement."""

    def __init__(self, max_concurrent_requests: int) -> None:
        self._active_requests = 0
        self._waiting_requests = 0
        self._max_concurrent_requests = max_concurrent_requests
        self._condition = asyncio.Condition()
        self._rate_limiter = RateLimiter()

    async def allow_rate(self, limit: int) -> bool:
        return await self._rate_limiter.allow(limit)

    async def configure(self, *, max_concurrent_requests: int) -> None:
        async with self._condition:
            self._max_concurrent_requests = max_concurrent_requests
            self._condition.notify_all()

    async def acquire(self, *, timeout_s: float) -> None:
        async with asyncio.timeout(timeout_s):
            async with self._condition:
                self._waiting_requests += 1
                try:
                    await self._condition.wait_for(lambda: self._active_requests < self._max_concurrent_requests)
                    self._active_requests += 1
                finally:
                    self._waiting_requests -= 1

    async def release(self) -> None:
        async with self._condition:
            if self._active_requests < 1:
                raise RuntimeError("credential binding runtime was released without acquisition")
            self._active_requests -= 1
            self._condition.notify_all()

    async def is_idle(self) -> bool:
        async with self._condition:
            return self._active_requests == 0 and self._waiting_requests == 0


class BrokerCapacity:
    """Broker-wide admission for process overhead and buffered request bodies."""

    def __init__(self, *, max_inflight_requests: int, max_buffered_request_bytes: int) -> None:
        self.max_inflight_requests = _positive_int(
            max_inflight_requests,
            name="broker max inflight requests",
        )
        self.max_buffered_request_bytes = _positive_int(
            max_buffered_request_bytes,
            name="broker max buffered request bytes",
        )
        self._inflight_requests = 0
        self._waiting_requests = 0
        self._buffered_request_bytes = 0
        self._condition = asyncio.Condition()

    async def acquire(self, *, request_bytes: int, timeout_s: float) -> None:
        if request_bytes > self.max_buffered_request_bytes:
            raise web.HTTPRequestEntityTooLarge(
                max_size=self.max_buffered_request_bytes,
                actual_size=request_bytes,
            )
        async with asyncio.timeout(timeout_s):
            async with self._condition:
                self._waiting_requests += 1
                try:
                    await self._condition.wait_for(
                        lambda: (
                            self._inflight_requests < self.max_inflight_requests
                            and self._buffered_request_bytes + request_bytes <= self.max_buffered_request_bytes
                        )
                    )
                    self._inflight_requests += 1
                    self._buffered_request_bytes += request_bytes
                finally:
                    self._waiting_requests -= 1

    async def release(self, *, request_bytes: int) -> None:
        async with self._condition:
            if self._inflight_requests < 1 or self._buffered_request_bytes < request_bytes:
                raise RuntimeError("credential broker capacity was released without acquisition")
            self._inflight_requests -= 1
            self._buffered_request_bytes -= request_bytes
            self._condition.notify_all()

    async def is_idle(self) -> bool:
        async with self._condition:
            return self._inflight_requests == 0 and self._waiting_requests == 0


@dataclass(frozen=True, slots=True)
class BindingLimits:
    max_concurrent_requests: int
    requests_per_minute: int
    max_request_bytes: int
    queue_timeout_s: float
    request_read_timeout_s: float

    @classmethod
    def parse(cls, value: Any) -> BindingLimits:
        if not isinstance(value, Mapping):
            raise ValueError("binding limits must be an object")
        max_request_bytes = _positive_int(
            value.get("max_request_bytes"),
            name="max_request_bytes",
        )
        if max_request_bytes > MAX_REQUEST_BYTES:
            raise ValueError("max_request_bytes must not exceed 64 MiB")
        return cls(
            max_concurrent_requests=_positive_int(
                value.get("max_concurrent_requests"),
                name="max_concurrent_requests",
            ),
            requests_per_minute=_positive_int(
                value.get("requests_per_minute"),
                name="requests_per_minute",
            ),
            max_request_bytes=max_request_bytes,
            queue_timeout_s=_positive_float(value.get("queue_timeout_s"), name="queue_timeout_s"),
            request_read_timeout_s=_positive_float(
                value.get("request_read_timeout_s"),
                name="request_read_timeout_s",
            ),
        )


@dataclass(frozen=True, slots=True)
class BrokerBinding:
    binding_id: str
    kind: str
    secret: str = field(repr=False)
    allowed_local_ips: frozenset[str] = field(default_factory=frozenset)
    limits: BindingLimits = field(
        default_factory=lambda: BindingLimits(
            max_concurrent_requests=2,
            requests_per_minute=120,
            max_request_bytes=32 * 1024 * 1024,
            queue_timeout_s=5.0,
            request_read_timeout_s=15.0,
        )
    )
    capability: str | None = None
    target_origin: str | None = None
    credential_header: str = "Authorization"
    credential_prefix: str = "Bearer "
    allowed_methods: frozenset[str] = field(default_factory=frozenset)
    allowed_path_prefixes: tuple[str, ...] = ()
    allowed_request_headers: frozenset[str] = field(default_factory=frozenset)
    allowed_models: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True, slots=True)
class TenantBindingRegistry:
    openai: BrokerBinding | None
    by_capability: Mapping[str, BrokerBinding]


@dataclass(frozen=True, slots=True)
class BindingRegistry:
    by_tenant: Mapping[str, TenantBindingRegistry]
    tenant_by_local_ip: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class RegistryUpdate:
    tenant_key: str
    registry: TenantBindingRegistry | None


def _tenant_registry_bindings(registry: TenantBindingRegistry) -> tuple[BrokerBinding, ...]:
    openai = (registry.openai,) if registry.openai is not None else ()
    return (*openai, *registry.by_capability.values())


def _registry_bindings(registry: BindingRegistry) -> tuple[BrokerBinding, ...]:
    return tuple(
        binding
        for tenant_registry in registry.by_tenant.values()
        for binding in _tenant_registry_bindings(tenant_registry)
    )


def _registry_contains(registry: BindingRegistry, binding_id: str) -> bool:
    return any(binding.binding_id == binding_id for binding in _registry_bindings(registry))


class CredentialBroker:
    def __init__(
        self,
        *,
        initial_registry: BindingRegistry | None = None,
        admin_socket_path: Path | None = None,
        max_inflight_requests: int | None = None,
        max_buffered_request_bytes: int | None = None,
        max_inflight_requests_per_tenant: int | None = None,
        max_buffered_request_bytes_per_tenant: int | None = None,
    ) -> None:
        self._registry = initial_registry or BindingRegistry(by_tenant={}, tenant_by_local_ip={})
        self._registry_lock = asyncio.Lock()
        self._binding_runtimes: dict[str, BindingRuntime] = {}
        self._capacity = BrokerCapacity(
            max_inflight_requests=(
                max_inflight_requests
                if max_inflight_requests is not None
                else _env_positive_int(
                    "SOVEREN_BROKER_MAX_INFLIGHT_REQUESTS",
                    DEFAULT_MAX_INFLIGHT_REQUESTS,
                )
            ),
            max_buffered_request_bytes=(_max_buffered_request_bytes(max_buffered_request_bytes)),
        )
        configured_tenant_inflight = (
            max_inflight_requests_per_tenant
            if max_inflight_requests_per_tenant is not None
            else _env_positive_int(
                "SOVEREN_BROKER_MAX_INFLIGHT_REQUESTS_PER_TENANT",
                DEFAULT_MAX_INFLIGHT_REQUESTS_PER_TENANT,
            )
        )
        configured_tenant_buffer = (
            max_buffered_request_bytes_per_tenant
            if max_buffered_request_bytes_per_tenant is not None
            else _env_positive_int(
                "SOVEREN_BROKER_MAX_BUFFERED_REQUEST_BYTES_PER_TENANT",
                DEFAULT_MAX_BUFFERED_REQUEST_BYTES_PER_TENANT,
            )
        )
        self._max_inflight_requests_per_tenant = min(
            _positive_int(
                configured_tenant_inflight,
                name="broker max inflight requests per tenant",
            ),
            self._capacity.max_inflight_requests,
        )
        self._max_buffered_request_bytes_per_tenant = min(
            _positive_int(
                configured_tenant_buffer,
                name="broker max buffered request bytes per tenant",
            ),
            self._capacity.max_buffered_request_bytes,
        )
        self._tenant_capacities: dict[str, BrokerCapacity] = {}
        self._validate_registry_capacity(self._registry)
        self._admin_socket_path = admin_socket_path
        self._admin_server: asyncio.AbstractServer | None = None
        self._session: ClientSession | None = None
        self._egress_proxy: str | None = None

    async def start(self, app: web.Application) -> None:
        timeout = ClientTimeout(total=None, connect=15.0, sock_connect=15.0, sock_read=310.0)
        self._session = ClientSession(
            timeout=timeout,
            connector=TCPConnector(limit=32, ttl_dns_cache=60),
            auto_decompress=False,
            skip_auto_headers={"Accept", "Accept-Encoding", "Content-Type", "User-Agent"},
            trust_env=False,
        )
        self._egress_proxy = _egress_proxy()
        path = self._admin_socket_path
        if path is not None:
            path.unlink(missing_ok=True)
            self._admin_server = await asyncio.start_unix_server(self._handle_admin, path=str(path))
            os.chmod(path, 0o600)

    async def stop(self, app: web.Application) -> None:
        server = self._admin_server
        self._admin_server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        if self._admin_socket_path is not None:
            self._admin_socket_path.unlink(missing_ok=True)
        session = self._session
        self._session = None
        if session is not None:
            await session.close()

    async def health(self, request: web.Request) -> web.Response:
        return web.Response(status=204)

    async def forward_openai(self, request: web.Request) -> web.StreamResponse:
        return await self._forward(request, route_kind="openai", audit_route="openai")

    async def forward_binding(self, request: web.Request) -> web.StreamResponse:
        return await self._forward(
            request,
            route_kind="http_binding",
            audit_route="http_binding",
        )

    async def _forward(
        self,
        request: web.Request,
        *,
        route_kind: Literal["http_binding", "openai"],
        audit_route: str,
    ) -> web.StreamResponse:
        started = time.monotonic()
        request_id = secrets.token_hex(8)
        status = 500
        request_bytes = 0
        reserved_request_bytes = 0
        runtime: BindingRuntime | None = None
        tenant_capacity: BrokerCapacity | None = None
        tenant_key: str | None = None
        binding_id: str | None = None
        runtime_acquired = False
        tenant_capacity_acquired = False
        capacity_acquired = False
        try:
            if request.headers.get("Content-Encoding"):
                raise web.HTTPUnsupportedMediaType(text="compressed requests are not supported")
            async with self._registry_lock:
                tenant_key, binding, _ = self._resolve_binding(request, route_kind=route_kind)
                binding_id = binding.binding_id
                runtime = self._binding_runtimes.setdefault(
                    binding_id,
                    BindingRuntime(binding.limits.max_concurrent_requests),
                )
                tenant_capacity = self._tenant_capacities.setdefault(
                    tenant_key,
                    BrokerCapacity(
                        max_inflight_requests=self._max_inflight_requests_per_tenant,
                        max_buffered_request_bytes=self._max_buffered_request_bytes_per_tenant,
                    ),
                )
            content_length = request.content_length
            if content_length is not None and content_length > binding.limits.max_request_bytes:
                raise web.HTTPRequestEntityTooLarge(
                    max_size=binding.limits.max_request_bytes,
                    actual_size=content_length,
                )
            deadline = asyncio.get_running_loop().time() + binding.limits.queue_timeout_s
            assert runtime is not None and tenant_capacity is not None
            try:
                await runtime.acquire(
                    timeout_s=_remaining_timeout(deadline),
                )
            except TimeoutError as exc:
                raise web.HTTPTooManyRequests(text="credential concurrency limit exceeded") from exc
            runtime_acquired = True
            reserved_request_bytes = _request_buffer_reservation(
                request,
                max_request_bytes=binding.limits.max_request_bytes,
            )
            try:
                await tenant_capacity.acquire(
                    request_bytes=reserved_request_bytes,
                    timeout_s=_remaining_timeout(deadline),
                )
            except TimeoutError as exc:
                raise web.HTTPTooManyRequests(text="tenant credential capacity exceeded") from exc
            tenant_capacity_acquired = True
            try:
                await self._capacity.acquire(
                    request_bytes=reserved_request_bytes,
                    timeout_s=_remaining_timeout(deadline),
                )
            except TimeoutError as exc:
                raise web.HTTPTooManyRequests(text="credential broker capacity exceeded") from exc
            capacity_acquired = True
            try:
                async with asyncio.timeout(binding.limits.request_read_timeout_s):
                    body = await _read_bounded_body(
                        request,
                        max_bytes=binding.limits.max_request_bytes,
                    )
            except TimeoutError as exc:
                raise web.HTTPRequestTimeout(text="request body read timed out") from exc
            request_bytes = len(body)
            async with self._registry_lock:
                current_tenant_key, current_binding, upstream = self._resolve_binding(
                    request,
                    route_kind=route_kind,
                )
                if current_tenant_key != tenant_key or current_binding.binding_id != binding_id:
                    raise web.HTTPNotFound(text="credential binding was replaced")
                if request_bytes > current_binding.limits.max_request_bytes:
                    raise web.HTTPRequestEntityTooLarge(
                        max_size=current_binding.limits.max_request_bytes,
                        actual_size=request_bytes,
                    )
                if current_binding.kind == "openai_responses":
                    _enforce_model_policy(body, current_binding.allowed_models)
                if not await runtime.allow_rate(current_binding.limits.requests_per_minute):
                    raise web.HTTPTooManyRequests(text="credential request rate exceeded")
                headers = _request_headers(
                    request.headers.items(),
                    binding=current_binding,
                    upstream=upstream,
                )
                binding = current_binding
            session = self._session
            if session is None:
                raise web.HTTPServiceUnavailable(text="credential broker is starting")
            try:
                async with session.request(
                    request.method,
                    upstream,
                    data=body,
                    headers=headers,
                    allow_redirects=False,
                    proxy=self._egress_proxy,
                ) as upstream_response:
                    status = upstream_response.status
                    response = web.StreamResponse(
                        status=status,
                        reason=upstream_response.reason,
                        headers=_response_headers(
                            upstream_response.headers.items(),
                            credential_header=binding.credential_header,
                        ),
                    )
                    await response.prepare(request)
                    async for chunk in upstream_response.content.iter_any():
                        await response.write(chunk)
                    await response.write_eof()
                    return response
            except ClientError as exc:
                status = 502
                raise web.HTTPBadGateway(text="credential upstream unavailable") from exc
        except web.HTTPException as exc:
            status = exc.status
            raise
        finally:
            if capacity_acquired:
                await self._capacity.release(request_bytes=reserved_request_bytes)
            if tenant_capacity_acquired and tenant_capacity is not None and tenant_key is not None:
                await self._release_tenant_capacity(
                    tenant_key,
                    tenant_capacity,
                    request_bytes=reserved_request_bytes,
                )
            if runtime_acquired and runtime is not None and binding_id is not None:
                await self._release_binding_runtime(binding_id, runtime)
            _audit(request_id, audit_route, request.method, status, request_bytes, started)

    def _resolve_binding(
        self,
        request: web.Request,
        *,
        route_kind: Literal["http_binding", "openai"],
    ) -> tuple[str, BrokerBinding, URL]:
        local_ip = _request_local_ip(request)
        tenant_key = self._registry.tenant_by_local_ip.get(local_ip)
        if tenant_key is None:
            raise web.HTTPForbidden(text="credential tenant network is not authorized")
        tenant_registry = self._registry.by_tenant[tenant_key]
        if route_kind == "openai":
            binding = tenant_registry.openai
            if binding is None:
                raise web.HTTPServiceUnavailable(text="OpenAI credential binding is not provisioned")
            _ensure_binding_access(local_ip, binding)
            if request.query_string:
                raise web.HTTPBadRequest(text="query parameters are not supported")
            return tenant_key, binding, URL(OPENAI_UPSTREAM[request.path])

        capability = request.match_info["capability"]
        binding = tenant_registry.by_capability.get(capability)
        if binding is None:
            raise web.HTTPNotFound(text="credential binding not found")
        _ensure_binding_access(local_ip, binding)
        if request.method not in binding.allowed_methods:
            raise web.HTTPMethodNotAllowed(request.method, binding.allowed_methods)
        path = _request_binding_path(request.match_info.get("tail", ""))
        if not any(_path_matches_prefix(path, prefix) for prefix in binding.allowed_path_prefixes):
            raise web.HTTPForbidden(text="request path is not allowed for this credential binding")
        if binding.target_origin is None:
            raise web.HTTPServiceUnavailable(text="credential binding is invalid")
        return tenant_key, binding, URL(binding.target_origin).with_path(path).with_query(request.query)

    async def _release_binding_runtime(
        self,
        binding_id: str,
        runtime: BindingRuntime,
    ) -> None:
        await runtime.release()
        async with self._registry_lock:
            if (
                self._binding_runtimes.get(binding_id) is runtime
                and not _registry_contains(self._registry, binding_id)
                and await runtime.is_idle()
            ):
                self._binding_runtimes.pop(binding_id, None)

    async def _release_tenant_capacity(
        self,
        tenant_key: str,
        capacity: BrokerCapacity,
        *,
        request_bytes: int,
    ) -> None:
        await capacity.release(request_bytes=request_bytes)
        async with self._registry_lock:
            if (
                self._tenant_capacities.get(tenant_key) is capacity
                and tenant_key not in self._registry.by_tenant
                and await capacity.is_idle()
            ):
                self._tenant_capacities.pop(tenant_key, None)

    async def _replace_tenant_registry(self, update: RegistryUpdate) -> None:
        async with self._registry_lock:
            by_tenant = dict(self._registry.by_tenant)
            if update.registry is None:
                by_tenant.pop(update.tenant_key, None)
            else:
                by_tenant[update.tenant_key] = update.registry
            registry = _build_registry(by_tenant)
            self._validate_registry_capacity(registry)
            bindings_by_id = {binding.binding_id: binding for binding in _registry_bindings(registry)}
            for binding_id, runtime in self._binding_runtimes.items():
                binding = bindings_by_id.get(binding_id)
                if binding is not None:
                    await runtime.configure(max_concurrent_requests=binding.limits.max_concurrent_requests)
            self._registry = registry
            active_ids = frozenset(bindings_by_id)
            for binding_id, runtime in tuple(self._binding_runtimes.items()):
                if binding_id not in active_ids and await runtime.is_idle():
                    self._binding_runtimes.pop(binding_id, None)
            for tenant_key, capacity in tuple(self._tenant_capacities.items()):
                if tenant_key not in registry.by_tenant and await capacity.is_idle():
                    self._tenant_capacities.pop(tenant_key, None)

    def _validate_registry_capacity(self, registry: BindingRegistry) -> None:
        max_buffered = self._capacity.max_buffered_request_bytes
        max_tenant_buffered = self._max_buffered_request_bytes_per_tenant
        if any(
            binding.limits.max_request_bytes > min(max_buffered, max_tenant_buffered)
            for binding in _registry_bindings(registry)
        ):
            raise ValueError("credential binding request size exceeds the broker-wide buffer budget")

    async def _handle_admin(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        response = {"ok": False, "error": "credential registry update failed"}
        try:
            size = struct.unpack("!I", await reader.readexactly(4))[0]
            if size < 1 or size > MAX_ADMIN_PAYLOAD_BYTES:
                raise ValueError("credential registry payload size is invalid")
            payload = await reader.readexactly(size)
            update = _parse_registry_update(payload)
            await self._replace_tenant_registry(update)
            response = {"ok": True}
        except (asyncio.IncompleteReadError, UnicodeError, ValueError, json.JSONDecodeError):
            pass
        finally:
            encoded = json.dumps(response, separators=(",", ":")).encode("utf-8")
            writer.write(struct.pack("!I", len(encoded)) + encoded)
            try:
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()


def _parse_registry_update(payload: bytes) -> RegistryUpdate:
    raw = json.loads(payload)
    if not isinstance(raw, dict) or raw.get("version") != REGISTRY_VERSION:
        raise ValueError("credential registry version is invalid")
    tenant_key = raw.get("tenant_key")
    if not isinstance(tenant_key, str) or not TENANT_KEY_RE.fullmatch(tenant_key):
        raise ValueError("credential registry tenant key is invalid")
    operation = raw.get("operation")
    if operation == "remove_tenant":
        if "bindings" in raw:
            raise ValueError("credential tenant removal must not contain bindings")
        return RegistryUpdate(tenant_key=tenant_key, registry=None)
    if operation != "replace_tenant":
        raise ValueError("credential registry operation is invalid")
    values = raw.get("bindings")
    if not isinstance(values, list) or not values or len(values) > MAX_BINDINGS_PER_TENANT:
        raise ValueError("credential registry bindings are invalid")
    return RegistryUpdate(
        tenant_key=tenant_key,
        registry=_parse_tenant_registry(values),
    )


def _parse_registry(payload: bytes) -> BindingRegistry:
    """Parse one initial registry while retaining the version-1 test/CLI format."""
    raw = json.loads(payload)
    if not isinstance(raw, dict):
        raise ValueError("credential registry is invalid")
    if raw.get("version") == REGISTRY_VERSION:
        update = _parse_registry_update(payload)
        if update.registry is None:
            return BindingRegistry(by_tenant={}, tenant_by_local_ip={})
        return _build_registry({update.tenant_key: update.registry})
    if raw.get("version") != LEGACY_REGISTRY_VERSION:
        raise ValueError("credential registry version is invalid")
    values = raw.get("bindings")
    if not isinstance(values, list) or len(values) > MAX_BINDINGS_PER_TENANT:
        raise ValueError("credential registry bindings are invalid")
    if not values:
        return BindingRegistry(by_tenant={}, tenant_by_local_ip={})
    return _build_registry({LEGACY_TENANT_KEY: _parse_tenant_registry(values)})


def _parse_tenant_registry(values: list[Any]) -> TenantBindingRegistry:
    openai: BrokerBinding | None = None
    by_capability: dict[str, BrokerBinding] = {}
    binding_ids: set[str] = set()
    for value in values:
        binding = _parse_binding(value)
        if binding.binding_id in binding_ids:
            raise ValueError("credential registry contains duplicate binding ids")
        binding_ids.add(binding.binding_id)
        if binding.kind == "openai_responses":
            if openai is not None:
                raise ValueError("credential registry contains multiple OpenAI bindings")
            openai = binding
            continue
        capability = binding.capability
        if capability is None or capability in by_capability:
            raise ValueError("credential registry contains duplicate capabilities")
        by_capability[capability] = binding
    return TenantBindingRegistry(openai=openai, by_capability=by_capability)


def _build_registry(by_tenant: Mapping[str, TenantBindingRegistry]) -> BindingRegistry:
    if len(by_tenant) > MAX_TENANTS:
        raise ValueError("credential registry tenant limit exceeded")
    tenant_by_local_ip: dict[str, str] = {}
    binding_ids: set[str] = set()
    total_bindings = 0
    for tenant_key, tenant_registry in by_tenant.items():
        if not TENANT_KEY_RE.fullmatch(tenant_key):
            raise ValueError("credential registry tenant key is invalid")
        bindings = _tenant_registry_bindings(tenant_registry)
        if not bindings or len(bindings) > MAX_BINDINGS_PER_TENANT:
            raise ValueError("credential registry bindings are invalid")
        total_bindings += len(bindings)
        if total_bindings > MAX_TOTAL_BINDINGS:
            raise ValueError("credential registry global binding limit exceeded")
        for binding in bindings:
            if binding.binding_id in binding_ids:
                raise ValueError("credential registry contains cross-tenant duplicate binding ids")
            binding_ids.add(binding.binding_id)
            for local_ip in binding.allowed_local_ips:
                owner = tenant_by_local_ip.setdefault(local_ip, tenant_key)
                if owner != tenant_key:
                    raise ValueError("credential registry local IP belongs to multiple tenants")
    return BindingRegistry(
        by_tenant=dict(by_tenant),
        tenant_by_local_ip=tenant_by_local_ip,
    )


def _parse_binding(value: Any) -> BrokerBinding:
    if not isinstance(value, Mapping):
        raise ValueError("credential binding must be an object")
    binding_id = value.get("binding_id")
    kind = value.get("kind")
    if not isinstance(binding_id, str) or not BINDING_ID_RE.fullmatch(binding_id):
        raise ValueError("credential binding id is invalid")
    if kind not in {"http", "openai_responses"}:
        raise ValueError("credential binding kind is invalid")
    secret = _decode_secret(value.get("secret"))
    local_ips = _parse_local_ips(value.get("allowed_local_ips"))
    limits = BindingLimits.parse(value.get("limits"))
    if kind == "openai_responses":
        allowed_models = value.get("allowed_models", [])
        if not isinstance(allowed_models, list) or any(
            not isinstance(model, str) or not model for model in allowed_models
        ):
            raise ValueError("OpenAI binding model allowlist is invalid")
        return BrokerBinding(
            binding_id=binding_id,
            kind=kind,
            secret=secret,
            allowed_local_ips=local_ips,
            limits=limits,
            allowed_methods=frozenset({"POST"}),
            allowed_models=frozenset(allowed_models),
        )

    capability = value.get("capability")
    target_origin = value.get("target_origin")
    credential_header = value.get("credential_header")
    credential_prefix = value.get("credential_prefix")
    if not isinstance(capability, str) or not CAPABILITY_RE.fullmatch(capability):
        raise ValueError("HTTP credential capability is invalid")
    if not isinstance(target_origin, str):
        raise ValueError("HTTP credential target is invalid")
    normalized_origin = _normalize_https_origin(target_origin)
    if (
        not isinstance(credential_header, str)
        or len(credential_header) > 128
        or not HEADER_NAME_RE.fullmatch(credential_header)
    ):
        raise ValueError("HTTP credential header is invalid")
    if credential_header.lower() in STRIPPED_REQUEST_HEADERS - {"authorization", "api-key", "x-api-key"}:
        raise ValueError("HTTP credential header is unsafe")
    if (
        not isinstance(credential_prefix, str)
        or len(credential_prefix) > 128
        or any(ord(character) < 32 or ord(character) > 126 for character in credential_prefix)
    ):
        raise ValueError("HTTP credential prefix is invalid")
    methods = frozenset(
        method.upper() for method in _parse_string_set(value.get("allowed_methods"), name="allowed methods")
    )
    if not methods or any(method not in HTTP_METHODS for method in methods):
        raise ValueError("HTTP credential methods are invalid")
    prefixes = _parse_path_prefixes(value.get("allowed_path_prefixes"))
    request_headers = frozenset(
        header.lower() for header in _parse_string_set(value.get("allowed_request_headers"), name="request headers")
    )
    injected = credential_header.lower()
    if any(
        not HEADER_NAME_RE.fullmatch(header)
        or len(header) > 128
        or header in STRIPPED_REQUEST_HEADERS
        or header == injected
        for header in request_headers
    ):
        raise ValueError("HTTP credential request headers are unsafe")
    return BrokerBinding(
        binding_id=binding_id,
        kind=kind,
        secret=secret,
        allowed_local_ips=local_ips,
        limits=limits,
        capability=capability,
        target_origin=normalized_origin,
        credential_header=credential_header,
        credential_prefix=credential_prefix,
        allowed_methods=methods,
        allowed_path_prefixes=prefixes,
        allowed_request_headers=request_headers,
    )


def _decode_secret(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("credential secret is invalid")
    try:
        raw = base64.b64decode(value, validate=True)
        decoded = raw.decode("ascii")
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("credential secret is invalid") from exc
    if (
        not decoded
        or len(raw) > 16 * 1024
        or decoded != decoded.strip()
        or any(ord(character) < 33 or ord(character) > 126 for character in decoded)
    ):
        raise ValueError("credential secret is invalid")
    return decoded


def _parse_local_ips(value: Any) -> frozenset[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("credential binding local IP allowlist is invalid")
    parsed: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("credential binding local IP allowlist is invalid")
        address = ipaddress.ip_address(item)
        parsed.add(str(address))
    return frozenset(parsed)


def _parse_string_set(value: Any, *, name: str) -> frozenset[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"credential binding {name} are invalid")
    return frozenset(value)


def _parse_path_prefixes(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or any(not isinstance(item, str) for item in value):
        raise ValueError("credential binding path prefixes are invalid")
    return tuple(dict.fromkeys(_normalize_path_prefix(item) for item in value))


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
        raise ValueError("credential target must be an HTTPS origin")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("credential target port is invalid") from exc
    hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        labels = hostname.split(".")
        if len(hostname) > 253 or len(labels) < 2 or any(not DOMAIN_LABEL_RE.fullmatch(label) for label in labels):
            raise ValueError("credential target hostname must be a normalized public DNS name")
    else:
        if not address.is_global:
            raise ValueError("credential target IP must be global")
        if address.version == 6:
            hostname = f"[{hostname}]"
    if port not in {None, 443}:
        raise ValueError("credential target must use HTTPS port 443")
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
        raise ValueError("credential path prefix is invalid")
    return prefix.rstrip("/") if prefix != "/" else prefix


def _request_binding_path(tail: str) -> str:
    path = f"/{tail}" if tail else "/"
    if "//" in path or "\\" in path or any(segment in {".", ".."} for segment in path.split("/")):
        raise web.HTTPBadRequest(text="request path is not normalized")
    return path


def _path_matches_prefix(path: str, prefix: str) -> bool:
    return prefix == "/" or path == prefix or path.startswith(f"{prefix}/")


def _request_local_ip(request: web.Request) -> str:
    transport = request.transport
    sockname = transport.get_extra_info("sockname") if transport is not None else None
    if not isinstance(sockname, tuple) or not sockname:
        raise web.HTTPForbidden(text="credential binding network could not be verified")
    try:
        return str(ipaddress.ip_address(sockname[0]))
    except ValueError as exc:
        raise web.HTTPForbidden(text="credential binding network could not be verified") from exc


def _ensure_binding_access(local_ip: str, binding: BrokerBinding) -> None:
    if local_ip not in binding.allowed_local_ips:
        raise web.HTTPForbidden(text="credential binding is not authorized for this conversation network")


def _enforce_model_policy(body: bytes | bytearray, allowed_models: frozenset[str]) -> None:
    if not allowed_models:
        return
    try:
        payload: Any = json.loads(body)
    except json.JSONDecodeError as exc:
        raise web.HTTPBadRequest(text="request body must be JSON") from exc
    model = payload.get("model") if isinstance(payload, dict) else None
    if not isinstance(model, str) or model not in allowed_models:
        raise web.HTTPForbidden(text="model is not allowed for this tenant")


def _remaining_timeout(deadline: float) -> float:
    return max(0.0, deadline - asyncio.get_running_loop().time())


def _request_buffer_reservation(request: web.Request, *, max_request_bytes: int) -> int:
    content_length = request.content_length
    if content_length is not None:
        return content_length
    if request.can_read_body:
        return max_request_bytes
    return 0


async def _read_bounded_body(request: web.Request, *, max_bytes: int) -> bytearray:
    body = bytearray()
    while True:
        remaining = max_bytes - len(body) + 1
        chunk = await request.content.read(min(64 * 1024, remaining))
        if not chunk:
            return body
        body.extend(chunk)
        if len(body) > max_bytes:
            raise web.HTTPRequestEntityTooLarge(
                max_size=max_bytes,
                actual_size=len(body),
            )


def _request_headers(
    headers: Iterable[tuple[str, str]],
    *,
    binding: BrokerBinding,
    upstream: URL,
) -> CIMultiDict[str]:
    items = list(headers)
    connection_headers = {
        token.strip().lower()
        for name, value in items
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    excluded = STRIPPED_REQUEST_HEADERS | connection_headers | {binding.credential_header.lower()}
    if binding.kind == "openai_responses":
        forwarded = CIMultiDict((name, value) for name, value in items if name.lower() not in excluded)
    else:
        forwarded = CIMultiDict(
            (name, value)
            for name, value in items
            if name.lower() in binding.allowed_request_headers and name.lower() not in excluded
        )
    forwarded[binding.credential_header] = f"{binding.credential_prefix}{binding.secret}"
    forwarded["Host"] = _upstream_host_header(upstream)
    return forwarded


def _upstream_host_header(upstream: URL) -> str:
    host = upstream.raw_host or ""
    if ":" in host:
        host = f"[{host}]"
    port = upstream.explicit_port
    return f"{host}:{port}" if port is not None else host


def _response_headers(
    headers: Iterable[tuple[str, str]],
    *,
    credential_header: str,
) -> CIMultiDict[str]:
    items = list(headers)
    connection_headers = {
        token.strip().lower()
        for name, value in items
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    excluded = STRIPPED_RESPONSE_HEADERS | connection_headers | {credential_header.lower()}
    return CIMultiDict((name, value) for name, value in items if name.lower() not in excluded)


def _audit(
    request_id: str,
    route: str,
    method: str,
    status: int,
    request_bytes: int,
    started: float,
) -> None:
    print(
        json.dumps(
            {
                "event": "credential_broker_request",
                "request_id": request_id,
                "route": route,
                "method": method,
                "status": status,
                "request_bytes": request_bytes,
                "duration_ms": round((time.monotonic() - started) * 1000),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def _legacy_openai_registry(api_key: str) -> BindingRegistry:
    payload = {
        "version": LEGACY_REGISTRY_VERSION,
        "bindings": [
            {
                "binding_id": "0" * 64,
                "kind": "openai_responses",
                "secret": base64.b64encode(api_key.encode("ascii")).decode("ascii"),
                "allowed_local_ips": ["127.0.0.1", "::1"],
                "limits": {
                    "max_concurrent_requests": _env_positive_int("SOVEREN_BROKER_MAX_CONCURRENT", 2),
                    "requests_per_minute": _env_positive_int("SOVEREN_BROKER_REQUESTS_PER_MINUTE", 120),
                    "max_request_bytes": _env_positive_int(
                        "SOVEREN_BROKER_MAX_REQUEST_BYTES",
                        32 * 1024 * 1024,
                    ),
                    "queue_timeout_s": _env_positive_float("SOVEREN_BROKER_QUEUE_TIMEOUT_S", 5.0),
                    "request_read_timeout_s": _env_positive_float(
                        "SOVEREN_BROKER_REQUEST_READ_TIMEOUT_S",
                        15.0,
                    ),
                },
                "allowed_models": list(_allowed_models_from_env()),
            }
        ],
    }
    return _parse_registry(json.dumps(payload).encode("utf-8"))


def create_app(
    api_key: str | None = None,
    *,
    registry_payload: bytes | None = None,
    admin_socket_path: Path | None = None,
    max_inflight_requests: int | None = None,
    max_buffered_request_bytes: int | None = None,
    max_inflight_requests_per_tenant: int | None = None,
    max_buffered_request_bytes_per_tenant: int | None = None,
) -> web.Application:
    if api_key is not None and registry_payload is not None:
        raise ValueError("provide either api_key or registry_payload, not both")
    if api_key is not None:
        registry = _legacy_openai_registry(api_key)
    elif registry_payload is not None:
        registry = _parse_registry(registry_payload)
    else:
        registry = BindingRegistry(by_tenant={}, tenant_by_local_ip={})
    broker = CredentialBroker(
        initial_registry=registry,
        admin_socket_path=admin_socket_path,
        max_inflight_requests=max_inflight_requests,
        max_buffered_request_bytes=max_buffered_request_bytes,
        max_inflight_requests_per_tenant=max_inflight_requests_per_tenant,
        max_buffered_request_bytes_per_tenant=max_buffered_request_bytes_per_tenant,
    )
    app = web.Application(client_max_size=MAX_REQUEST_BYTES)
    app.on_startup.append(broker.start)
    app.on_cleanup.append(broker.stop)
    app.router.add_get("/healthz", broker.health)
    app.router.add_post("/v1/responses", broker.forward_openai)
    app.router.add_post("/v1/responses/compact", broker.forward_openai)
    app.router.add_route("*", "/bindings/{capability}", broker.forward_binding)
    app.router.add_route("*", "/bindings/{capability}/{tail:.*}", broker.forward_binding)
    return app


async def _send_admin_payload(path: Path, payload: bytes) -> dict[str, Any]:
    if not payload or len(payload) > MAX_ADMIN_PAYLOAD_BYTES:
        raise ValueError("credential registry payload size is invalid")
    reader, writer = await asyncio.open_unix_connection(str(path))
    try:
        writer.write(struct.pack("!I", len(payload)) + payload)
        await writer.drain()
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        if size < 1 or size > 4096:
            raise RuntimeError("credential broker admin response is invalid")
        response = json.loads(await reader.readexactly(size))
        if not isinstance(response, dict):
            raise RuntimeError("credential broker admin response is invalid")
        return response
    finally:
        writer.close()
        await writer.wait_closed()


async def _admin_main() -> None:
    payload = await asyncio.to_thread(sys.stdin.buffer.read, MAX_ADMIN_PAYLOAD_BYTES + 1)
    response = await _send_admin_payload(ADMIN_SOCKET_PATH, payload)
    if response.get("ok") is not True:
        raise RuntimeError("credential registry update failed")


def main() -> None:
    if sys.argv[1:] == ["admin"]:
        asyncio.run(_admin_main())
        return
    if sys.argv[1:]:
        raise SystemExit("unsupported credential broker command")
    app = create_app(admin_socket_path=ADMIN_SOCKET_PATH)
    web.run_app(app, host="0.0.0.0", port=8080, access_log=None, print=None)


if __name__ == "__main__":
    main()
