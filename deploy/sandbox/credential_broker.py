"""Narrow OpenAI Responses API credential broker for tenant sandboxes."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from collections import deque
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector, web
from multidict import CIMultiDict

KEY_PATH = Path("/run/soveren/openai-api-key")
UPSTREAM = {
    "/v1/responses": "https://api.openai.com/v1/responses",
    "/v1/responses/compact": "https://api.openai.com/v1/responses/compact",
}
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
    "x-api-key",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
}
STRIPPED_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {
    "content-length",
    "set-cookie",
}


def _positive_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _allowed_models() -> frozenset[str]:
    raw = json.loads(os.environ.get("SOVEREN_BROKER_ALLOWED_MODELS", "[]"))
    if not isinstance(raw, list) or any(not isinstance(model, str) or not model for model in raw):
        raise ValueError("SOVEREN_BROKER_ALLOWED_MODELS must be a JSON array of model names")
    return frozenset(raw)


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


def _load_api_key() -> str:
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        try:
            raw = KEY_PATH.read_bytes()
        except FileNotFoundError:
            time.sleep(0.1)
            continue
        KEY_PATH.unlink(missing_ok=True)
        try:
            value = raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("broker API key must be ASCII") from exc
        invalid_character = any(ord(character) < 33 or ord(character) > 126 for character in value)
        if not value or value != value.strip() or invalid_character:
            raise ValueError("broker API key contains invalid characters")
        return value
    raise TimeoutError("broker API key was not provisioned")


class RateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self._limit = requests_per_minute
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        now = time.monotonic()
        async with self._lock:
            cutoff = now - 60.0
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._limit:
                return False
            self._timestamps.append(now)
            return True


class CredentialBroker:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._egress_proxy = _egress_proxy()
        self._max_request_bytes = _positive_int("SOVEREN_BROKER_MAX_REQUEST_BYTES", 32 * 1024 * 1024)
        self._queue_timeout_s = _positive_float("SOVEREN_BROKER_QUEUE_TIMEOUT_S", 5.0)
        self._allowed_models = _allowed_models()
        self._slots = asyncio.Semaphore(_positive_int("SOVEREN_BROKER_MAX_CONCURRENT", 2))
        self._rate_limiter = RateLimiter(_positive_int("SOVEREN_BROKER_REQUESTS_PER_MINUTE", 120))
        self._session: ClientSession | None = None

    async def start(self, app: web.Application) -> None:
        timeout = ClientTimeout(total=None, connect=15.0, sock_connect=15.0, sock_read=310.0)
        self._session = ClientSession(
            timeout=timeout,
            connector=TCPConnector(limit=8, ttl_dns_cache=60),
            auto_decompress=False,
            skip_auto_headers={"Accept-Encoding", "User-Agent"},
            trust_env=False,
        )

    async def stop(self, app: web.Application) -> None:
        session = self._session
        self._session = None
        if session is not None:
            await session.close()

    async def health(self, request: web.Request) -> web.Response:
        return web.Response(status=204)

    async def forward(self, request: web.Request) -> web.StreamResponse:
        started = time.monotonic()
        request_id = secrets.token_hex(8)
        status = 500
        request_bytes = 0
        acquired = False
        try:
            if request.query_string:
                raise web.HTTPBadRequest(text="query parameters are not supported")
            if request.headers.get("Content-Encoding"):
                raise web.HTTPUnsupportedMediaType(text="compressed requests are not supported")
            if not await self._rate_limiter.allow():
                raise web.HTTPTooManyRequests(text="tenant request rate exceeded")
            try:
                async with asyncio.timeout(self._queue_timeout_s):
                    await self._slots.acquire()
            except TimeoutError as exc:
                raise web.HTTPTooManyRequests(text="tenant concurrency limit exceeded") from exc
            acquired = True
            body = await request.read()
            request_bytes = len(body)
            if request_bytes > self._max_request_bytes:
                raise web.HTTPRequestEntityTooLarge(
                    max_size=self._max_request_bytes,
                    actual_size=request_bytes,
                )
            self._enforce_model_policy(body)
            upstream = UPSTREAM[request.path]
            session = self._session
            if session is None:
                raise web.HTTPServiceUnavailable(text="credential broker is starting")
            headers = _request_headers(request.headers.items(), self._api_key)
            try:
                async with session.post(
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
                        headers=_response_headers(upstream_response.headers.items()),
                    )
                    await response.prepare(request)
                    async for chunk in upstream_response.content.iter_any():
                        await response.write(chunk)
                    await response.write_eof()
                    return response
            except ClientError as exc:
                status = 502
                raise web.HTTPBadGateway(text="model provider unavailable") from exc
        except web.HTTPException as exc:
            status = exc.status
            raise
        finally:
            if acquired:
                self._slots.release()
            _audit(request_id, request.path, status, request_bytes, started)

    def _enforce_model_policy(self, body: bytes) -> None:
        if not self._allowed_models:
            return
        try:
            payload: Any = json.loads(body)
        except json.JSONDecodeError as exc:
            raise web.HTTPBadRequest(text="request body must be JSON") from exc
        model = payload.get("model") if isinstance(payload, dict) else None
        if not isinstance(model, str) or model not in self._allowed_models:
            raise web.HTTPForbidden(text="model is not allowed for this tenant")


def _request_headers(headers: Iterable[tuple[str, str]], api_key: str) -> CIMultiDict[str]:
    items = list(headers)
    connection_headers = {
        token.strip().lower()
        for name, value in items
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    excluded = STRIPPED_REQUEST_HEADERS | connection_headers
    forwarded = CIMultiDict(
        (name, value) for name, value in items if name.lower() not in excluded
    )
    forwarded["Authorization"] = f"Bearer {api_key}"
    forwarded["Host"] = "api.openai.com"
    return forwarded


def _response_headers(headers: Iterable[tuple[str, str]]) -> CIMultiDict[str]:
    items = list(headers)
    connection_headers = {
        token.strip().lower()
        for name, value in items
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    excluded = STRIPPED_RESPONSE_HEADERS | connection_headers
    return CIMultiDict(
        (name, value) for name, value in items if name.lower() not in excluded
    )


def _audit(request_id: str, path: str, status: int, request_bytes: int, started: float) -> None:
    print(
        json.dumps(
            {
                "event": "credential_broker_request",
                "request_id": request_id,
                "path": path,
                "status": status,
                "request_bytes": request_bytes,
                "duration_ms": round((time.monotonic() - started) * 1000),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def create_app(api_key: str) -> web.Application:
    broker = CredentialBroker(api_key)
    app = web.Application(client_max_size=broker._max_request_bytes)
    app.on_startup.append(broker.start)
    app.on_cleanup.append(broker.stop)
    app.router.add_get("/healthz", broker.health)
    app.router.add_post("/v1/responses", broker.forward)
    app.router.add_post("/v1/responses/compact", broker.forward)
    return app


def main() -> None:
    app = create_app(_load_api_key())
    web.run_app(app, host="0.0.0.0", port=8080, access_log=None, print=None)


if __name__ == "__main__":
    main()
