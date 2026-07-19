import asyncio
import base64
import gzip
import importlib.util
import json
import stat
import sys
from pathlib import Path

import pytest
from aiohttp import ClientResponse, web
from aiohttp.test_utils import TestClient, TestServer

BROKER_PATH = Path(__file__).parents[1] / "deploy" / "sandbox" / "credential_broker.py"
BROKER_SPEC = importlib.util.spec_from_file_location("soveren_test_credential_broker", BROKER_PATH)
assert BROKER_SPEC is not None and BROKER_SPEC.loader is not None
credential_broker = importlib.util.module_from_spec(BROKER_SPEC)
sys.modules[BROKER_SPEC.name] = credential_broker
BROKER_SPEC.loader.exec_module(credential_broker)


def _limits(**overrides: int | float) -> dict[str, int | float]:
    values: dict[str, int | float] = {
        "max_concurrent_requests": 2,
        "requests_per_minute": 120,
        "max_request_bytes": 32 * 1024 * 1024,
        "queue_timeout_s": 5.0,
        "request_read_timeout_s": 15.0,
        "response_timeout_s": 300.0,
    }
    values.update(overrides)
    return values


def _http_registry(
    *,
    secret: str,
    capability: str,
    target_origin: str,
    allowed_local_ips: list[str] | None = None,
    binding_id: str = "1" * 64,
    limits: dict[str, int | float] | None = None,
    tenant_key: str = "a" * 64,
) -> bytes:
    return json.dumps(
        {
            "version": 2,
            "operation": "replace_tenant",
            "tenant_key": tenant_key,
            "bindings": [
                {
                    "binding_id": binding_id,
                    "kind": "http",
                    "secret": base64.b64encode(secret.encode("ascii")).decode("ascii"),
                    "allowed_local_ips": allowed_local_ips or ["127.0.0.1"],
                    "limits": limits or _limits(),
                    "capability": capability,
                    "target_origin": target_origin,
                    "credential_header": "X-Api-Key",
                    "credential_prefix": "",
                    "allowed_methods": ["GET", "POST"],
                    "allowed_path_prefixes": ["/repos"],
                    "allowed_request_headers": ["accept", "content-type", "user-agent"],
                }
            ],
        }
    ).encode()


def _remove_tenant(tenant_key: str = "a" * 64) -> bytes:
    return json.dumps(
        {
            "version": 2,
            "operation": "remove_tenant",
            "tenant_key": tenant_key,
        }
    ).encode()


def test_credential_broker_replaces_auth_and_only_forwards_responses_routes(monkeypatch, capsys):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)

    async def run() -> None:
        captured: dict[str, object] = {}

        async def upstream_handler(request: web.Request) -> web.Response:
            captured["authorization"] = request.headers.get("Authorization")
            captured["project"] = request.headers.get("OpenAI-Project")
            captured["x_api_key"] = request.headers.get("X-Api-Key")
            captured["connection_scoped"] = request.headers.get("X-Remove-Me")
            captured["host"] = request.headers.get("Host")
            captured["body"] = await request.json()
            return web.Response(
                body=b'data: {"type":"response.completed"}\n\n',
                headers={"Content-Type": "text/event-stream", "X-Request-Id": "upstream-1"},
            )

        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/responses", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        upstream_url = upstream_server.make_url("/v1/responses")
        monkeypatch.setitem(
            credential_broker.OPENAI_UPSTREAM,
            "/v1/responses",
            str(upstream_url),
        )

        client = TestClient(TestServer(credential_broker.create_app("sk-real-secret")))
        await client.start_server()
        try:
            response = await client.post(
                "/v1/responses",
                json={"model": "gpt-test", "input": "private prompt"},
                headers={
                    "Authorization": "Bearer attacker-value",
                    "OpenAI-Project": "attacker-project",
                    "X-Api-Key": "attacker-api-key",
                    "Connection": "X-Remove-Me",
                    "X-Remove-Me": "attacker-connection-value",
                },
            )
            assert response.status == 200
            assert await response.read() == b'data: {"type":"response.completed"}\n\n'
            assert response.headers["X-Request-Id"] == "upstream-1"
            assert (await client.get("/v1/responses")).status == 405
            assert (await client.post("/v1/responses?upstream=evil", json={})).status == 400
            assert (await client.post("/v1/files", json={})).status == 404
        finally:
            await client.close()
            await upstream_server.close()

        assert captured == {
            "authorization": "Bearer sk-real-secret",
            "project": None,
            "x_api_key": None,
            "connection_scoped": None,
            "host": f"{upstream_url.host}:{upstream_url.port}",
            "body": {"model": "gpt-test", "input": "private prompt"},
        }

    asyncio.run(run())

    audit = capsys.readouterr().out
    assert "credential_broker_request" in audit
    assert "private prompt" not in audit
    assert "sk-real-secret" not in audit
    assert "attacker-value" not in audit


def test_credential_broker_enforces_allowed_models(monkeypatch):
    monkeypatch.setenv("SOVEREN_BROKER_ALLOWED_MODELS", json.dumps(["gpt-allowed"]))
    monkeypatch.setenv("SOVEREN_BROKER_EGRESS_PROXY", "http://soveren-sandbox-egress:3128")

    async def run() -> None:
        client = TestClient(TestServer(credential_broker.create_app("sk-real-secret")))
        await client.start_server()
        try:
            denied = await client.post("/v1/responses", json={"model": "gpt-denied", "input": "x"})
            assert denied.status == 403
            compressed = await client.post(
                "/v1/responses",
                data=gzip.compress(b"compressed"),
                headers={"Content-Encoding": "gzip"},
            )
            assert compressed.status == 415
        finally:
            await client.close()

    asyncio.run(run())


def test_credential_broker_times_out_slow_request_bodies_and_releases_slot(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setenv("SOVEREN_BROKER_ALLOWED_MODELS", json.dumps(["gpt-allowed"]))
    monkeypatch.setenv("SOVEREN_BROKER_MAX_CONCURRENT", "1")
    monkeypatch.setenv("SOVEREN_BROKER_QUEUE_TIMEOUT_S", "0.02")
    monkeypatch.setenv("SOVEREN_BROKER_REQUEST_READ_TIMEOUT_S", "0.05")

    async def run() -> None:
        client = TestClient(TestServer(credential_broker.create_app("sk-real-secret")))
        await client.start_server()
        reader, writer = await asyncio.open_connection(client.server.host, client.server.port)
        try:
            writer.write(
                b"POST /v1/responses HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: 1000\r\n"
                b"Connection: close\r\n\r\n{"
            )
            await writer.drain()
            headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=1)
            assert headers.startswith(b"HTTP/1.1 408")

            denied = await client.post(
                "/v1/responses",
                json={"model": "gpt-denied", "input": "x"},
            )
            assert denied.status == 403
        finally:
            writer.close()
            await writer.wait_closed()
            await client.close()

    asyncio.run(run())


def test_credential_broker_bounds_chunked_body_while_reading(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    payload = json.loads(
        _http_registry(
            secret="protected-secret",
            capability=capability,
            target_origin="https://api.example.com",
        )
    )
    payload["bindings"][0]["limits"]["max_request_bytes"] = 4

    async def chunks():
        yield b"123"
        yield b"45"

    async def run() -> None:
        client = TestClient(
            TestServer(
                credential_broker.create_app(
                    registry_payload=json.dumps(payload).encode(),
                )
            )
        )
        await client.start_server()
        try:
            response = await client.post(
                f"/bindings/{capability}/repos/x",
                data=chunks(),
            )
            assert response.status == 413
        finally:
            await client.close()

    asyncio.run(run())


def test_http_binding_fixes_target_policy_and_injected_credential(monkeypatch, capsys):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"

    async def run() -> None:
        captured: list[dict[str, object]] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            captured.append(
                {
                    "path": request.path_qs,
                    "api_key": request.headers.get("X-Api-Key"),
                    "authorization": request.headers.get("Authorization"),
                    "cookie": request.headers.get("Cookie"),
                    "leak": request.headers.get("X-Leak"),
                    "content_type": request.headers.get("Content-Type"),
                    "body": await request.json(),
                }
            )
            return web.json_response(
                {"ok": True},
                headers={"X-Api-Key": "protected-secret"},
            )

        upstream_app = web.Application()
        upstream_app.router.add_post("/repos/{tail:.*}", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        target_origin = str(upstream_server.make_url("/"))
        monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)

        app = credential_broker.create_app(
            registry_payload=_http_registry(
                secret="protected-secret",
                capability=capability,
                target_origin=target_origin,
            )
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.post(
                f"/bindings/{capability}/repos/neureca/project?state=open",
                json={"private": "request body"},
                headers={
                    "Authorization": "Bearer attacker",
                    "Cookie": "session=attacker",
                    "X-Api-Key": "attacker-key",
                    "X-Leak": "attacker-header",
                },
            )
            assert response.status == 200
            assert await response.json() == {"ok": True}
            assert "X-Api-Key" not in response.headers
            assert (await client.post(f"/bindings/{capability}/admin", json={})).status == 403
            assert (await client.delete(f"/bindings/{capability}/repos/x")).status == 405
            assert (await client.get("/bindings/unknown-capability/repos/x")).status == 404
        finally:
            await client.close()
            await upstream_server.close()

        assert captured == [
            {
                "path": "/repos/neureca/project?state=open",
                "api_key": "protected-secret",
                "authorization": None,
                "cookie": None,
                "leak": None,
                "content_type": "application/json",
                "body": {"private": "request body"},
            }
        ]

    asyncio.run(run())

    audit = capsys.readouterr().out
    assert "http_binding" in audit
    assert capability not in audit
    assert "protected-secret" not in audit
    assert "request body" not in audit


def test_http_binding_rejects_another_conversation_network(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"

    async def run() -> None:
        client = TestClient(
            TestServer(
                credential_broker.create_app(
                    registry_payload=_http_registry(
                        secret="protected-secret",
                        capability=capability,
                        target_origin="https://api.example.com",
                        allowed_local_ips=["192.0.2.10"],
                    )
                )
            )
        )
        await client.start_server()
        try:
            response = await client.get(f"/bindings/{capability}/repos/x")
            assert response.status == 403
        finally:
            await client.close()

    asyncio.run(run())


def test_http_binding_does_not_follow_upstream_redirects(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"

    async def run() -> None:
        redirected = False

        async def redirect_handler(request: web.Request) -> web.Response:
            raise web.HTTPFound("/captured")

        async def captured_handler(request: web.Request) -> web.Response:
            nonlocal redirected
            redirected = True
            return web.Response()

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/redirect", redirect_handler)
        upstream_app.router.add_get("/captured", captured_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
        client = TestClient(
            TestServer(
                credential_broker.create_app(
                    registry_payload=_http_registry(
                        secret="protected-secret",
                        capability=capability,
                        target_origin=str(upstream_server.make_url("/")),
                    )
                )
            )
        )
        await client.start_server()
        try:
            response = await client.get(
                f"/bindings/{capability}/repos/redirect",
                allow_redirects=False,
            )
            assert response.status == 302
            assert not redirected
        finally:
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_admin_registry_rotation_and_revocation_are_atomic(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        received_keys: list[str | None] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_keys.append(request.headers.get("X-Api-Key"))
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        target_origin = str(upstream_server.make_url("/"))
        monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)

        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        try:
            assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
            first = _http_registry(
                secret="first-secret",
                capability=capability,
                target_origin=target_origin,
            )
            assert await credential_broker._send_admin_payload(socket_path, first) == {"ok": True}
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            assert await credential_broker._send_admin_payload(socket_path, b"{}") == {
                "ok": False,
                "error": "credential registry update failed",
            }
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            rotated = _http_registry(
                secret="second-secret",
                capability=capability,
                target_origin=target_origin,
            )
            assert await credential_broker._send_admin_payload(socket_path, rotated) == {"ok": True}
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            empty = _remove_tenant()
            assert await credential_broker._send_admin_payload(socket_path, empty) == {"ok": True}
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 403
        finally:
            await client.close()
            await upstream_server.close()

        assert received_keys == ["first-secret", "first-secret", "second-secret"]
        assert not socket_path.exists()

    asyncio.run(run())


def test_shared_broker_resolves_tenants_by_network_and_updates_them_independently(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "shared_capability_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"
    local_ip = {"value": "192.0.2.10"}
    monkeypatch.setattr(credential_broker, "_request_local_ip", lambda request: local_ip["value"])

    async def run() -> None:
        received_keys: list[str | None] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_keys.append(request.headers.get("X-Api-Key"))
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        target_origin = str(upstream_server.make_url("/"))
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        try:
            tenant_a = _http_registry(
                secret="tenant-a-secret",
                capability=capability,
                target_origin=target_origin,
                allowed_local_ips=["192.0.2.10"],
                binding_id="1" * 64,
                tenant_key="a" * 64,
            )
            tenant_b = _http_registry(
                secret="tenant-b-secret",
                capability=capability,
                target_origin=target_origin,
                allowed_local_ips=["192.0.2.20"],
                binding_id="2" * 64,
                tenant_key="b" * 64,
            )
            assert await credential_broker._send_admin_payload(socket_path, tenant_a) == {"ok": True}
            assert await credential_broker._send_admin_payload(socket_path, tenant_b) == {"ok": True}

            assert (
                await client.get(
                    f"/bindings/{capability}/repos/x",
                    headers={"X-Soveren-Tenant": "b" * 64},
                )
            ).status == 204
            local_ip["value"] = "192.0.2.20"
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            conflicting = _http_registry(
                secret="tenant-c-secret",
                capability=capability,
                target_origin=target_origin,
                allowed_local_ips=["192.0.2.20"],
                binding_id="3" * 64,
                tenant_key="c" * 64,
            )
            assert await credential_broker._send_admin_payload(socket_path, conflicting) == {
                "ok": False,
                "error": "credential registry update failed",
            }
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            assert await credential_broker._send_admin_payload(socket_path, _remove_tenant("a" * 64)) == {
                "ok": True
            }
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204
            local_ip["value"] = "192.0.2.10"
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 403
        finally:
            await client.close()
            await upstream_server.close()

        assert received_keys == [
            "tenant-a-secret",
            "tenant-b-secret",
            "tenant-b-secret",
            "tenant-b-secret",
        ]

    asyncio.run(run())


def test_revocation_rejects_a_request_that_has_not_been_admitted_upstream(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        body_read_started = asyncio.Event()
        finish_body_read = asyncio.Event()
        received_keys: list[str | None] = []
        original_read = credential_broker._read_bounded_body

        async def controlled_read(request, *, max_bytes):
            body_read_started.set()
            await finish_body_read.wait()
            return await original_read(request, max_bytes=max_bytes)

        async def upstream_handler(request: web.Request) -> web.Response:
            received_keys.append(request.headers.get("X-Api-Key"))
            return web.Response(status=204)

        monkeypatch.setattr(credential_broker, "_read_bounded_body", controlled_read)
        upstream_app = web.Application()
        upstream_app.router.add_post("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        request_task: asyncio.Task | None = None
        try:
            registry = _http_registry(
                secret="old-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
            )
            assert await credential_broker._send_admin_payload(socket_path, registry) == {"ok": True}
            request_task = asyncio.create_task(client.post(f"/bindings/{capability}/repos/x", data=b"body"))
            await asyncio.wait_for(body_read_started.wait(), timeout=1)

            empty = _remove_tenant()
            assert await credential_broker._send_admin_payload(socket_path, empty) == {"ok": True}
            finish_body_read.set()

            response = await asyncio.wait_for(request_task, timeout=1)
            assert response.status == 403
            await response.read()
            assert received_keys == []
        finally:
            finish_body_read.set()
            if request_task is not None and not request_task.done():
                request_task.cancel()
                await asyncio.gather(request_task, return_exceptions=True)
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_registry_rotation_preserves_binding_concurrency(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        first_arrived = asyncio.Event()
        release_first = asyncio.Event()
        received_keys: list[str | None] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_keys.append(request.headers.get("X-Api-Key"))
            first_arrived.set()
            await release_first.wait()
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        first_task: asyncio.Task | None = None
        try:
            limits = _limits(max_concurrent_requests=1, queue_timeout_s=0.05)
            first = _http_registry(
                secret="first-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=limits,
            )
            assert await credential_broker._send_admin_payload(socket_path, first) == {"ok": True}
            first_task = asyncio.create_task(client.get(f"/bindings/{capability}/repos/x"))
            await asyncio.wait_for(first_arrived.wait(), timeout=1)

            rotated = _http_registry(
                secret="second-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=limits,
            )
            assert await credential_broker._send_admin_payload(socket_path, rotated) == {"ok": True}
            denied = await client.get(f"/bindings/{capability}/repos/x")
            assert denied.status == 429
            await denied.read()
            assert received_keys == ["first-secret"]

            release_first.set()
            first_response = await asyncio.wait_for(first_task, timeout=1)
            assert first_response.status == 204
            await first_response.read()
        finally:
            release_first.set()
            if first_task is not None and not first_task.done():
                first_task.cancel()
                await asyncio.gather(first_task, return_exceptions=True)
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_response_timeout_terminates_slow_stream_and_releases_capacity(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        first_arrived = asyncio.Event()
        release_first = asyncio.Event()
        request_count = 0

        async def upstream_handler(request: web.Request) -> web.StreamResponse:
            nonlocal request_count
            request_count += 1
            if request_count > 1:
                return web.Response(status=204)
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(b"data: started\n\n")
            first_arrived.set()
            await release_first.wait()
            return response

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        first_response: ClientResponse | None = None
        try:
            registry = _http_registry(
                secret="secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=_limits(
                    max_concurrent_requests=1,
                    queue_timeout_s=0.5,
                    response_timeout_s=0.05,
                ),
            )
            assert await credential_broker._send_admin_payload(socket_path, registry) == {"ok": True}
            first_response = await client.get(f"/bindings/{capability}/repos/x")
            assert first_response.status == 200
            await asyncio.wait_for(first_arrived.wait(), timeout=1)
            await asyncio.sleep(0.1)

            second_response = await client.get(f"/bindings/{capability}/repos/x")
            assert second_response.status == 204
            await second_response.read()
        finally:
            release_first.set()
            if first_response is not None:
                first_response.close()
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_registry_rotation_tightens_concurrency_for_waiting_requests(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        arrived = {name: asyncio.Event() for name in ("one", "two", "three")}
        release = {name: asyncio.Event() for name in ("one", "two")}

        async def upstream_handler(request: web.Request) -> web.Response:
            name = request.match_info["name"]
            arrived[name].set()
            if name in release:
                await release[name].wait()
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/{name}", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        tasks: list[asyncio.Task] = []
        try:
            initial_limits = _limits(max_concurrent_requests=2, queue_timeout_s=0.15)
            initial = _http_registry(
                secret="first-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=initial_limits,
            )
            assert await credential_broker._send_admin_payload(socket_path, initial) == {"ok": True}
            tasks.extend(
                asyncio.create_task(client.get(f"/bindings/{capability}/repos/{name}")) for name in ("one", "two")
            )
            await asyncio.wait_for(
                asyncio.gather(arrived["one"].wait(), arrived["two"].wait()),
                timeout=1,
            )
            third_task = asyncio.create_task(client.get(f"/bindings/{capability}/repos/three"))
            tasks.append(third_task)
            await asyncio.sleep(0.01)

            tightened = _http_registry(
                secret="second-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=_limits(max_concurrent_requests=1, queue_timeout_s=0.15),
            )
            assert await credential_broker._send_admin_payload(socket_path, tightened) == {"ok": True}
            release["one"].set()

            third_response = await asyncio.wait_for(third_task, timeout=1)
            assert third_response.status == 429
            await third_response.read()
            assert not arrived["three"].is_set()

            release["two"].set()
            first_responses = await asyncio.gather(*tasks[:2])
            assert [response.status for response in first_responses] == [204, 204]
            await asyncio.gather(*(response.read() for response in first_responses))
        finally:
            for event in release.values():
                event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_registry_rotation_preserves_binding_rate_window(tmp_path, monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    capability = "capability_token_abcdefghijklmnopqrstuvwxyz012345"
    socket_path = tmp_path / "broker.sock"

    async def run() -> None:
        received_keys: list[str | None] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_keys.append(request.headers.get("X-Api-Key"))
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_get("/repos/x", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        client = TestClient(TestServer(credential_broker.create_app(admin_socket_path=socket_path)))
        await client.start_server()
        try:
            limits = _limits(requests_per_minute=1)
            first = _http_registry(
                secret="first-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=limits,
            )
            assert await credential_broker._send_admin_payload(socket_path, first) == {"ok": True}
            assert (await client.get(f"/bindings/{capability}/repos/x")).status == 204

            rotated = _http_registry(
                secret="second-secret",
                capability=capability,
                target_origin=str(upstream_server.make_url("/")),
                limits=limits,
            )
            assert await credential_broker._send_admin_payload(socket_path, rotated) == {"ok": True}
            denied = await client.get(f"/bindings/{capability}/repos/x")
            assert denied.status == 429
            await denied.read()
            assert received_keys == ["first-secret"]
        finally:
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_broker_enforces_one_buffer_budget_across_bindings(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    first_capability = "first_capability_abcdefghijklmnopqrstuvwxyz012345"
    second_capability = "second_capability_abcdefghijklmnopqrstuvwxyz01234"

    async def run() -> None:
        first_arrived = asyncio.Event()
        release_first = asyncio.Event()
        received_paths: list[str] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_paths.append(request.path)
            await request.read()
            first_arrived.set()
            await release_first.wait()
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_post("/repos/{tail:.*}", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        target_origin = str(upstream_server.make_url("/"))
        limits = _limits(max_request_bytes=4, queue_timeout_s=0.05)
        payload = json.loads(
            _http_registry(
                secret="first-secret",
                capability=first_capability,
                target_origin=target_origin,
                limits=limits,
            )
        )
        second = json.loads(
            _http_registry(
                secret="second-secret",
                capability=second_capability,
                target_origin=target_origin,
                binding_id="2" * 64,
                limits=limits,
            )
        )
        payload["bindings"].extend(second["bindings"])
        client = TestClient(
            TestServer(
                credential_broker.create_app(
                    registry_payload=json.dumps(payload).encode(),
                    max_inflight_requests=2,
                    max_buffered_request_bytes=4,
                )
            )
        )
        await client.start_server()
        first_task: asyncio.Task | None = None
        try:
            first_task = asyncio.create_task(client.post(f"/bindings/{first_capability}/repos/first", data=b"1234"))
            await asyncio.wait_for(first_arrived.wait(), timeout=1)
            denied = await client.post(
                f"/bindings/{second_capability}/repos/second",
                data=b"5678",
            )
            assert denied.status == 429
            await denied.read()
            assert received_paths == ["/repos/first"]

            release_first.set()
            first_response = await asyncio.wait_for(first_task, timeout=1)
            assert first_response.status == 204
            await first_response.read()
        finally:
            release_first.set()
            if first_task is not None and not first_task.done():
                first_task.cancel()
                await asyncio.gather(first_task, return_exceptions=True)
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


def test_shared_broker_enforces_tenant_capacity_across_bindings(monkeypatch):
    monkeypatch.setattr(credential_broker, "_egress_proxy", lambda: None)
    monkeypatch.setattr(credential_broker, "_normalize_https_origin", lambda value: value)
    first_capability = "first_tenant_capability_abcdefghijklmnopqrstuvwx012345"
    second_capability = "second_tenant_capability_abcdefghijklmnopqrstuvw012345"

    async def run() -> None:
        first_arrived = asyncio.Event()
        release_first = asyncio.Event()
        received_paths: list[str] = []

        async def upstream_handler(request: web.Request) -> web.Response:
            received_paths.append(request.path)
            first_arrived.set()
            await release_first.wait()
            return web.Response(status=204)

        upstream_app = web.Application()
        upstream_app.router.add_post("/repos/{tail:.*}", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        target_origin = str(upstream_server.make_url("/"))
        limits = _limits(max_request_bytes=4, queue_timeout_s=0.05)
        payload = json.loads(
            _http_registry(
                secret="first-secret",
                capability=first_capability,
                target_origin=target_origin,
                limits=limits,
            )
        )
        second = json.loads(
            _http_registry(
                secret="second-secret",
                capability=second_capability,
                target_origin=target_origin,
                binding_id="2" * 64,
                limits=limits,
            )
        )
        payload["bindings"].extend(second["bindings"])
        client = TestClient(
            TestServer(
                credential_broker.create_app(
                    registry_payload=json.dumps(payload).encode(),
                    max_inflight_requests=2,
                    max_buffered_request_bytes=8,
                    max_inflight_requests_per_tenant=1,
                    max_buffered_request_bytes_per_tenant=8,
                )
            )
        )
        await client.start_server()
        first_task: asyncio.Task | None = None
        try:
            first_task = asyncio.create_task(
                client.post(f"/bindings/{first_capability}/repos/first", data=b"1234")
            )
            await asyncio.wait_for(first_arrived.wait(), timeout=1)
            denied = await client.post(
                f"/bindings/{second_capability}/repos/second",
                data=b"5678",
            )
            assert denied.status == 429
            await denied.read()
            assert received_paths == ["/repos/first"]

            release_first.set()
            first_response = await asyncio.wait_for(first_task, timeout=1)
            assert first_response.status == 204
            await first_response.read()
        finally:
            release_first.set()
            if first_task is not None and not first_task.done():
                first_task.cancel()
                await asyncio.gather(first_task, return_exceptions=True)
            await client.close()
            await upstream_server.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "origin",
    [
        "http://api.example.com",
        "https://127.0.0.1",
        "https://169.254.169.254",
        "https://api.example.com:8443",
        "https://api.example.com/path",
        "https://user:pass@api.example.com",
    ],
)
def test_http_binding_rejects_unsafe_origins(origin):
    with pytest.raises(ValueError):
        credential_broker._normalize_https_origin(origin)


def test_registry_rejects_non_string_path_prefix_instead_of_silently_dropping_it():
    payload = json.loads(
        _http_registry(
            secret="protected-secret",
            capability="capability_token_abcdefghijklmnopqrstuvwxyz012345",
            target_origin="https://api.example.com",
        )
    )
    payload["bindings"][0]["allowed_path_prefixes"] = [42]

    with pytest.raises(ValueError, match="path prefixes"):
        credential_broker._parse_registry(json.dumps(payload).encode())
