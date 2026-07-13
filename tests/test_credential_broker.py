import asyncio
import gzip
import importlib.util
import json
import sys
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

BROKER_PATH = Path(__file__).parents[1] / "deploy" / "sandbox" / "credential_broker.py"
BROKER_SPEC = importlib.util.spec_from_file_location("soveren_test_credential_broker", BROKER_PATH)
assert BROKER_SPEC is not None and BROKER_SPEC.loader is not None
credential_broker = importlib.util.module_from_spec(BROKER_SPEC)
sys.modules[BROKER_SPEC.name] = credential_broker
BROKER_SPEC.loader.exec_module(credential_broker)


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
        upstream_app.router.add_post("/v1/responses/compact", upstream_handler)
        upstream_server = TestServer(upstream_app)
        await upstream_server.start_server()
        monkeypatch.setitem(
            credential_broker.UPSTREAM,
            "/v1/responses",
            str(upstream_server.make_url("/v1/responses")),
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
            "host": "api.openai.com",
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


def test_credential_broker_consumes_and_removes_tmpfs_key(tmp_path, monkeypatch):
    key_path = tmp_path / "openai-api-key"
    key_path.write_bytes(b"sk-real-secret")
    monkeypatch.setattr(credential_broker, "KEY_PATH", key_path)

    assert credential_broker._load_api_key() == "sk-real-secret"
    assert not key_path.exists()
