"""Offline contract checks against the packaged Codex binary, with synthetic OAuth tokens."""

import asyncio
import base64
import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from soveren_agent_platform.sessions import CodexAppServerBackend, CodexTurnFailure, OpenSpec
from soveren_agent_platform.sessions.backends.codex_app_server import JsonRpcStdioClient


@pytest.mark.parametrize("scenario", ["direct403", "refresh403", "transient502", "transient503", "refresh_success"])
def test_native_auth_and_transient_contract(tmp_path, scenario):
    refresh = scenario in {"refresh403", "refresh_success"}
    transient = scenario.startswith("transient")
    binary = os.environ.get("SOVEREN_TEST_CODEX_BINARY")
    if not binary:
        pytest.skip("requires the explicitly selected packaged Codex app-server binary")
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self.respond(200, {"models": []})

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append(self.path)
            if self.path == "/refresh" and scenario == "refresh_success":
                self.respond(200, {"access_token": "fixture-renewed", "refresh_token": "fixture-renewed-refresh"})
                return
            if transient or scenario == "refresh_success" and "/refresh" in calls:
                if transient and len(calls) == 1:
                    self.respond(int(scenario[-3:]), {"error": {"message": "temporarily unavailable"}})
                    return
                events = [
                    {"type": "response.created", "response": {"id": "fixture-response"}},
                    {
                        "type": "response.output_item.added",
                        "output_index": 0,
                        "item": {"type": "message", "id": "fixture-message", "role": "assistant", "content": []},
                    },
                    {
                        "type": "response.output_text.delta",
                        "item_id": "fixture-message",
                        "output_index": 0,
                        "content_index": 0,
                        "delta": "ok",
                    },
                    {
                        "type": "response.output_item.done",
                        "output_index": 0,
                        "item": {
                            "type": "message",
                            "id": "fixture-message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "ok"}],
                        },
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "fixture-response",
                            "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                        },
                    },
                ]
                data = "".join("data: " + json.dumps(event) + "\n\n" for event in events).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if self.path == "/refresh" or not refresh:
                self.respond(
                    403,
                    {
                        "error": {
                            "code": "unsupported_country_region_territory",
                            "message": "Country, region, or territory not supported",
                        }
                    },
                )
            else:
                self.respond(401, {"error": {"code": "token_expired", "message": "Token expired"}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"

    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    jwt = ".".join(
        (
            encode({"alg": "none", "typ": "JWT"}),
            encode(
                {
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "fixture-account",
                        "chatgpt_plan_type": "pro",
                    },
                }
            ),
            "c2ln",
        )
    )
    (tmp_path / "auth.json").write_text(
        json.dumps(
            {
                "OPENAI_API_KEY": None,
                "tokens": {
                    "id_token": jwt,
                    "access_token": "fixture-access",
                    "refresh_token": "fixture-refresh",
                    "account_id": "fixture-account",
                },
                "last_refresh": datetime.now(timezone.utc).isoformat(),
            }
        )
    )
    # A complete test-owned provider sends every auth/model request to the
    # local stand-in. This does not change the platform's production provider.
    (tmp_path / "config.toml").write_text(
        'model_provider="fixture"\n[model_providers.fixture]\nname="OpenAI"\n'
        f'base_url={json.dumps(url + "/v1")}\nwire_api="responses"\n'
        "requires_openai_auth=true\nsupports_websockets=false\nrequest_max_retries=4\nstream_max_retries=5\n"
    )

    async def run():
        client = JsonRpcStdioClient(
            command=[binary, "app-server", "--listen", "stdio://"],
            cwd=None,
            env={
                "PATH": os.environ["PATH"],
                "CODEX_HOME": str(tmp_path),
                "CODEX_REFRESH_TOKEN_URL_OVERRIDE": url + "/refresh",
            },
            request_timeout_s=15,
        )
        notifications = []
        interrupt_calls = []
        original_notification = client._handle_notification
        original_request = client.request

        def notification(message):
            if message.get("method") == "error":
                notifications.append(message["params"])
            original_notification(message)

        async def request(method, params):
            if method == "turn/interrupt":
                interrupt_calls.append(params)
            return await original_request(method, params)

        client._handle_notification = notification
        client.request = request
        try:
            await client.request(
                "initialize",
                {
                    "clientInfo": {"name": "offline-error-contract", "version": "1"},
                    "capabilities": {"experimentalApi": True},
                },
            )
            backend = CodexAppServerBackend(client=client, model="gpt-5.4", turn_timeout_s=15)
            opened = await backend.open(OpenSpec(kind="codex", cwd=str(tmp_path)))
            await backend.send(opened.backend_session_id, "Say ok, no tools")
            if transient or scenario == "refresh_success":
                captured = await backend.capture(opened.backend_session_id)
                assert not captured.timed_out
                assert captured.text == "ok"
                assert interrupt_calls == []
                await backend.close(opened.backend_session_id)
                return
            with pytest.raises(CodexTurnFailure) as caught:
                await backend.capture(opened.backend_session_id)
            assert caught.value.http_status_code == 403
            assert caught.value.reason == "authentication_failed"
            assert len(interrupt_calls) == 1
            assert notifications[0]["willRetry"] is True
            info = notifications[0]["error"]["codexErrorInfo"]
            assert info == {"responseStreamDisconnected": {"httpStatusCode": None if refresh else 403}}
            if refresh:
                assert notifications[0]["error"]["additionalDetails"].startswith(
                    "Failed to refresh token: 403 Forbidden: "
                )
            await backend.close(opened.backend_session_id)
        finally:
            await client.close()

    try:
        asyncio.run(run())
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    # Native auth recovery reloads the same cache once before refreshing once.
    if transient:
        assert calls == ["/v1/responses", "/v1/responses"]
    elif scenario == "refresh_success":
        assert calls == ["/v1/responses", "/v1/responses", "/refresh", "/v1/responses"]
    else:
        assert calls == (["/v1/responses", "/v1/responses", "/refresh"] if refresh else ["/v1/responses"])
