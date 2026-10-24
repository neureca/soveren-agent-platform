"""Pinned native protocol proof with synthetic code and a loopback Responses provider."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import ToolExecutionContext
from soveren_agent_platform.sessions import (
    CodexAppServerBackend,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    OpenSpec,
)


def _events(output, response_id):
    for index, item in enumerate(output):
        if item["type"] == "function_call":
            initial = {**item, "arguments": "", "status": "in_progress"}
            yield {"type": "response.output_item.added", "output_index": index, "item": initial}
            yield {"type": "response.function_call_arguments.delta", "item_id": item["id"],
                   "output_index": index, "delta": item["arguments"]}
            yield {"type": "response.function_call_arguments.done", "item_id": item["id"],
                   "output_index": index, "arguments": item["arguments"]}
        else:
            initial = {**item, "content": [], "status": "in_progress"}
            yield {"type": "response.output_item.added", "output_index": index, "item": initial}
            yield {"type": "response.content_part.added", "item_id": item["id"], "output_index": index,
                   "content_index": 0, "part": {"type": "output_text", "text": ""}}
            yield {"type": "response.output_text.delta", "item_id": item["id"], "output_index": index,
                   "content_index": 0, "delta": item["content"][0]["text"]}
            yield {"type": "response.output_text.done", "item_id": item["id"], "output_index": index,
                   "content_index": 0, "text": item["content"][0]["text"]}
            yield {"type": "response.content_part.done", "item_id": item["id"], "output_index": index,
                   "content_index": 0, "part": item["content"][0]}
        yield {"type": "response.output_item.done", "output_index": index, "item": item}
    yield {"type": "response.completed", "response": {
        "id": response_id, "object": "response", "created_at": int(time.time()), "status": "completed",
        "model": "continuation-probe", "output": output,
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
    }}


@pytest.mark.skipif(not os.environ.get("SOVEREN_TEST_CODEX_BINARY"), reason="requires pinned native Codex binary")
def test_native_restart_continuation_preserves_completed_tools_and_trusted_callbacks(tmp_path):
    requests = []
    waiting_after_read = threading.Event()
    release_waiter = threading.Event()
    errors = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            number = len(requests)
            if number == 2:
                waiting_after_read.set()
                release_waiter.wait(30)
            if number in (1, 3):
                name = "read_first" if number == 1 else "read_second"
                namespace = next((tool for tool in body.get("tools", []) if tool.get("name") == "probe"), {})
                if name not in {tool.get("name") for tool in namespace.get("tools", [])}:
                    errors.append(f"Missing persisted dynamic tool: {name}")
                    self.send_error(500)
                    return
                output = [{"type": "function_call", "id": f"fc_{number}", "call_id": f"call_{number}",
                           "namespace": "probe", "name": name, "arguments": "{}", "status": "completed"}]
            else:
                output = [{"id": f"msg_{number}", "type": "message", "role": "assistant", "status": "completed",
                           "content": [{"type": "output_text", "text": "continued-final"}]}]
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                for event in _events(output, f"response_{number}"):
                    self.wfile.write(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n".encode())
                    self.wfile.flush()
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                # The first process deliberately cancels its blocked provider stream.
                if number != 2:
                    raise

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(
        'model = "continuation-probe"\nmodel_provider = "probe"\n'
        '[model_providers.probe]\nname = "probe"\n'
        f'base_url = "http://127.0.0.1:{server.server_address[1]}/v1"\n'
        'wire_api = "responses"\nrequires_openai_auth = false\nsupports_websockets = false\n'
    )
    scope = ConversationScope("tenant-a", "chat-a")
    execution = ToolExecutionContext(scope, "execution-a", "task-a")
    allowlist = (("probe", "read_first"), ("probe", "read_second"))
    calls = []

    def backend():
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)

        async def read(call):
            assert call.execution_context == execution
            assert call.namespace == "probe"
            calls.append((call.thread_id, call.turn_id, call.tool))
            return DynamicToolResult.text("first-file-result" if call.tool == "read_first" else "second-file-result")

        for name in ("read_first", "read_second"):
            registry.register(DynamicToolSpec(name, "Synthetic code read", {"type": "object", "properties": {}},
                                             namespace="probe"), read)
        return CodexAppServerBackend(
            command=[os.environ["SOVEREN_TEST_CODEX_BINARY"], "app-server", "--listen", "stdio://"],
            codex_home=codex_home, model="continuation-probe", dynamic_tools=registry,
            request_timeout_s=10, turn_timeout_s=15,
        )

    async def run():
        first, second = backend(), None
        spec = OpenSpec("codex_cli", str(tmp_path / "work"), conversation_scope=scope,
                        dynamic_tool_allowlist=allowlist)
        try:
            opened = await first.open(spec)
            receipt = await first.send_with_context(
                opened.backend_session_id, "ORIGINAL_CODE_ANALYSIS_REQUEST", execution_context=execution,
                output_schema=None, dynamic_tool_allowlist=allowlist,
            )
            assert await asyncio.to_thread(waiting_after_read.wait, 20), errors
            paused = await first.pause_delivery(opened.backend_session_id, receipt)
            assert paused.status == "interrupted"
            release_waiter.set()
            await first.shutdown()
            second = backend()
            resumed = await second.resume(spec, opened.backend_session_id)
            assert resumed.backend_session_id == opened.backend_session_id
            continued = await second.send_with_context(
                resumed.backend_session_id, "CONTINUE_FROM_PRIOR_TOOL_RESULT", execution_context=execution,
                output_schema=None, dynamic_tool_allowlist=allowlist,
            )
            result = await second.capture_delivery_with_watchdog(
                resumed.backend_session_id, continued, timeout_s=15, idle_timeout_s=10,
            )
            assert not result.timed_out
            assert result.text == "continued-final"
            assert [call[2] for call in calls] == ["read_first", "read_second"]
            assert {call[0] for call in calls} == {opened.backend_session_id}
            assert calls[0][1] != calls[1][1]
            resumed_input = json.dumps(requests[2]["input"])
            assert "first-file-result" in resumed_input
            assert resumed_input.count("ORIGINAL_CODE_ANALYSIS_REQUEST") == 1
            assert resumed_input.count("CONTINUE_FROM_PRIOR_TOOL_RESULT") == 1
            assert not errors
            await second.close(resumed.backend_session_id)
        finally:
            release_waiter.set()
            await first.shutdown()
            if second is not None:
                await second.shutdown()

    try:
        asyncio.run(asyncio.wait_for(run(), timeout=60))
    finally:
        release_waiter.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
