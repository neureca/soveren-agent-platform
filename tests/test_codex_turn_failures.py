import asyncio
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.agent.worker import run_agent_worker
from soveren_agent_platform.llm.backends.session import SessionLlmBackend
from soveren_agent_platform.queue import durable
from soveren_agent_platform.runtime.planner import PlannerRuntimeConfig, run_planner_turn
from soveren_agent_platform.sessions import CodexAppServerBackend, CodexTurnFailure
from soveren_agent_platform.sessions.backends.codex_app_server import JsonRpcStdioClient, codex_turn_failure
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def error_frame(error, *, retry=True, turn_id="turn-1"):
    return {
        "method": "error",
        "params": {
            "threadId": "thread-1",
            "turnId": turn_id,
            "willRetry": retry,
            "error": error,
        },
    }


@pytest.mark.parametrize(
    "error",
    [
        {"message": "Reconnecting... 1/5", "codexErrorInfo": {"responseStreamDisconnected": {"httpStatusCode": 403}}},
        {
            "message": "Reconnecting... 2/5",
            "codexErrorInfo": {"responseStreamDisconnected": {"httpStatusCode": None}},
            "additionalDetails": "Failed to refresh token: 403 Forbidden: Country, region, or territory not supported",
        },
        {"message": "Login expired", "codexErrorInfo": "unauthorized"},
    ],
)
def test_terminal_auth_interrupts_once_and_capture_raises_typed_failure(error):
    async def run():
        client = JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)
        writes = []

        async def request(method, params):
            writes.append((method, params))
            return {}

        client.request = request
        state = client.set_last_turn("thread-1", "turn-1")
        for _ in range(5):
            client._handle_notification(error_frame(error))
        await asyncio.wait_for(state.done.wait(), 1)
        backend = CodexAppServerBackend(client=client)
        backend._loaded_thread_ids.add("thread-1")
        with pytest.raises(CodexTurnFailure) as caught:
            await backend.capture("thread-1")
        assert caught.value.reason == "authentication_failed"
        # A delayed notification after capture must not start a second interrupt.
        client._handle_notification(error_frame(error))
        await asyncio.sleep(0)
        assert writes == [("turn/interrupt", {"threadId": "thread-1", "turnId": "turn-1"})]
        assert client.last_turn("thread-1") is None
        client.release_thread("thread-1")
        assert client._failed_turn_by_thread == {}

    asyncio.run(run())


@pytest.mark.parametrize("status", [429, 502, 503, None])
def test_transient_notifications_preserve_native_reconnects_and_can_complete(status):
    async def run():
        client = JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)
        state = client.set_last_turn("thread-1", "turn-1")
        for _ in range(5):
            client._handle_notification(
                error_frame(
                    {
                        "message": "Reconnecting...",
                        "codexErrorInfo": {"responseStreamDisconnected": {"httpStatusCode": status}},
                    }
                )
            )
        assert not state.done.is_set()
        assert not state.interrupt_requested
        client._handle_notification(
            {
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "delta": "ok",
                },
            }
        )
        client._handle_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "completed"},
                },
            }
        )
        backend = CodexAppServerBackend(client=client)
        backend._loaded_thread_ids.add("thread-1")
        assert (await backend.capture("thread-1")).text == "ok"

    asyncio.run(run())


def test_prose_and_unrelated_details_do_not_become_auth_errors():
    for text in (
        "upstream returned 403",
        "unsupported_country_region_territory",
        "prefix Failed to refresh token: 403 Forbidden: x",
    ):
        assert codex_turn_failure("turn-1", {"message": text, "additionalDetails": text}).reason == "turn_failed"


def test_late_tools_of_failed_turn_are_rejected():
    async def run():
        client = JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)
        calls = []

        async def request(method, params):
            calls.append(method)
            return {}

        client.request = request

        async def response(request_id, result):
            calls.append((request_id, result["success"]))

        client._send_response = response
        state = client.set_last_turn("thread-1", "turn-1")
        client._handle_notification(
            error_frame(
                {
                    "codexErrorInfo": {
                        "responseStreamDisconnected": {"httpStatusCode": 403},
                    }
                }
            )
        )
        await state.done.wait()
        assert state.failure.reason == "authentication_failed"
        await client._handle_server_request(
            {
                "method": "item/tool/call",
                "id": 7,
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                },
            }
        )
        assert calls == ["turn/interrupt", (7, False)]

    asyncio.run(run())


class ScriptedWireClient(JsonRpcStdioClient):
    """Drive the real stdio reader with native protocol frames; no direct failure injection."""

    def __init__(self, error, *, cleanup_fails):
        super().__init__(command=["unused"], cwd=None, env={}, request_timeout_s=1)
        self.frames = asyncio.StreamReader()
        self._proc = SimpleNamespace(stdout=self.frames)
        self.error = error
        self.cleanup_fails = cleanup_fails
        self.calls = []

    async def request(self, method, params):
        self.calls.append(method)
        if method == "thread/start":
            return {"thread": {"id": "thread-1"}}
        if method == "turn/start":
            self.frames.feed_data((json.dumps(error_frame(self.error)) + "\n").encode())
            return {"turn": {"id": "turn-1"}}
        if method == "thread/archive" and self.cleanup_fails:
            raise RuntimeError("archive unavailable")
        return {}


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_auth_failure_crosses_reader_planner_worker_and_terminalizes_sqlite_once(tmp_path, cleanup_fails):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = durable.enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent",
        message_type="ChatBatchReady",
        payload={"source_id": "chat-a", "text": "hello"},
        idempotency_key="message-1",
    )

    async def run():
        stop = asyncio.Event()
        client = ScriptedWireClient(
            {
                "codexErrorInfo": {"responseStreamDisconnected": {"httpStatusCode": None}},
                "additionalDetails": "Failed to refresh token: 403 Forbidden: region blocked",
            },
            cleanup_fails=cleanup_fails,
        )
        reader = asyncio.create_task(client._read_stdout())
        backend = CodexAppServerBackend(client=client)
        llm = SessionLlmBackend(backend=backend, kind="codex")

        class Prompt:
            def build_prompt(self, **kwargs):
                return "hello"

            def build_system_prompt(self, **kwargs):
                return "Return JSON."

        class Parser:
            def parse(self, text):
                pytest.fail("A failed model call cannot dispatch a decision")

        class Handler:
            calls = 0

            async def handle(self, event):
                self.calls += 1
                try:
                    await run_planner_turn(
                        conn,
                        event=event,
                        llm_backend=llm,
                        prompt_builder=Prompt(),
                        decision_parser=Parser(),
                        config=PlannerRuntimeConfig(
                            model="gpt-5.4",
                            prompt_version="1",
                            cwd=tmp_path,
                            env_home=tmp_path,
                        ),
                    )
                finally:
                    stop.set()

        handler = Handler()
        try:
            await asyncio.wait_for(run_agent_worker(tmp_path / "app.db", stop, handler=handler), 2)
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        assert handler.calls == 1
        assert client.calls.count("turn/start") == 1
        assert client.calls.count("turn/interrupt") == 1

    asyncio.run(run())
    row = conn.execute("SELECT * FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert (row["status"], row["attempts"], row["max_attempts"]) == ("dead_letter", 1, 5)
    assert "authentication_failed (HTTP 403)" in row["last_error"]
    assert row["lease_token"] is None
    assert durable.claim_due(conn, recipient="agent", limit=1, lease_owner="next", lease_seconds=60) == []
    assert (
        durable.enqueue(
            conn,
            tenant_id="tenant-a",
            recipient="agent",
            message_type="ChatBatchReady",
            payload={"source_id": "chat-a", "text": "hello"},
            idempotency_key="message-1",
        )
        is None
    )
    run = conn.execute("SELECT status, output_json FROM agent_runs").fetchone()
    assert run["status"] == "failed"
    assert "CodexTurnFailure" in run["output_json"]
    conn.close()


def test_dead_letter_rejects_stale_lease_and_cannot_reopen_terminal_row(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = durable.enqueue(
        conn, tenant_id="tenant-a", recipient="agent", message_type="Test", payload={}, idempotency_key="one", now=100
    )
    first = durable.claim_due(conn, recipient="agent", limit=1, lease_owner="first", lease_seconds=1, now=100)[0]
    second = durable.claim_due(conn, recipient="agent", limit=1, lease_owner="second", lease_seconds=60, now=102)[0]
    assert not durable.mark_dead_letter(conn, event_id, lease_token=first["lease_token"], last_error="stale", now=102)
    assert durable.mark_dead_letter(conn, event_id, lease_token=second["lease_token"], last_error="final", now=102)
    assert (
        durable.mark_retry(conn, event_id, lease_token=second["lease_token"], run_after=103, last_error="retry") is None
    )
    conn.close()


def test_failed_transport_before_capture_keeps_accepted_turn_failure():
    async def run():
        client = JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)
        state = client.set_last_turn("thread-1", "turn-1")
        client._mark_failed("stdout closed")
        backend = CodexAppServerBackend(client=client)
        # capture must consume the failed state before attempting client recreation.
        with pytest.raises(CodexTurnFailure, match="transport_failed") as caught:
            await backend.capture("thread-1")
        assert caught.value.interrupt_failed
        assert client.last_turn("thread-1") is None
        assert state.done.is_set()

    asyncio.run(run())


def test_auth_interrupt_failure_retains_typed_cause_and_fails_client():
    async def run():
        client = JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)

        async def request(method, params):
            raise RuntimeError("interrupt rejected")

        client.request = request
        state = client.set_last_turn("thread-1", "turn-1")
        client._handle_notification(error_frame({"codexErrorInfo": "unauthorized"}))
        await asyncio.wait_for(state.done.wait(), 1)
        assert state.failure.interrupt_failed
        assert client.failed
        backend = CodexAppServerBackend(client=client)
        with pytest.raises(CodexTurnFailure, match="authentication_failed"):
            await backend.capture("thread-1")

    asyncio.run(run())
