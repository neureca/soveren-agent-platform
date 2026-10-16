"""Observe preparation through real SIWC/runtime/native/Telegram composition."""

import asyncio
import logging
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_codex_latency_events import SERVER
from test_sandbox_manager import FakeSandboxManager
from test_telegram_ptb_adapter import FakeUpdate
from test_turn_latency import Handler, composition, summaries, until

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.llm.backends import sandboxed_codex as runtime_module
from soveren_agent_platform.runtime import latency
from soveren_agent_platform.runtime.latency import (
    CodexTurnTiming,
    SQLiteTurnLatencyLogger,
    _runtime_details,
    measure_codex_runtime,
    measure_phase,
    record_codex_turn_timing,
)
from soveren_agent_platform.sandbox import SandboxSpec
from soveren_agent_platform.sessions import ChatGPTPlanAccessToken, CodexChatGPTPlanCredentials, CodexClientInfo
from soveren_agent_platform.sessions.backends.sandboxed_codex import SandboxedCodexAppServerBackend
from soveren_agent_platform.sessions.codex_plan_credentials import CodexCredentialUnavailable


class DelayedManager(FakeSandboxManager):
    def __init__(self, command, *, mode="normal"):
        super().__init__()
        self.command = command
        self.mode = mode
        self.handle = replace(self.handle, tenant_id="tenant", conversation_id="456")

    async def acquire(self, spec):
        result = await super().acquire(spec)
        await asyncio.sleep(0.01)
        if self.mode == "acquire_failure" and len(self.acquired) == 2:
            raise CodexCredentialUnavailable()
        return result

    async def provision_http_credential(self, *args, **kwargs):
        endpoint = await super().provision_http_credential(*args, **kwargs)
        if self.mode == "rebuild" and len(self.http_broker_calls) >= 2:
            return replace(endpoint, base_url="http://soveren-credential-broker:8080/bindings/new-capability")
        return endpoint

    def exec_command(self, *args, **kwargs):
        super().exec_command(*args, **kwargs)
        return self.command


def test_nested_runtime_coverage_never_adds_parent_and_child_twice(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(latency, "time", SimpleNamespace(monotonic=lambda: clock.now))
    handler = Handler(tmp_path / "app.db")
    reporter = SQLiteTurnLatencyLogger._from_connection(handler.conn)
    changes = handler.conn.total_changes
    with reporter.agent("tenant", "456", "event", None):
        with measure_phase("codex_turn"):
            with measure_codex_runtime("prepare_turn"):
                with measure_codex_runtime("prepare_warm"):
                    with measure_codex_runtime("sandbox_acquire"):
                        clock.now += 2
                    with measure_codex_runtime("credentials_refresh"):
                        clock.now += 3
            with measure_codex_runtime("ensure_thread"):
                clock.now += 1
            record_codex_turn_timing("native", CodexTurnTiming(request_started_at=6, observed_until=10))
            # Update of the same native observation cannot double-count that interval.
            record_codex_turn_timing("native", CodexTurnTiming(request_started_at=6, observed_until=10))
            clock.now = 10
    trace = reporter._traces[("tenant", "456", "event")]
    result = _runtime_details(trace)
    assert result["prepare_turn"]["duration_ms"] == 5000
    assert result["sandbox_acquire"]["duration_ms"] == 2000
    assert result["credentials_refresh"]["duration_ms"] == 3000
    assert result["accounted"]["duration_ms"] == 10000
    assert result["unmeasured"]["duration_ms"] == 0  # derived exact coverage, not a missing-data default
    assert result["prepare_unmeasured"]["duration_ms"] == 0
    assert result["backend_rebuild"]["duration_ms"] is None
    assert handler.conn.total_changes == changes
    handler.conn.close()


@pytest.mark.parametrize("mode", ["normal", "rebuild", "acquire_failure", "refresh_failure"])
def test_real_siwc_preparation_reaches_one_terminal_log_without_extra_calls(tmp_path, caplog, monkeypatch, mode):
    caplog.set_level(logging.INFO)

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        manager = DelayedManager([sys.executable, str(script), "normal"], mode=mode)
        token_calls = 0

        async def token():
            nonlocal token_calls
            token_calls += 1
            await asyncio.sleep(0.01)
            if mode == "refresh_failure" and token_calls == 2:
                raise CodexCredentialUnavailable()
            return ChatGPTPlanAccessToken("fixture-bearer-secret", datetime.now(UTC) + timedelta(hours=1))

        provider = CodexChatGPTPlanCredentials("tenant", token, CodexClientInfo("Pulsy", "0.1.0"))

        async def resolve(tenant):
            assert tenant == "tenant"
            return provider

        monkeypatch.setattr(runtime_module, "_create_sandbox_manager", lambda **kwargs: manager)
        path = tmp_path / "app.db"
        platform = AgentPlatformApp(db_path=path)
        codex = platform.configure_sandboxed_codex(credentials_for_tenant=resolve, model="test", max_active_sandboxes=3)
        handler = Handler(path, llm_backend=codex)
        app, builder = await composition(path, handler, platform=platform, batching_idle_max_s=0.02)
        try:
            await builder.app.handlers[0].callback(FakeUpdate(update_id=1), SimpleNamespace())
            await app.start()
            await until(lambda: len(summaries(caplog)) == 1, timeout=3)
            first = summaries(caplog)[0]
            details = first["codex_turn_details"]["runtime"]
            assert details["attempts"] == 1
            assert details["prepare_turn"]["samples"] == details["prepare_warm"]["samples"] == 1
            assert details["prepare_cold"]["duration_ms"] is None  # cold open belongs to session_open
            assert details["sandbox_acquire"]["samples"] == 1
            assert details["sandbox_acquire"]["duration_ms"] >= 5
            assert len(manager.acquired) == 2  # one open + one prepare; capture makes no extra acquire
            if mode.endswith("failure"):
                assert first["outcome"] == "agent_failed"
                assert details["prepare_turn"]["availability"] == "partial"
                stage = "sandbox_acquire" if mode == "acquire_failure" else "credentials_refresh"
                assert details[stage]["availability"] == "partial"
                assert details["ensure_thread"]["duration_ms"] is None
                assert details["unmeasured"]["availability"] == "partial"
                assert token_calls == (1 if mode == "acquire_failure" else 2)
            else:
                assert first["outcome"] == "sent"
                assert details["credentials_refresh"]["duration_ms"] >= 5
                assert details["credentials_refresh"]["samples"] == 1
                assert details["ensure_thread"]["samples"] == 1
                assert details["native_event_window"]["availability"] == "measured"
                assert details["accounted"]["duration_ms"] + details["unmeasured"]["duration_ms"] == pytest.approx(
                    first["phases"]["codex_turn"]["duration_ms"], abs=0.01
                )
                if mode == "rebuild":
                    assert token_calls == len(manager.http_broker_calls) == 3
                    assert details["backend_rebuild"]["samples"] == 1
                    assert details["backend_build"]["samples"] == 1
                    assert details["backend_shutdown"]["samples"] == 1
                    assert sum(call[0] == "docker" for call in manager.commands) == 2
                else:
                    assert token_calls == len(manager.http_broker_calls) == 2
                    assert details["backend_rebuild"]["duration_ms"] is None
                    assert details["backend_shutdown"]["duration_ms"] is None
                    await builder.app.handlers[0].callback(FakeUpdate(update_id=2), SimpleNamespace())
                    await until(lambda: len(summaries(caplog)) == 2, timeout=3)
                    assert token_calls == len(manager.acquired) == 3
                    assert summaries(caplog)[1]["codex_turn_details"]["runtime"]["attempts"] == 1
                    before = token_calls
                    await handler.handle(handler.event)  # durable receipt replay
                    assert token_calls == before
                    assert len(summaries(caplog)) == 2
            assert "fixture-bearer-secret" not in caplog.text
            assert "fixture-bearer-secret" not in repr(manager.commands)
            assert "fixture-bearer-secret" not in repr(manager.exec_envs)
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())


def test_cold_prepare_timeout_and_non_rotating_credentials_are_explicit(tmp_path):
    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        manager = DelayedManager([sys.executable, str(script), "no_first_timeout"])
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager, sandbox_spec=SandboxSpec("tenant", "456", "fixture"), turn_timeout_s=0.01
        )
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        try:
            with logger.agent("tenant", "456", "cold", None):
                with pytest.raises(TimeoutError):
                    with measure_phase("codex_turn"):
                        await backend.send("thread-existing", "request")  # no prior open: cold prepare
                        result = await backend.capture("thread-existing")
                        assert result.timed_out
                        raise TimeoutError()
            trace = logger._traces[("tenant", "456", "cold")]
            details = _runtime_details(trace)
            assert details["prepare_cold"]["samples"] == 1
            assert details["prepare_warm"]["duration_ms"] is None
            assert details["backend_build"]["samples"] == 1
            assert details["sandbox_acquire"]["samples"] == 1
            assert details["credentials_refresh"]["availability"] == "unavailable"
            assert details["unmeasured"]["availability"] == "partial"
            assert details["unmeasured"]["duration_ms"] > 0
        finally:
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())


def test_parallel_turns_and_detached_tasks_do_not_share_runtime_samples(tmp_path):
    async def run():
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)

        async def detached():
            with measure_codex_runtime("backend_shutdown"):
                await asyncio.sleep(0)

        async def one(event, stage):
            with logger.agent("tenant", "source", event, None):
                with measure_phase("codex_turn"):
                    await asyncio.create_task(detached())  # inherits ContextVars but is not the owner
                    with measure_codex_runtime(stage):
                        await asyncio.sleep(0.01)

        await asyncio.gather(one("a", "sandbox_acquire"), one("b", "credentials_refresh"))
        first = _runtime_details(logger._traces[("tenant", "source", "a")])
        second = _runtime_details(logger._traces[("tenant", "source", "b")])
        assert first["sandbox_acquire"]["samples"] == second["credentials_refresh"]["samples"] == 1
        assert first["credentials_refresh"]["duration_ms"] is None
        assert second["sandbox_acquire"]["duration_ms"] is None
        assert first["backend_shutdown"]["duration_ms"] is None
        assert second["backend_shutdown"]["duration_ms"] is None
        handler.conn.close()

    asyncio.run(run())


def test_cancellation_is_partial_and_retained_samples_are_bounded(tmp_path):
    async def run():
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        with logger.agent("tenant", "source", "cancelled", None):
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.01):
                    with measure_phase("codex_turn"):
                        with measure_codex_runtime("credentials_refresh"):
                            await asyncio.Event().wait()
        trace = logger._traces[("tenant", "source", "cancelled")]
        metric = _runtime_details(trace)["credentials_refresh"]
        assert metric["availability"] == "partial"
        assert metric["incomplete_samples"] == 1
        assert trace.runtime_attempts[0].owner is None
        with logger.agent("tenant", "source", "bounded", None):
            with measure_phase("codex_turn"):
                for _ in range(300):
                    with measure_codex_runtime("ensure_thread"):
                        pass
        trace = logger._traces[("tenant", "source", "bounded")]
        details = _runtime_details(trace)
        assert trace.runtime_span_count == 256
        assert details["truncated"] is True
        assert details["ensure_thread"]["samples"] == 256
        assert details["ensure_thread"]["availability"] == "partial"
        assert details["ensure_thread"]["reason"] == "observation_limit"
        handler.conn.close()

    asyncio.run(run())


def test_real_lifecycle_lock_wait_is_preparation_residual(tmp_path):
    from soveren_agent_platform.conversation import ConversationScope
    from soveren_agent_platform.sessions.backend import OpenSpec

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        manager = DelayedManager([sys.executable, str(script), "normal"])
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec("tenant", "456", "fixture"),
        )
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        entered = asyncio.Event()
        try:
            opened = await backend.open(
                OpenSpec(kind="codex_cli", cwd="/workspace", conversation_scope=ConversationScope("tenant", "456"))
            )

            async def turn():
                with logger.agent("tenant", "456", "locked", None):
                    with measure_phase("codex_turn"):
                        entered.set()
                        await backend.send(opened.backend_session_id, "request")
                        await backend.capture(opened.backend_session_id)

            async with backend._lifecycle_lock:
                task = asyncio.create_task(turn())
                await entered.wait()
                await asyncio.sleep(0.02)
            await task
            details = _runtime_details(logger._traces[("tenant", "456", "locked")])
            assert details["prepare_unmeasured"]["duration_ms"] >= 10
            assert details["prepare_turn"]["duration_ms"] > details["sandbox_acquire"]["duration_ms"]
            assert details["prepare_warm"]["samples"] == 1
            assert len(manager.acquired) == 2
        finally:
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())
