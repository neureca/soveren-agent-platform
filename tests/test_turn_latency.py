"""Exercise latency reporting through the shipped Telegram composition and SQLite."""

import asyncio
import json
import logging
import time
from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import BaseModel
from test_telegram_ptb_adapter import FakeApplicationBuilder, FakeHandler, FakeUpdate

from soveren_agent_platform.agent.worker import _process_event
from soveren_agent_platform.decisions import DecisionDispatcher, DecisionRegistry, DispatchResult
from soveren_agent_platform.llm.backends.session import SessionLlmBackend
from soveren_agent_platform.outbound import SQLiteOutboundQueue
from soveren_agent_platform.outbound.contracts import SendResult
from soveren_agent_platform.outbound.worker import _send_message
from soveren_agent_platform.queue import SQLiteEventQueue
from soveren_agent_platform.runtime.failures import NonRetryableEventError
from soveren_agent_platform.runtime.latency import SQLiteTurnLatencyLogger
from soveren_agent_platform.runtime.planner import PlannerRuntimeConfig, run_planner_dispatch_turn
from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import create_telegram_agent_app, enqueue_telegram_text

SECRET = "private content and secret-token"


class Reply(BaseModel):
    kind: Literal["reply"]
    text: str


class SessionBackend:
    def __init__(self, fail=False):
        self.calls = 0
        self.fail = fail

    async def open(self, spec):
        await asyncio.sleep(0.01)
        return OpenResult("native-session")

    async def send(self, session_id, text):
        self.calls += 1
        await asyncio.sleep(0.01)
        if self.fail:
            raise NonRetryableEventError(SECRET)

    async def capture(self, session_id):
        return CaptureResult(json.dumps({"kind": "reply", "text": SECRET * 300}), False)

    async def close(self, session_id):
        pass


class Prompts:
    def build_prompt(self, **kwargs):
        return SECRET

    def build_system_prompt(self, **kwargs):
        return SECRET


class Handler:
    def __init__(self, path, *, fail=False, retry_dispatch=False, llm_backend=None):
        self.conn = open_sqlite(path)
        apply_platform_migrations(self.conn)
        self.backend = SessionBackend(fail)
        self.llm_backend = llm_backend
        self.result = None
        self.dispatch_calls = 0
        self.retry_dispatch = retry_dispatch
        self.event = None

    async def dispatch(self, effects, decision, context):
        self.dispatch_calls += 1
        if self.retry_dispatch and self.dispatch_calls == 1:
            raise RuntimeError(SECRET)
        parts = await enqueue_telegram_text(
            effects.outbound,
            tenant_id=context.tenant_id,
            source_id=context.source_id,
            destination_id=context.source_id,
            text=decision.text,
            idempotency_key=f"reply:{context.source_event_id}",
            correlation_id=context.source_event_id,
        )
        return DispatchResult(target="outbound", id=next((part for part in parts if part), None))

    async def handle(self, event):
        self.event = event
        registry = DecisionRegistry()
        registry.register("reply", Reply)
        dispatcher = DecisionDispatcher()
        dispatcher.register("reply", self)
        self.result = await run_planner_dispatch_turn(
            self.conn,
            event=event,
            prompt_builder=Prompts(),
            llm_backend=self.llm_backend or SessionLlmBackend(self.backend, "codex_cli"),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=PlannerRuntimeConfig(model="test", prompt_version="v1", cwd=".", env_home="."),
        )


def summaries(caplog):
    return [record.turn_latency for record in caplog.records if hasattr(record, "turn_latency")]


async def until(predicate, timeout=2):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def composition(path, handler, *, outbound_idle=0.02, logging_enabled=True, **kwargs):
    builder = FakeApplicationBuilder()
    app = await create_telegram_agent_app(
        token=SECRET,
        db_path=path,
        tenant_id="tenant",
        handler=handler,
        allowed_chat_ids=[456],
        quiet_window_s=0,
        max_window_s=1,
        latency_logging=logging_enabled,
        agent_idle_max_s=0.02,
        outbound_idle_max_s=outbound_idle,
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
        **kwargs,
    )
    return app, builder


def test_composition_fast_multipart_summary_and_duplicate_receipt(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        path = tmp_path / "app.db"
        handler = Handler(path)
        app, builder = await composition(path, handler)
        callback = builder.app.handlers[0].callback
        update = FakeUpdate(text=SECRET)
        # Ingress is queued before workers start so every measured stage is deterministic.
        event_id = await callback(update, SimpleNamespace())
        assert event_id is not None
        assert await callback(update, SimpleNamespace()) is None
        started = time.monotonic()
        try:
            await app.start()
            await until(lambda: summaries(caplog), timeout=0.8)
            assert time.monotonic() - started < 0.8
            payload = summaries(caplog)[0]
            assert payload["outcome"] == "sent"
            assert payload["reply_parts"] > 1
            assert payload["reply_states"] == {"sent": payload["reply_parts"]}
            assert payload["event_id"] == handler.event.id
            assert payload["run_ids"] == [handler.result.planner.run_id]
            assert payload["agent_attempts"] == 1
            assert all(phase["availability"] == "measured" for phase in payload["phases"].values())
            assert payload["phases"]["session_open"]["duration_ms"] >= 5
            assert payload["phases"]["codex_turn"]["duration_ms"] >= 5
            assert payload["total_ms"] > 0
            llm = json.loads(handler.conn.execute("SELECT output_json FROM agent_runs").fetchone()[0])["llm"]
            assert llm["duration_ms"] >= 15
            assert llm["input_tokens"] is None and llm["cost_usd"] is None
            # Receipt replay uses the original result and never starts another model/send.
            await handler.handle(handler.event)
            await asyncio.sleep(0.05)
            assert handler.backend.calls == 1
            assert len(summaries(caplog)) == 1
            assert len(builder.app.bot.calls) == payload["reply_parts"]
            assert SECRET not in caplog.text
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())


def test_failed_turn_has_one_summary_and_no_content(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        handler = Handler(tmp_path / "app.db", fail=True)
        app, builder = await composition(tmp_path / "app.db", handler)
        await builder.app.handlers[0].callback(FakeUpdate(text=SECRET), SimpleNamespace())
        try:
            await app.start()
            await until(lambda: summaries(caplog))
            result = summaries(caplog)[0]
            assert result["outcome"] == "agent_failed"
            assert result["run_ids"]
            assert result["phases"]["codex_turn"]["duration_ms"] >= 5
            assert result["phases"]["dispatch"]["availability"] == "unavailable"
            assert result["phases"]["telegram_send"]["duration_ms"] is None
            assert handler.conn.execute("SELECT status FROM agent_runs").fetchone()[0] == "failed"
            assert SECRET not in caplog.text
            assert len(summaries(caplog)) == 1
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())


def test_restart_before_send_keeps_identity_and_marks_missing_measurements(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        path = tmp_path / "app.db"
        handler = Handler(path)
        first, builder = await composition(path, handler, outbound_idle=60)
        # Let outbound enter its idle sleep before publishing a turn.
        await first.start()
        await asyncio.sleep(0.03)
        await builder.app.handlers[0].callback(FakeUpdate(text=SECRET), SimpleNamespace())
        try:
            await until(
                lambda: handler.conn.execute(
                    "SELECT 1 FROM event_queue WHERE recipient = 'agent' AND status = 'done'"
                ).fetchone()
            )
            assert not summaries(caplog)
        finally:
            await first.stop()
        second, _ = await composition(path, handler)
        try:
            await second.start()
            await until(lambda: summaries(caplog))
            result = summaries(caplog)[0]
            assert result["outcome"] == "sent"
            assert result["event_id"] == handler.event.id
            assert result["run_ids"] == [handler.result.planner.run_id]
            assert result["total_ms"] is None
            assert result["phases"]["session_open"]["availability"] == "unavailable"
            assert result["phases"]["telegram_send"]["availability"] == "measured"
            assert result["partial"] is True
            assert handler.backend.calls == 1
        finally:
            await second.stop()
            handler.conn.close()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["retry", "permanent", "uncertain"])
def test_outbound_retry_failure_and_uncertainty_are_not_false_success(tmp_path, caplog, outcome):
    caplog.set_level(logging.INFO)

    class Sender:
        calls = 0

        async def send(self, message):
            self.calls += 1
            if outcome == "uncertain":
                raise RuntimeError(SECRET)
            if outcome == "permanent":
                return SendResult.permanent_failure(SECRET)
            if self.calls == 1:
                return SendResult.retryable_failure(SECRET, retry_after_s=0)
            return SendResult.sent()

    async def run():
        path = tmp_path / "app.db"
        handler = Handler(path)
        app, builder = await composition(path, handler, outbound_idle=60)
        await app.start()
        await asyncio.sleep(0.03)
        await builder.app.handlers[0].callback(FakeUpdate(text=SECRET), SimpleNamespace())
        try:
            await until(
                lambda: handler.conn.execute(
                    "SELECT 1 FROM event_queue WHERE recipient = 'agent' AND status = 'done'"
                ).fetchone()
            )
            # A fresh observer also exercises missing pre-send measurements.
            reporter = SQLiteTurnLatencyLogger._from_connection(handler.conn)
            queue = SQLiteOutboundQueue._from_connection(handler.conn)
            sender = Sender()
            for _ in range(10):
                messages = await queue.claim_due(
                    channel="telegram", tenant_id="tenant", limit=5, lease_owner="test", lease_seconds=10
                )
                for message in messages:
                    await _send_message(queue, message, sender=sender, retry_backoff_s=0, latency_logger=reporter)
                if summaries(caplog):
                    break
                assert outcome == "retry"
            result = summaries(caplog)[0]
            assert result["outcome"] == {"retry": "sent", "permanent": "send_failed", "uncertain": "uncertain"}[outcome]
            if outcome == "retry":
                assert result["outbound_attempts"] == result["reply_parts"] + 1
            assert len(summaries(caplog)) == 1
            await asyncio.gather(reporter.finish('tenant', '456', handler.event.id),
                                 reporter.finish('tenant', '456', handler.event.id))
            assert len(summaries(caplog)) == 1
            assert SECRET not in caplog.text
            await reporter.finish("foreign-tenant", "456", handler.event.id)
            assert len(summaries(caplog)) == 1
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())


def test_agent_dispatch_retry_reuses_model_and_logs_once(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        path = tmp_path / "app.db"
        handler = Handler(path, retry_dispatch=True)
        events = SQLiteEventQueue._from_connection(handler.conn)
        reporter = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        await events.enqueue(
            tenant_id="tenant",
            recipient="agent",
            message_type="ChatBatchReady",
            payload={"source_id": "456", "text": SECRET},
            idempotency_key="event",
        )
        for _ in range(2):
            (event,) = await events.claim_due(recipient="agent", limit=1, lease_owner="test", lease_seconds=10)
            await _process_event(events, event, handler=handler, retry_backoff_s=0, latency_logger=reporter)
            assert not summaries(caplog)
        queue = SQLiteOutboundQueue._from_connection(handler.conn)

        class Sender:
            async def send(self, message):
                return SendResult.sent()

        for _ in range(10):
            parts = await queue.claim_due(channel="telegram", limit=5, lease_owner="send", lease_seconds=10)
            for part in parts:
                await _send_message(queue, part, sender=Sender(), retry_backoff_s=0, latency_logger=reporter)
            if summaries(caplog):
                break
        assert len(summaries(caplog)) == 1
        assert summaries(caplog)[0]["agent_attempts"] == 2
        assert handler.backend.calls == 1
        assert handler.dispatch_calls == 2
        handler.conn.close()

    asyncio.run(run())


def test_stale_workers_cannot_publish_terminal_summaries(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        handler = Handler(tmp_path / "app.db")
        conn = handler.conn
        reporter = SQLiteTurnLatencyLogger._from_connection(conn)
        events = SQLiteEventQueue._from_connection(conn)
        event_id = await events.enqueue(
            tenant_id="tenant",
            recipient="agent",
            message_type="ChatBatchReady",
            payload={"source_id": "456"},
            idempotency_key="old",
        )
        (old,) = await events.claim_due(recipient="agent", limit=1, lease_owner="old", lease_seconds=10)
        conn.execute("UPDATE event_queue SET lease_until = 0 WHERE id = ?", (event_id,))
        (new,) = await events.claim_due(recipient="agent", limit=1, lease_owner="new", lease_seconds=10)
        await events.mark_done(event_id, lease_token=new.lease_token)

        class Noop:
            async def handle(self, event):
                pass

        await _process_event(events, old, handler=Noop(), retry_backoff_s=0, latency_logger=reporter)
        assert not summaries(caplog)
        outbound = SQLiteOutboundQueue._from_connection(conn)
        message_id = await outbound.enqueue(
            tenant_id="tenant",
            source_id="456",
            channel="telegram",
            destination_id="456",
            text=SECRET,
            idempotency_key="send",
            correlation_id=event_id,
        )
        (message,) = await outbound.claim_due(channel="telegram", limit=1, lease_owner="old", lease_seconds=10)

        class LosesLeaseDuringSend:
            async def send(self, message):
                conn.execute("UPDATE outbound_messages SET lease_until = 0 WHERE id = ?", (message_id,))
                assert await outbound.claim_due(channel="telegram", limit=1, lease_owner="new", lease_seconds=10) == []
                return SendResult.sent()

        await _send_message(
            outbound, message, sender=LosesLeaseDuringSend(), retry_backoff_s=0, latency_logger=reporter
        )
        assert conn.execute("SELECT status FROM outbound_messages").fetchone()[0] == "uncertain"
        assert not summaries(caplog)
        conn.close()

    asyncio.run(run())


def test_telemetry_failure_does_not_retry_committed_send(tmp_path, caplog, monkeypatch):
    import soveren_agent_platform.runtime.latency as latency

    caplog.set_level(logging.INFO)

    async def run():
        handler = Handler(tmp_path / "app.db")
        events = SQLiteEventQueue._from_connection(handler.conn)
        event_id = await events.enqueue(
            tenant_id="tenant",
            recipient="agent",
            message_type="ChatBatchReady",
            payload={"source_id": "456"},
            idempotency_key="turn",
        )
        (event,) = await events.claim_due(recipient="agent", limit=1, lease_owner="test", lease_seconds=10)
        await events.mark_done(event_id, lease_token=event.lease_token)
        outbound = SQLiteOutboundQueue._from_connection(handler.conn)
        await outbound.enqueue(
            tenant_id="tenant",
            source_id="456",
            channel="telegram",
            destination_id="456",
            text=SECRET,
            idempotency_key="send",
            correlation_id=event_id,
        )
        (message,) = await outbound.claim_due(channel="telegram", limit=1, lease_owner="test", lease_seconds=10)
        reporter = SQLiteTurnLatencyLogger._from_connection(handler.conn)

        async def failed_read(*args, **kwargs):
            raise RuntimeError(SECRET)

        class Sender:
            async def send(self, message):
                return SendResult.sent()

        monkeypatch.setattr(latency, "run_sqlite", failed_read)
        await _send_message(outbound, message, sender=Sender(), retry_backoff_s=0, latency_logger=reporter)
        assert handler.conn.execute("SELECT status, attempts FROM outbound_messages").fetchone()[:] == ("sent", 1)
        assert not summaries(caplog)
        assert SECRET not in caplog.text
        handler.conn.close()

    asyncio.run(run())


def test_telemetry_caches_are_bounded_and_turn_lookup_is_indexed(tmp_path):
    handler = Handler(tmp_path / "app.db")
    reporter = SQLiteTurnLatencyLogger._from_connection(handler.conn, cache_limit=2)
    for index in range(10):
        reporter.ingress("tenant", "456", str(index), started=time.monotonic())
        with reporter.agent("tenant", "456", str(index), None):
            pass
    assert len(reporter._ingress) == len(reporter._traces) == 2
    plan = handler.conn.execute(
        "EXPLAIN QUERY PLAN SELECT status, COUNT(*) FROM outbound_messages"
        " WHERE tenant_id = ? AND source_id = ? AND correlation_id = ? AND channel = 'telegram' GROUP BY status",
        ("tenant", "456", "event"),
    ).fetchall()
    assert any("idx_outbound_turn_lookup" in row["detail"] for row in plan)
    handler.conn.close()
