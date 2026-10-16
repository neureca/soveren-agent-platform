"""Polling affects observation latency, not the stored quiet-window rule."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from test_telegram_ptb_adapter import FakeApplicationBuilder, FakeHandler, FakeUpdate
from test_turn_latency import until

from soveren_agent_platform.batching import store as batch_store
from soveren_agent_platform.batching import worker as batch_worker
from soveren_agent_platform.queue import durable
from soveren_agent_platform.runtime import worker_loop
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import create_telegram_agent_app


class Handler:
    def __init__(self):
        self.events = []

    async def handle(self, event):
        self.events.append(event)


def test_batching_worker_caps_idle_sleeps_and_keeps_generic_default(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "app.db"
        conn = open_sqlite(path)
        apply_platform_migrations(conn)
        conn.close()
        stop = asyncio.Event()
        sleeps = []

        async def sleep(stop_event, seconds):
            sleeps.append(seconds)
            if len(sleeps) == 5:
                stop_event.set()

        monkeypatch.setattr(worker_loop, "sleep_or_stop", sleep)
        await batch_worker.run_batching_worker(path, stop, idle_max_s=1.0)
        assert sleeps == [0.5, 1.0, 1.0, 1.0, 1.0]

    asyncio.run(run())
    import inspect

    assert inspect.signature(batch_worker.run_batching_worker).parameters["idle_max_s"].default == 5.0
    assert inspect.signature(create_telegram_agent_app).parameters["batching_idle_max_s"].default == 5.0


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True])
def test_batching_poll_max_is_validated_at_factory_and_worker(tmp_path, value):
    async def run():
        path = tmp_path / "factory.db"
        with pytest.raises(ValueError, match="finite and positive"):
            await create_telegram_agent_app(
                token="fixture",
                db_path=path,
                tenant_id="tenant",
                handler=Handler(),
                allowed_chat_ids=[456],
                batching_idle_max_s=value,
            )
        assert not path.exists()
        path = tmp_path / "worker.db"
        with pytest.raises(ValueError, match="finite and positive"):
            await batch_worker.run_batching_worker(path, asyncio.Event(), idle_max_s=value)

    asyncio.run(run())


@pytest.mark.parametrize("gap", [1, 3])
def test_factory_preserves_two_second_quiet_window_with_one_second_polling(tmp_path, monkeypatch, gap):
    # Advance only operational UTC clocks; event-loop/phase clocks remain monotonic.
    clock = SimpleNamespace(now=100)
    wall = SimpleNamespace(time=lambda: clock.now)
    monkeypatch.setattr(batch_store, "time", wall)
    monkeypatch.setattr(batch_worker, "time", wall)
    monkeypatch.setattr(durable, "time", wall)

    async def run():
        path = tmp_path / "app.db"
        conn = open_sqlite(path)
        apply_platform_migrations(conn)
        handler = Handler()
        builder = FakeApplicationBuilder()
        app = await create_telegram_agent_app(
            token="fixture",
            db_path=path,
            tenant_id="tenant",
            handler=handler,
            allowed_chat_ids=[456],
            batching_idle_max_s=1.0,
            # Default quiet_window_s=2 is intentionally left untouched.
            application_builder=builder,
            message_handler_cls=FakeHandler,
            callback_query_handler_cls=FakeHandler,
            message_filter="all",
        )
        callback = builder.app.handlers[0].callback

        async def enqueue(update_id, at):
            clock.now = at
            update = FakeUpdate(update_id=update_id, text="plain message")
            update.effective_message.date = datetime.fromtimestamp(at, UTC)
            await callback(update, SimpleNamespace())

        def stored(n):
            return (
                conn.execute("SELECT COUNT(*) FROM inbound_batch_messages").fetchone()[0] == n
                and conn.execute(
                    "SELECT COUNT(*) FROM event_queue WHERE message_type='InboundMessageReceived' AND status='done'"
                ).fetchone()[0]
                == n
            )

        try:
            await enqueue(1, 100)
            await app.start()
            await until(lambda: stored(1), timeout=3)
            if gap == 1:
                await enqueue(2, 101)
                await until(lambda: stored(2), timeout=3)
                clock.now = 102
                await until(
                    lambda: conn.execute(
                        "SELECT 1 FROM event_queue WHERE message_type='FlushInboundBatch'"
                        " AND idempotency_key LIKE 'inbound-batch-flush:%:102' AND status IN ('done','retrying')"
                    ).fetchone(),
                    timeout=3,
                )
                # The legacy scheduler can retry the obsolete flush on a causation
                # fingerprint conflict; the replacement deadline still owns grouping.
                assert (
                    conn.execute(
                        "SELECT status FROM event_queue WHERE idempotency_key LIKE 'inbound-batch-flush:%:102'"
                    ).fetchone()[0]
                    == "retrying"
                )  # known legacy causation conflict, intentionally preserved
                assert conn.execute(
                    "SELECT 1 FROM event_queue WHERE message_type='FlushInboundBatch' AND run_after=103"
                    " AND status='queued'"
                ).fetchone()
                assert not handler.events  # second inbound resets the quiet deadline
                clock.now = 103
                await until(lambda: len(handler.events) == 1, timeout=3)
                assert handler.events[0].payload["batch_message_count"] == 2
            else:
                clock.now = 102
                await until(lambda: len(handler.events) == 1, timeout=3)
                await enqueue(2, 103)  # >2s, following the observed flush
                await until(lambda: stored(2), timeout=3)
                clock.now = 105
                await until(lambda: len(handler.events) == 2, timeout=3)
                assert [event.payload["batch_message_count"] for event in handler.events] == [1, 1]
                assert len({event.payload["batch_id"] for event in handler.events}) == 2
        finally:
            await app.stop()
            conn.close()

    asyncio.run(run())
