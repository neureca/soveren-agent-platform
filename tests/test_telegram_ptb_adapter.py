import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from soveren_agent_platform.outbound.contracts import OutboundMessage
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import (
    TelegramRuntimeHooks,
    TelegramSender,
    build_telegram_polling_application,
    enqueue_telegram_update,
    handle_telegram_callback_query,
    handle_telegram_message_update,
)
from soveren_agent_platform.telegram.ptb import (
    PtbRuntimeHooks,
    PtbTelegramSender,
    build_ptb_application,
    build_ptb_inline_keyboard,
    enqueue_ptb_update,
    handle_ptb_callback_query,
    handle_ptb_message_update,
    update_to_inbound_message,
)


class FakeUpdate:
    update_id = 123

    def __init__(self) -> None:
        self.effective_chat = SimpleNamespace(id=456)
        self.effective_user = SimpleNamespace(id=789, username="ivan", first_name="Ivan")
        self.effective_message = SimpleNamespace(
            message_id=10,
            text="привет",
            caption=None,
            date=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        )

    def to_dict(self):
        return {"update_id": self.update_id, "message": {"text": "привет"}}


class FakeBot:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(message_id=42)


class FakeCallbackQuery:
    data = "approve:123"

    def __init__(self) -> None:
        self.answered = False

    async def answer(self):
        self.answered = True


class FakeApplication:
    def __init__(self) -> None:
        self.handlers = []

    def add_handler(self, handler):
        self.handlers.append(handler)


class FakeApplicationBuilder:
    def __init__(self) -> None:
        self.value = None
        self.app = FakeApplication()

    def token(self, token):
        self.value = token
        return self

    def build(self):
        return self.app


class FakeHandler:
    def __init__(self, *args):
        self.args = args
        self.callback = args[-1]


def test_public_telegram_names_hide_ptb_implementation_details():
    assert TelegramRuntimeHooks is PtbRuntimeHooks
    assert TelegramSender is PtbTelegramSender
    assert build_telegram_polling_application is build_ptb_application
    assert enqueue_telegram_update is enqueue_ptb_update
    assert handle_telegram_callback_query is handle_ptb_callback_query
    assert handle_telegram_message_update is handle_ptb_message_update


def test_update_to_inbound_message_normalizes_ptb_like_update():
    message = update_to_inbound_message(FakeUpdate(), tenant_id="tenant-a")

    assert message is not None
    assert message.tenant_id == "tenant-a"
    assert message.chat_id == 456
    assert message.update_id == 123
    assert message.user_id == 789
    assert message.username == "ivan"
    assert message.text == "привет"
    assert message.payload["date"] == 1767268800
    assert message.payload["raw"]["update_id"] == 123


def test_enqueue_ptb_update_routes_to_batching_queue(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    event_id = enqueue_ptb_update(conn, FakeUpdate(), tenant_id="tenant-a")

    assert event_id is not None
    row = conn.execute("SELECT * FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    payload = json.loads(row["payload_json"])
    assert row["recipient"] == "batching"
    assert row["message_type"] == "InboundMessageReceived"
    assert payload["channel"] == "telegram"
    assert payload["source_id"] == "456"
    assert payload["text"] == "привет"


def test_handle_ptb_message_update_calls_enqueue_hook(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    calls = []

    async def on_update_enqueued(**kwargs):
        calls.append(kwargs)

    event_id = asyncio.run(handle_ptb_message_update(
        conn,
        FakeUpdate(),
        SimpleNamespace(name="context"),
        tenant_id="tenant-a",
        hooks=PtbRuntimeHooks(on_update_enqueued=on_update_enqueued),
    ))

    assert event_id is not None
    assert calls[0]["event_id"] == event_id
    assert calls[0]["message"].chat_id == 456


def test_handle_ptb_callback_query_answers_and_calls_hook():
    query = FakeCallbackQuery()
    calls = []

    def on_callback_query(**kwargs):
        calls.append(kwargs)
        return "handled"

    result = asyncio.run(handle_ptb_callback_query(
        SimpleNamespace(callback_query=query),
        SimpleNamespace(name="context"),
        hooks=PtbRuntimeHooks(on_callback_query=on_callback_query),
    ))

    assert result == "handled"
    assert query.answered
    assert calls[0]["data"] == "approve:123"


def test_build_ptb_application_registers_message_and_callback_handlers(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    builder = FakeApplicationBuilder()

    app = build_ptb_application(
        token="token-1",
        conn=conn,
        tenant_id="tenant-a",
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
    )

    assert app is builder.app
    assert builder.value == "token-1"
    assert len(app.handlers) == 2
    assert app.handlers[0].args[0] == "all"


def test_ptb_sender_sends_outbound_message_with_fake_bot():
    async def run():
        bot = FakeBot()
        sender = PtbTelegramSender(bot)
        result = await sender.send(
            OutboundMessage(
                id="out-1",
                tenant_id="tenant-a",
                channel="telegram",
                destination_id="456",
                text="hello",
                payload={"parse_mode": "HTML", "disable_web_page_preview": True},
            )
        )
        return bot, result

    bot, result = asyncio.run(run())

    assert bot.calls == [
        {
            "chat_id": 456,
            "text": "hello",
            "parse_mode": "HTML",
            "reply_markup": None,
            "disable_web_page_preview": True,
        }
    ]
    assert result.metadata == {"message_id": 42, "chat_id": "456"}


def test_build_ptb_inline_keyboard_requires_optional_dependency_when_missing():
    with pytest.raises(RuntimeError, match="Telegram adapter dependencies are required"):
        build_ptb_inline_keyboard([[{"text": "OK", "callback_data": "ok"}]])
