import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from agent_platform.outbound.contracts import OutboundMessage
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite
from agent_platform.telegram.ptb import (
    PtbTelegramSender,
    build_ptb_inline_keyboard,
    enqueue_ptb_update,
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
    with pytest.raises(RuntimeError, match="python-telegram-bot is required"):
        build_ptb_inline_keyboard([[{"text": "OK", "callback_data": "ok"}]])

