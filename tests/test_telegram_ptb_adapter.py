import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from soveren_agent_platform.outbound.contracts import OutboundMessage
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import (
    TelegramAccessPolicy,
    TelegramAgentApp,
    TelegramChatRegistrationPolicy,
    TelegramRuntimeHooks,
    TelegramSender,
    build_telegram_polling_application,
    create_telegram_agent_app,
    enqueue_telegram_update,
    handle_telegram_callback_query,
    handle_telegram_message_update,
    telegram_chat_registered,
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
    def __init__(
        self,
        *,
        update_id: int = 123,
        chat_id: int = 456,
        user_id: int = 789,
        text: str = "привет",
    ) -> None:
        self.update_id = update_id
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=user_id, username="ivan", first_name="Ivan")
        self.effective_message = SimpleNamespace(
            message_id=10,
            text=text,
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


class FakeUpdater:
    def __init__(self) -> None:
        self.calls = []

    async def start_polling(self):
        self.calls.append("start_polling")

    async def stop(self):
        self.calls.append("stop")


class FakeCallbackQuery:
    data = "approve:123"

    def __init__(self) -> None:
        self.answered = False

    async def answer(self):
        self.answered = True


class FakeApplication:
    def __init__(self) -> None:
        self.handlers = []
        self.bot = FakeBot()
        self.updater = FakeUpdater()
        self.calls = []

    def add_handler(self, handler):
        self.handlers.append(handler)

    async def initialize(self):
        self.calls.append("initialize")

    async def start(self):
        self.calls.append("start")

    async def stop(self):
        self.calls.append("stop")

    async def shutdown(self):
        self.calls.append("shutdown")


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


class NoopAgentHandler:
    async def handle(self, event):
        return None


class FakePlatformApp:
    last_instance = None

    def __init__(self, *, db_path, bootstrap_storage=True) -> None:
        self.db_path = db_path
        self.bootstrap_storage = bootstrap_storage
        self.worker_names = ()
        self.calls = []
        FakePlatformApp.last_instance = self

    def use_batching(self, **kwargs):
        self.calls.append(("use_batching", kwargs))
        return self

    def use_agent(self, *, handler, **kwargs):
        self.calls.append(("use_agent", {"handler": handler, **kwargs}))
        return self

    def use_actions(self, *, registry, **kwargs):
        self.calls.append(("use_actions", {"registry": registry, **kwargs}))
        return self

    def use_outbound(self, *, registry, channels):
        self.calls.append(("use_outbound", {"registry": registry, "channels": tuple(channels)}))
        return self

    async def start(self):
        return None

    async def stop(self, *, timeout_s=5.0):
        return None


def test_public_telegram_names_hide_ptb_implementation_details():
    assert TelegramAccessPolicy.__name__ == "TelegramAccessPolicy"
    assert TelegramAgentApp.__name__ == "TelegramAgentApp"
    assert TelegramChatRegistrationPolicy.__name__ == "TelegramChatRegistrationPolicy"
    assert TelegramRuntimeHooks is PtbRuntimeHooks
    assert TelegramSender is PtbTelegramSender
    assert build_telegram_polling_application is build_ptb_application
    assert enqueue_telegram_update is enqueue_ptb_update
    assert handle_telegram_callback_query is handle_ptb_callback_query
    assert handle_telegram_message_update is handle_ptb_message_update
    assert TelegramSender.__name__ == "TelegramSender"
    assert build_telegram_polling_application.__name__ == "build_telegram_polling_application"
    assert enqueue_telegram_update.__name__ == "enqueue_telegram_update"


def test_update_to_inbound_message_applies_access_policy():
    assert update_to_inbound_message(
        FakeUpdate(),
        tenant_id="tenant-a",
        access_policy=TelegramAccessPolicy(allowed_chat_ids=frozenset({456})),
    ) is not None
    assert update_to_inbound_message(
        FakeUpdate(),
        tenant_id="tenant-a",
        access_policy=TelegramAccessPolicy(allowed_chat_ids=frozenset({999})),
    ) is None
    assert update_to_inbound_message(
        FakeUpdate(),
        tenant_id="tenant-a",
        access_policy=TelegramAccessPolicy(allowed_user_ids=frozenset({999})),
    ) is None


def test_build_telegram_polling_application_drops_disallowed_updates(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    builder = FakeApplicationBuilder()
    app = build_telegram_polling_application(
        token="token-1",
        conn=conn,
        tenant_id="tenant-a",
        access_policy=TelegramAccessPolicy(allowed_chat_ids=frozenset({999})),
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
    )

    event_id = asyncio.run(app.handlers[0].callback(FakeUpdate(), SimpleNamespace()))

    assert event_id is None
    assert conn.execute("SELECT COUNT(*) AS c FROM event_queue").fetchone()["c"] == 0


def test_registration_policy_registers_trusted_chat_then_allows_messages(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    calls = []

    async def on_chat_registered(**kwargs):
        calls.append(kwargs)

    registration = TelegramChatRegistrationPolicy(trusted_user_ids=frozenset({789}))

    registration_event = asyncio.run(handle_telegram_message_update(
        conn,
        FakeUpdate(text="/register"),
        SimpleNamespace(),
        tenant_id="tenant-a",
        hooks=TelegramRuntimeHooks(on_chat_registered=on_chat_registered),
        registration_policy=registration,
    ))
    message_event = asyncio.run(handle_telegram_message_update(
        conn,
        FakeUpdate(update_id=124, text="сделай отчет"),
        SimpleNamespace(),
        tenant_id="tenant-a",
        registration_policy=registration,
    ))
    repeated_registration_event = asyncio.run(handle_telegram_message_update(
        conn,
        FakeUpdate(update_id=125, text="/register"),
        SimpleNamespace(),
        tenant_id="tenant-a",
        registration_policy=registration,
    ))

    assert registration_event is None
    assert message_event is not None
    assert repeated_registration_event is None
    assert telegram_chat_registered(conn, tenant_id="tenant-a", chat_id=456)
    assert calls[0]["message"].chat_id == 456
    assert conn.execute("SELECT COUNT(*) AS c FROM event_queue").fetchone()["c"] == 1


def test_registration_policy_rejects_untrusted_registration(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    event_id = asyncio.run(handle_telegram_message_update(
        conn,
        FakeUpdate(text="/register"),
        SimpleNamespace(),
        tenant_id="tenant-a",
        registration_policy=TelegramChatRegistrationPolicy(trusted_user_ids=frozenset({111})),
    ))

    assert event_id is None
    assert not telegram_chat_registered(conn, tenant_id="tenant-a", chat_id=456)
    assert conn.execute("SELECT COUNT(*) AS c FROM event_queue").fetchone()["c"] == 0


def test_create_telegram_agent_app_passes_registration_user_ids(tmp_path):
    builder = FakeApplicationBuilder()
    runtime = create_telegram_agent_app(
        token="token-1",
        db_path=tmp_path / "app.db",
        tenant_id="tenant-a",
        handler=NoopAgentHandler(),
        registration_user_ids=[789],
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
    )
    apply_platform_migrations(runtime.conn)

    registration_event = asyncio.run(builder.app.handlers[0].callback(FakeUpdate(text="/start"), SimpleNamespace()))
    message_event = asyncio.run(builder.app.handlers[0].callback(
        FakeUpdate(update_id=124, text="обычное сообщение"),
        SimpleNamespace(),
    ))

    assert registration_event is None
    assert message_event is not None
    assert telegram_chat_registered(runtime.conn, tenant_id="tenant-a", chat_id=456)


def test_create_telegram_agent_app_passes_batching_and_access_config(tmp_path, monkeypatch):
    import soveren_agent_platform.telegram.ptb as ptb_module

    monkeypatch.setattr(ptb_module, "AgentPlatformApp", FakePlatformApp)
    builder = FakeApplicationBuilder()

    runtime = create_telegram_agent_app(
        token="token-1",
        db_path=tmp_path / "app.db",
        tenant_id="tenant-a",
        handler=NoopAgentHandler(),
        allowed_chat_ids=[456],
        quiet_window_s=5,
        max_window_s=30,
        max_count=3,
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
    )
    apply_platform_migrations(runtime.conn)
    event_id = asyncio.run(builder.app.handlers[0].callback(FakeUpdate(), SimpleNamespace()))

    assert event_id is not None
    assert FakePlatformApp.last_instance.calls[0] == (
        "use_batching",
        {"quiet_window_s": 5, "max_window_s": 30, "max_count": 3},
    )


def test_create_telegram_agent_app_rejects_duplicate_access_policy_inputs(tmp_path):
    with pytest.raises(ValueError, match="either access_policy"):
        create_telegram_agent_app(
            token="token-1",
            db_path=tmp_path / "app.db",
            tenant_id="tenant-a",
            handler=NoopAgentHandler(),
            access_policy=TelegramAccessPolicy(allowed_chat_ids=frozenset({456})),
            allowed_chat_ids=[456],
            application_builder=FakeApplicationBuilder(),
            message_handler_cls=FakeHandler,
            callback_query_handler_cls=FakeHandler,
            message_filter="all",
        )


def test_create_telegram_agent_app_wires_high_level_polling_runtime(tmp_path):
    builder = FakeApplicationBuilder()
    outbound = OutboundRegistry()

    runtime = create_telegram_agent_app(
        token="token-1",
        db_path=tmp_path / "app.db",
        tenant_id="tenant-a",
        handler=NoopAgentHandler(),
        outbound=outbound,
        application_builder=builder,
        message_handler_cls=FakeHandler,
        callback_query_handler_cls=FakeHandler,
        message_filter="all",
    )

    assert isinstance(runtime, TelegramAgentApp)
    assert runtime.telegram_app is builder.app
    assert runtime.platform.worker_names == ("batching", "agent", "actions", "outbound:telegram")
    assert isinstance(outbound.get("telegram"), TelegramSender)


def test_telegram_agent_app_manages_platform_and_polling_lifecycle(tmp_path):
    async def run():
        builder = FakeApplicationBuilder()
        runtime = create_telegram_agent_app(
            token="token-1",
            db_path=tmp_path / "app.db",
            tenant_id="tenant-a",
            handler=NoopAgentHandler(),
            application_builder=builder,
            message_handler_cls=FakeHandler,
            callback_query_handler_cls=FakeHandler,
            message_filter="all",
        )

        await runtime.start()
        assert builder.app.calls == ["initialize", "start"]
        assert builder.app.updater.calls == ["start_polling"]
        assert runtime.platform.worker_names == ("batching", "agent", "actions", "outbound:telegram")

        await runtime.stop()
        with pytest.raises(RuntimeError, match="cannot be restarted"):
            await runtime.start()
        return builder.app

    app = asyncio.run(run())

    assert app.updater.calls == ["start_polling", "stop"]
    assert app.calls == ["initialize", "start", "stop", "shutdown"]


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
