"""Optional python-telegram-bot adapter.

This module intentionally uses duck typing for PTB objects so importing
`soveren_agent_platform` does not require `python-telegram-bot` as a core dependency.
"""
from __future__ import annotations

import inspect
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.agent.contracts import AgentHandler
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.batching.rules import DEFAULT_MAX_COUNT, DEFAULT_MAX_WINDOW_S, DEFAULT_QUIET_WINDOW_S
from soveren_agent_platform.outbound.contracts import OutboundMessage, SendResult
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram.contracts import TelegramInboundMessage
from soveren_agent_platform.telegram.ingress import enqueue_telegram_message

Hook = Callable[..., Any]


@dataclass(slots=True)
class TelegramRuntimeHooks:
    on_update_enqueued: Hook | None = None
    on_callback_query: Hook | None = None


@dataclass(frozen=True, slots=True)
class TelegramAccessPolicy:
    allowed_chat_ids: frozenset[int] | None = None
    allowed_user_ids: frozenset[int] | None = None

    def allows(self, *, chat_id: int, user_id: int | None) -> bool:
        if self.allowed_chat_ids is not None and chat_id not in self.allowed_chat_ids:
            return False
        if self.allowed_user_ids is not None and user_id not in self.allowed_user_ids:
            return False
        return True


class TelegramSender:
    """Outbound `ChannelSender` implementation for Telegram bots."""

    def __init__(self, bot: Any) -> None:
        self.bot = bot

    async def send(self, message: OutboundMessage) -> SendResult:
        payload = message.payload or {}
        sent = await self.bot.send_message(
            chat_id=_coerce_chat_id(message.destination_id),
            text=message.text,
            parse_mode=payload.get("parse_mode"),
            reply_markup=payload.get("reply_markup") or build_telegram_inline_keyboard(payload.get("buttons")),
            disable_web_page_preview=payload.get("disable_web_page_preview"),
        )
        return SendResult(
            metadata={
                "message_id": getattr(sent, "message_id", None),
                "chat_id": message.destination_id,
            }
        )


@dataclass(slots=True)
class TelegramAgentApp:
    """High-level polling runtime for Telegram-backed agent applications."""

    platform: AgentPlatformApp
    telegram_app: Any
    conn: sqlite3.Connection
    _started: bool = False
    _closed: bool = False

    async def start(self) -> None:
        if self._started:
            return
        if self._closed:
            raise RuntimeError("Telegram agent app cannot be restarted after stop")
        await self.platform.start()
        try:
            await _call_method(self.telegram_app, "initialize")
            await _call_method(self.telegram_app, "start")
            updater = getattr(self.telegram_app, "updater", None)
            if updater is None:
                raise RuntimeError("Telegram polling application does not expose an updater")
            await _call_method(updater, "start_polling")
        except BaseException:
            await self.platform.stop()
            self.conn.close()
            self._closed = True
            raise
        self._started = True

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        if not self._started:
            return
        try:
            updater = getattr(self.telegram_app, "updater", None)
            if updater is not None:
                await _call_method(updater, "stop")
            await _call_method(self.telegram_app, "stop")
            await _call_method(self.telegram_app, "shutdown")
        finally:
            await self.platform.stop(timeout_s=timeout_s)
            self.conn.close()
            self._started = False
            self._closed = True

    async def run(self, stop_event: Any | None = None) -> None:
        await self.start()
        try:
            if stop_event is None:
                await _never_stop()
            else:
                await _maybe_await(stop_event.wait())
        finally:
            await self.stop()

    async def __aenter__(self) -> "TelegramAgentApp":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.stop()


def create_telegram_agent_app(
    *,
    token: str,
    db_path: Path,
    tenant_id: str,
    handler: AgentHandler,
    actions: ActionRegistry | None = None,
    outbound: OutboundRegistry | None = None,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    allowed_chat_ids: Iterable[int] | None = None,
    allowed_user_ids: Iterable[int] | None = None,
    quiet_window_s: int = DEFAULT_QUIET_WINDOW_S,
    max_window_s: int = DEFAULT_MAX_WINDOW_S,
    max_count: int = DEFAULT_MAX_COUNT,
    bootstrap_storage: bool = True,
    application_builder: Any | None = None,
    message_handler_cls: Any | None = None,
    callback_query_handler_cls: Any | None = None,
    message_filter: Any | None = None,
) -> TelegramAgentApp:
    conn = open_sqlite(db_path)
    try:
        telegram_app = build_telegram_polling_application(
            token=token,
            conn=conn,
            tenant_id=tenant_id,
            hooks=hooks,
            access_policy=_telegram_access_policy(
                access_policy,
                allowed_chat_ids=allowed_chat_ids,
                allowed_user_ids=allowed_user_ids,
            ),
            application_builder=application_builder,
            message_handler_cls=message_handler_cls,
            callback_query_handler_cls=callback_query_handler_cls,
            message_filter=message_filter,
        )
    except BaseException:
        conn.close()
        raise
    action_registry = actions or ActionRegistry()
    outbound_registry = outbound or OutboundRegistry()
    outbound_registry.register("telegram", TelegramSender(telegram_app.bot))
    platform = (
        AgentPlatformApp(db_path=db_path, bootstrap_storage=bootstrap_storage)
        .use_batching(
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )
        .use_agent(handler=handler)
        .use_actions(registry=action_registry)
        .use_outbound(registry=outbound_registry, channels=["telegram"])
    )
    return TelegramAgentApp(
        platform=platform,
        telegram_app=telegram_app,
        conn=conn,
    )


def update_to_inbound_message(
    update: Any,
    *,
    tenant_id: str,
    access_policy: TelegramAccessPolicy | None = None,
) -> TelegramInboundMessage | None:
    """Normalize a Telegram Update-like object into a platform TelegramInboundMessage."""
    message = getattr(update, "effective_message", None)
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if message is None or chat is None:
        return None
    chat_id = int(getattr(chat, "id"))
    user_id = getattr(user, "id", None)
    if access_policy is not None and not access_policy.allows(chat_id=chat_id, user_id=user_id):
        return None

    text = getattr(message, "text", None) or getattr(message, "caption", None)
    date = _timestamp(getattr(message, "date", None))
    raw_payload = update.to_dict() if hasattr(update, "to_dict") else {}
    payload = {
        "date": date,
        "message_id": getattr(message, "message_id", None),
        "from_first_name": getattr(user, "first_name", None),
        "from_username": getattr(user, "username", None),
        "raw": raw_payload,
    }
    return TelegramInboundMessage(
        tenant_id=tenant_id,
        chat_id=chat_id,
        update_id=int(getattr(update, "update_id")),
        user_id=user_id,
        username=getattr(user, "username", None),
        text=text,
        payload=payload,
    )


def enqueue_telegram_update(
    conn: sqlite3.Connection,
    update: Any,
    *,
    tenant_id: str,
    access_policy: TelegramAccessPolicy | None = None,
) -> str | None:
    message = update_to_inbound_message(update, tenant_id=tenant_id, access_policy=access_policy)
    if message is None:
        return None
    return enqueue_telegram_message(conn, message)


async def handle_telegram_message_update(
    conn: sqlite3.Connection,
    update: Any,
    context: Any,
    *,
    tenant_id: str,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
) -> str | None:
    message = update_to_inbound_message(update, tenant_id=tenant_id, access_policy=access_policy)
    if message is None:
        return None
    event_id = enqueue_telegram_message(conn, message)
    await _call_hook(
        hooks.on_update_enqueued if hooks else None,
        event_id=event_id,
        message=message,
        update=update,
        context=context,
    )
    return event_id


async def handle_telegram_callback_query(
    update: Any,
    context: Any,
    *,
    hooks: TelegramRuntimeHooks | None = None,
) -> Any:
    query = getattr(update, "callback_query", None)
    if query is not None and hasattr(query, "answer"):
        await _maybe_await(query.answer())
    return await _call_hook(
        hooks.on_callback_query if hooks else None,
        query=query,
        update=update,
        context=context,
        data=getattr(query, "data", None) if query is not None else None,
    )


def build_telegram_polling_application(
    *,
    token: str,
    conn: sqlite3.Connection,
    tenant_id: str,
    hooks: TelegramRuntimeHooks | None = None,
    access_policy: TelegramAccessPolicy | None = None,
    application_builder: Any | None = None,
    message_handler_cls: Any | None = None,
    callback_query_handler_cls: Any | None = None,
    message_filter: Any | None = None,
) -> Any:
    if application_builder is None or message_handler_cls is None or callback_query_handler_cls is None:
        try:
            from telegram.ext import (  # type: ignore[import-not-found]
                Application,
                CallbackQueryHandler,
                MessageHandler,
                filters,
            )
        except ImportError as exc:
            raise RuntimeError(
                "Telegram adapter dependencies are required to build a Telegram polling application"
            ) from exc
        application_builder = application_builder or Application.builder()
        message_handler_cls = message_handler_cls or MessageHandler
        callback_query_handler_cls = callback_query_handler_cls or CallbackQueryHandler
        message_filter = message_filter or filters.ALL

    async def on_message(update: Any, context: Any) -> str | None:
        return await handle_telegram_message_update(
            conn,
            update,
            context,
            tenant_id=tenant_id,
            hooks=hooks,
            access_policy=access_policy,
        )

    async def on_callback(update: Any, context: Any) -> Any:
        return await handle_telegram_callback_query(update, context, hooks=hooks)

    app = application_builder.token(token).build()
    app.add_handler(message_handler_cls(message_filter, on_message))
    app.add_handler(callback_query_handler_cls(on_callback))
    return app


def build_telegram_inline_keyboard(buttons: list[list[dict[str, str]]] | None) -> Any:
    if not buttons:
        return None
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Telegram adapter dependencies are required to build inline keyboard markup"
        ) from exc
    rows = [
        [
            InlineKeyboardButton(text=button["text"], callback_data=button["callback_data"])
            for button in row
        ]
        for row in buttons
    ]
    return InlineKeyboardMarkup(rows)


PtbRuntimeHooks = TelegramRuntimeHooks
PtbTelegramAccessPolicy = TelegramAccessPolicy
PtbTelegramSender = TelegramSender
PtbTelegramAgentApp = TelegramAgentApp
build_ptb_application = build_telegram_polling_application
build_ptb_inline_keyboard = build_telegram_inline_keyboard
create_ptb_agent_app = create_telegram_agent_app
enqueue_ptb_update = enqueue_telegram_update
handle_ptb_callback_query = handle_telegram_callback_query
handle_ptb_message_update = handle_telegram_message_update


def _telegram_access_policy(
    access_policy: TelegramAccessPolicy | None,
    *,
    allowed_chat_ids: Iterable[int] | None,
    allowed_user_ids: Iterable[int] | None,
) -> TelegramAccessPolicy | None:
    if access_policy is not None and (allowed_chat_ids is not None or allowed_user_ids is not None):
        raise ValueError("pass either access_policy or allowed_chat_ids/allowed_user_ids")
    if access_policy is not None:
        return access_policy
    if allowed_chat_ids is None and allowed_user_ids is None:
        return None
    chat_ids = frozenset(int(chat_id) for chat_id in allowed_chat_ids) if allowed_chat_ids is not None else None
    user_ids = frozenset(int(user_id) for user_id in allowed_user_ids) if allowed_user_ids is not None else None
    return TelegramAccessPolicy(
        allowed_chat_ids=chat_ids,
        allowed_user_ids=user_ids,
    )


def _timestamp(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_chat_id(value: str) -> int | str:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


async def _call_hook(hook: Hook | None, **kwargs: Any) -> Any:
    if hook is None:
        return None
    return await _maybe_await(hook(**kwargs))


async def _call_method(target: Any, name: str) -> Any:
    method = getattr(target, name)
    return await _maybe_await(method())


async def _never_stop() -> None:
    import asyncio

    await asyncio.Event().wait()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
