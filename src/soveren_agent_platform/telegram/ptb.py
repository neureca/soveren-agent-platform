"""Optional python-telegram-bot adapter.

This module intentionally uses duck typing for PTB objects so importing
`soveren_agent_platform` does not require `python-telegram-bot` as a core dependency.
"""
from __future__ import annotations

import inspect
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from soveren_agent_platform.outbound.contracts import OutboundMessage, SendResult
from soveren_agent_platform.telegram.contracts import TelegramInboundMessage
from soveren_agent_platform.telegram.ingress import enqueue_telegram_message

Hook = Callable[..., Any]


@dataclass(slots=True)
class PtbRuntimeHooks:
    on_update_enqueued: Hook | None = None
    on_callback_query: Hook | None = None


class PtbTelegramSender:
    """Outbound `ChannelSender` implementation for python-telegram-bot bots."""

    def __init__(self, bot: Any) -> None:
        self.bot = bot

    async def send(self, message: OutboundMessage) -> SendResult:
        payload = message.payload or {}
        sent = await self.bot.send_message(
            chat_id=_coerce_chat_id(message.destination_id),
            text=message.text,
            parse_mode=payload.get("parse_mode"),
            reply_markup=payload.get("reply_markup") or build_ptb_inline_keyboard(payload.get("buttons")),
            disable_web_page_preview=payload.get("disable_web_page_preview"),
        )
        return SendResult(
            metadata={
                "message_id": getattr(sent, "message_id", None),
                "chat_id": message.destination_id,
            }
        )


def update_to_inbound_message(update: Any, *, tenant_id: str) -> TelegramInboundMessage | None:
    """Normalize a PTB-like Update into a platform TelegramInboundMessage."""
    message = getattr(update, "effective_message", None)
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if message is None or chat is None:
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
        chat_id=int(getattr(chat, "id")),
        update_id=int(getattr(update, "update_id")),
        user_id=getattr(user, "id", None),
        username=getattr(user, "username", None),
        text=text,
        payload=payload,
    )


def enqueue_ptb_update(
    conn: sqlite3.Connection,
    update: Any,
    *,
    tenant_id: str,
) -> str | None:
    message = update_to_inbound_message(update, tenant_id=tenant_id)
    if message is None:
        return None
    return enqueue_telegram_message(conn, message)


async def handle_ptb_message_update(
    conn: sqlite3.Connection,
    update: Any,
    context: Any,
    *,
    tenant_id: str,
    hooks: PtbRuntimeHooks | None = None,
) -> str | None:
    message = update_to_inbound_message(update, tenant_id=tenant_id)
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


async def handle_ptb_callback_query(
    update: Any,
    context: Any,
    *,
    hooks: PtbRuntimeHooks | None = None,
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


def build_ptb_application(
    *,
    token: str,
    conn: sqlite3.Connection,
    tenant_id: str,
    hooks: PtbRuntimeHooks | None = None,
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
                "python-telegram-bot is required to build a PTB application"
            ) from exc
        application_builder = application_builder or Application.builder()
        message_handler_cls = message_handler_cls or MessageHandler
        callback_query_handler_cls = callback_query_handler_cls or CallbackQueryHandler
        message_filter = message_filter or filters.ALL

    async def on_message(update: Any, context: Any) -> str | None:
        return await handle_ptb_message_update(
            conn,
            update,
            context,
            tenant_id=tenant_id,
            hooks=hooks,
        )

    async def on_callback(update: Any, context: Any) -> Any:
        return await handle_ptb_callback_query(update, context, hooks=hooks)

    app = application_builder.token(token).build()
    app.add_handler(message_handler_cls(message_filter, on_message))
    app.add_handler(callback_query_handler_cls(on_callback))
    return app


def build_ptb_inline_keyboard(buttons: list[list[dict[str, str]]] | None) -> Any:
    if not buttons:
        return None
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "python-telegram-bot is required to build inline keyboard markup"
        ) from exc
    rows = [
        [
            InlineKeyboardButton(text=button["text"], callback_data=button["callback_data"])
            for button in row
        ]
        for row in buttons
    ]
    return InlineKeyboardMarkup(rows)


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


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
