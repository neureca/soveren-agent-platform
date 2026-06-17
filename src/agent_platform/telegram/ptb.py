"""Optional python-telegram-bot adapter.

This module intentionally uses duck typing for PTB objects so importing
`agent_platform` does not require `python-telegram-bot` as a core dependency.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from agent_platform.outbound.contracts import OutboundMessage, SendResult
from agent_platform.telegram.contracts import TelegramInboundMessage
from agent_platform.telegram.ingress import enqueue_telegram_message


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


def build_ptb_inline_keyboard(buttons: list[list[dict[str, str]]] | None) -> Any:
    if not buttons:
        return None
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
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

