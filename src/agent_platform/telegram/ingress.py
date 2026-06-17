"""Telegram ingress helpers that emit platform queue events."""
from __future__ import annotations

import sqlite3

from agent_platform.queue.durable import enqueue
from agent_platform.telegram.contracts import TelegramInboundMessage


def enqueue_telegram_message(
    conn: sqlite3.Connection,
    message: TelegramInboundMessage,
    *,
    recipient: str = "batching",
    message_type: str = "InboundMessageReceived",
) -> str | None:
    """Convert a normalized Telegram message into a durable batching event."""
    return enqueue(
        conn,
        tenant_id=message.tenant_id,
        recipient=recipient,
        message_type=message_type,
        payload={
            "channel": "telegram",
            "source_id": str(message.chat_id),
            "raw_event_id": f"telegram:{message.chat_id}:{message.update_id}",
            "source_event_id": str(message.update_id),
            "chat_id": message.chat_id,
            "update_id": message.update_id,
            "user_id": message.user_id,
            "username": message.username,
            "text": message.text,
            "message_at": message.payload.get("message_at") or message.payload.get("date"),
            "payload": message.payload,
        },
        idempotency_key=f"telegram:{message.chat_id}:{message.update_id}",
        correlation_id=f"telegram:{message.chat_id}",
    )
