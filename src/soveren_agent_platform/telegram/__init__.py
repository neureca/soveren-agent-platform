"""Telegram communication interface for the platform."""

from soveren_agent_platform.telegram.contracts import TelegramInboundMessage
from soveren_agent_platform.telegram.ingress import enqueue_telegram_message
from soveren_agent_platform.telegram.ptb import (
    PtbRuntimeHooks,
    PtbTelegramSender,
    build_ptb_application,
    enqueue_ptb_update,
    handle_ptb_callback_query,
    handle_ptb_message_update,
    update_to_inbound_message,
)

__all__ = [
    "PtbRuntimeHooks",
    "PtbTelegramSender",
    "TelegramInboundMessage",
    "build_ptb_application",
    "enqueue_ptb_update",
    "enqueue_telegram_message",
    "handle_ptb_callback_query",
    "handle_ptb_message_update",
    "update_to_inbound_message",
]
