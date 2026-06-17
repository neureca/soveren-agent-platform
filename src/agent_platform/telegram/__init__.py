"""Telegram communication interface for the platform."""

from agent_platform.telegram.contracts import TelegramInboundMessage
from agent_platform.telegram.ingress import enqueue_telegram_message
from agent_platform.telegram.ptb import PtbTelegramSender, enqueue_ptb_update, update_to_inbound_message

__all__ = [
    "PtbTelegramSender",
    "TelegramInboundMessage",
    "enqueue_ptb_update",
    "enqueue_telegram_message",
    "update_to_inbound_message",
]
