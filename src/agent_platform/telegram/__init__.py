"""Telegram communication interface for the platform."""

from agent_platform.telegram.contracts import TelegramInboundMessage
from agent_platform.telegram.ingress import enqueue_telegram_message

__all__ = ["TelegramInboundMessage", "enqueue_telegram_message"]

