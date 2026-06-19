"""Telegram-specific normalized interface contracts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class TelegramInboundMessage:
    tenant_id: str
    chat_id: int
    update_id: int
    user_id: int | None
    username: str | None
    text: str | None
    payload: dict[str, Any]

