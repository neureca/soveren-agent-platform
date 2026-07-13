"""Telegram-specific normalized interface contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class TelegramInboundMessage:
    tenant_id: str
    chat_id: int
    update_id: int
    user_id: int | None
    username: str | None
    text: str | None
    payload: dict[str, Any]


class TelegramChatRegistry(Protocol):
    async def is_registered(self, *, tenant_id: str, chat_id: int) -> bool: ...

    async def register(
        self,
        *,
        tenant_id: str,
        chat_id: int,
        registered_by_user_id: int,
    ) -> None: ...
