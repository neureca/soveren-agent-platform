"""Contracts for durable conversation history."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

MessageDirection = Literal["inbound", "outbound"]


@dataclass(frozen=True, slots=True)
class ConversationMessage:
    id: str
    tenant_id: str
    source_id: str
    channel: str
    direction: MessageDirection
    text: str
    source_message_id: str
    occurred_at: int
    author_id: str | None = None
    author_username: str | None = None
    author_display_name: str | None = None
    source_event_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: int | None = None


@dataclass(frozen=True, slots=True)
class ConversationSearchHit:
    match: ConversationMessage
    context: tuple[ConversationMessage, ...]


class ConversationHistoryStore(Protocol):
    async def record(
        self,
        *,
        tenant_id: str,
        source_id: str,
        channel: str,
        direction: MessageDirection,
        text: str,
        source_message_id: str,
        occurred_at: int,
        author_id: str | None = None,
        author_username: str | None = None,
        author_display_name: str | None = None,
        source_event_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[str, bool]: ...

    async def get(
        self,
        message_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> ConversationMessage | None: ...

    async def recent(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 20,
        before_message_id: str | None = None,
    ) -> list[ConversationMessage]: ...

    async def search(
        self,
        *,
        tenant_id: str,
        source_id: str,
        query: str,
        limit: int = 10,
        context_before: int = 3,
        context_after: int = 3,
        since: int | None = None,
        until: int | None = None,
    ) -> list[ConversationSearchHit]: ...

    async def prune_history_before(
        self,
        *,
        tenant_id: str,
        source_id: str,
        before: int,
        limit: int = 1000,
    ) -> int: ...
