"""SQLite adapter for conversation history."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.conversation_history import store
from soveren_agent_platform.conversation_history.contracts import (
    ConversationMessage,
    ConversationSearchHit,
    MessageDirection,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteConversationHistoryStore(SQLiteAdapter):
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
    ) -> tuple[str, bool]:
        return await run_sqlite(
            self._conn,
            store.record_message,
            tenant_id=tenant_id,
            source_id=source_id,
            channel=channel,
            direction=direction,
            text=text,
            source_message_id=source_message_id,
            occurred_at=occurred_at,
            author_id=author_id,
            author_username=author_username,
            author_display_name=author_display_name,
            source_event_id=source_event_id,
            metadata=metadata,
        )

    async def get(
        self,
        message_id: str,
        *,
        tenant_id: str,
        source_id: str,
    ) -> ConversationMessage | None:
        return await run_sqlite(
            self._conn,
            store.get_message,
            message_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )

    async def recent(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 20,
        before_message_id: str | None = None,
    ) -> list[ConversationMessage]:
        return await run_sqlite(
            self._conn,
            store.recent_messages,
            tenant_id=tenant_id,
            source_id=source_id,
            limit=limit,
            before_message_id=before_message_id,
        )

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
    ) -> list[ConversationSearchHit]:
        return await run_sqlite(
            self._conn,
            store.search_messages,
            tenant_id=tenant_id,
            source_id=source_id,
            query=query,
            limit=limit,
            context_before=context_before,
            context_after=context_after,
            since=since,
            until=until,
        )

    async def prune_history_before(
        self,
        *,
        tenant_id: str,
        source_id: str,
        before: int,
        limit: int = 1000,
    ) -> int:
        return await run_sqlite(
            self._conn,
            store.prune_history_before,
            tenant_id=tenant_id,
            source_id=source_id,
            before=before,
            limit=limit,
        )
