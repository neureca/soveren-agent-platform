"""SQLite adapter for app-neutral memory records."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from soveren_agent_platform.memory import store
from soveren_agent_platform.memory.contracts import MemoryRecord


class SQLiteMemoryStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def remember(
        self,
        *,
        tenant_id: str,
        scope: str,
        subject_id: str,
        text: str,
        kind: str = "note",
        metadata: dict[str, Any] | None = None,
        confidence: float = 1.0,
        source_id: str | None = None,
        source_event_id: str | None = None,
        created_by: str | None = None,
        idempotency_key: str | None = None,
        expires_at: int | None = None,
    ) -> tuple[str, bool]:
        return await asyncio.to_thread(
            store.remember,
            self.conn,
            tenant_id=tenant_id,
            scope=scope,
            subject_id=subject_id,
            text=text,
            kind=kind,
            metadata=metadata,
            confidence=confidence,
            source_id=source_id,
            source_event_id=source_event_id,
            created_by=created_by,
            idempotency_key=idempotency_key,
            expires_at=expires_at,
        )

    async def get(self, memory_id: str, *, tenant_id: str) -> MemoryRecord | None:
        return await asyncio.to_thread(store.get_memory, self.conn, memory_id, tenant_id=tenant_id)

    async def search(
        self,
        *,
        tenant_id: str,
        query: str = "",
        scope: str | None = None,
        subject_id: str | None = None,
        kind: str | None = None,
        limit: int = 10,
    ) -> list[MemoryRecord]:
        return await asyncio.to_thread(
            store.search_memory,
            self.conn,
            tenant_id=tenant_id,
            query=query,
            scope=scope,
            subject_id=subject_id,
            kind=kind,
            limit=limit,
        )

    async def forget(self, memory_id: str, *, tenant_id: str) -> bool:
        return await asyncio.to_thread(store.forget_memory, self.conn, memory_id, tenant_id=tenant_id)
