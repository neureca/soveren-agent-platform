"""SQLite adapter for app-neutral memory records."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.memory import store
from soveren_agent_platform.memory.contracts import MemoryRecord
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteMemoryStore(SQLiteAdapter):
    async def remember(
        self,
        *,
        tenant_id: str,
        source_id: str,
        scope: str,
        subject_id: str,
        text: str,
        kind: str = "note",
        metadata: dict[str, Any] | None = None,
        confidence: float = 1.0,
        source_event_id: str | None = None,
        created_by: str | None = None,
        idempotency_key: str | None = None,
        expires_at: int | None = None,
    ) -> tuple[str, bool]:
        return await run_sqlite(
            self._conn,
            store.remember,
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

    async def get(self, memory_id: str, *, tenant_id: str, source_id: str) -> MemoryRecord | None:
        return await run_sqlite(
            self._conn,
            store.get_memory,
            memory_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )

    async def search(
        self,
        *,
        tenant_id: str,
        source_id: str,
        query: str = "",
        scope: str | None = None,
        subject_id: str | None = None,
        kind: str | None = None,
        limit: int = 10,
    ) -> list[MemoryRecord]:
        return await run_sqlite(
            self._conn,
            store.search_memory,
            tenant_id=tenant_id,
            source_id=source_id,
            query=query,
            scope=scope,
            subject_id=subject_id,
            kind=kind,
            limit=limit,
        )

    async def forget(self, memory_id: str, *, tenant_id: str, source_id: str) -> bool:
        return await run_sqlite(
            self._conn,
            store.forget_memory,
            memory_id,
            tenant_id=tenant_id,
            source_id=source_id,
        )
