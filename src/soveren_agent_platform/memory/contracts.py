"""Contracts for durable app-neutral memory records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    id: str
    tenant_id: str
    scope: str
    subject_id: str
    kind: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    source_id: str | None = None
    source_event_id: str | None = None
    created_by: str | None = None
    idempotency_key: str | None = None
    expires_at: int | None = None
    deleted_at: int | None = None
    created_at: int | None = None
    updated_at: int | None = None


class MemoryStore(Protocol):
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
        ...

    async def get(self, memory_id: str, *, tenant_id: str) -> MemoryRecord | None:
        ...

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
        ...

    async def forget(self, memory_id: str, *, tenant_id: str) -> bool:
        ...
