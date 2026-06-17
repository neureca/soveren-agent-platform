"""Session and mailbox storage contracts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class RuntimeSession:
    id: str
    tenant_id: str
    source_id: str
    kind: str
    backend: str
    backend_session_id: str
    status: str
    owner_id: str | None = None
    title: str = ""
    cwd: str = ""
    current_action_id: str | None = None
    last_error: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class MailboxItem:
    id: str
    session_id: str
    tenant_id: str
    source_id: str
    prompt: str
    status: str
    action_id: str | None = None
    source_event_id: str | None = None
    last_error: str | None = None


class SessionStore(Protocol):
    async def get(self, session_id: str) -> RuntimeSession | None:
        ...

    async def set_status(
        self,
        session_id: str,
        status: str,
        *,
        current_action_id: str | None = None,
        last_error: str | None = None,
    ) -> None:
        ...


class SessionMailboxStore(Protocol):
    async def enqueue_prompt(
        self,
        *,
        session_id: str,
        tenant_id: str,
        source_id: str,
        prompt: str,
        action_id: str | None = None,
        source_event_id: str | None = None,
    ) -> tuple[str, bool]:
        ...

    async def ready_session_ids(self, *, tenant_id: str, limit: int) -> list[str]:
        ...

    async def claim_next(self, session_id: str) -> MailboxItem | None:
        ...

    async def mark_sent(self, mailbox_id: str, *, result: dict[str, Any] | None = None) -> None:
        ...

    async def requeue(self, mailbox_id: str, *, last_error: str) -> None:
        ...

    async def mark_failed(self, mailbox_id: str, *, last_error: str) -> None:
        ...

    async def fail_stale_sending(
        self,
        *,
        tenant_id: str,
        older_than_s: int,
        reason: str,
        limit: int,
    ) -> list[MailboxItem]:
        ...
