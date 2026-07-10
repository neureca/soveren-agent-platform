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
    accepted_at: int | None = None
    attempts: int = 0
    max_attempts: int = 3
    run_after: int = 0
    backend_receipt: dict[str, Any] | None = None


@dataclass(slots=True)
class RuntimeSessionEvent:
    id: str
    session_id: str
    direction: str
    payload_text: str
    action_id: str | None = None
    marker: str | None = None
    created_at: int | None = None


@dataclass(slots=True)
class RuntimeSessionContextSnapshot:
    id: str
    session_id: str
    version: int
    summary: str
    keywords: list[str]
    files: list[str]
    cwd: str
    confidence: float
    source_event_id: str | None = None
    source_range: dict[str, Any] | None = None
    entities: list[str] | None = None
    branch: str | None = None
    topic_key: str | None = None
    open_questions: list[str] | None = None
    last_user_intent: str | None = None
    last_agent_state: str | None = None
    created_at: int | None = None


@dataclass(slots=True)
class SessionInspection:
    session_id: str
    payload_text: str
    direction: str = "output"
    marker: str | None = None
    metadata: dict[str, Any] | None = None


class SessionStore(Protocol):
    async def create(
        self,
        *,
        tenant_id: str,
        source_id: str,
        kind: str,
        backend: str,
        backend_session_id: str,
        owner_id: str | None = None,
        title: str = "",
        cwd: str = "",
        status: str = "idle",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        ...

    async def get(self, session_id: str) -> RuntimeSession | None:
        ...

    async def list_active(self, *, tenant_id: str, limit: int) -> list[RuntimeSession]:
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


class SessionEventStore(Protocol):
    async def record(
        self,
        *,
        session_id: str,
        direction: str,
        payload_text: str,
        action_id: str | None = None,
        marker: str | None = None,
    ) -> str:
        ...

    async def recent(self, session_id: str, *, limit: int) -> list[RuntimeSessionEvent]:
        ...


class SessionSnapshotStore(Protocol):
    async def refresh(self, session_id: str) -> str | None:
        ...

    async def latest(self, session_id: str) -> RuntimeSessionContextSnapshot | None:
        ...


class SessionInspector(Protocol):
    async def inspect(self, session: RuntimeSession) -> SessionInspection | None:
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

    async def mark_accepted(
        self,
        mailbox_id: str,
        *,
        backend_receipt: dict[str, Any] | None = None,
    ) -> None:
        ...

    async def complete_delivery(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        result: dict[str, Any],
        session_status: str,
        current_action_id: str | None = None,
    ) -> None:
        ...

    async def fail_delivery(self, mailbox_id: str, *, session_id: str, last_error: str) -> None:
        ...

    async def defer_accepted(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        current_action_id: str | None,
        last_error: str,
        retry_after_s: int,
    ) -> bool:
        """Delay capture retry and return whether the item became terminal."""
        ...

    async def defer_pending(
        self,
        mailbox_id: str,
        *,
        session_id: str,
        current_action_id: str | None,
        last_error: str,
        retry_after_s: int,
    ) -> None:
        """Delay a still-running accepted delivery without consuming a failure attempt."""
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
