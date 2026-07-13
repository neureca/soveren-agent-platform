"""Codex app-server inspector for generalized session indexing."""
from __future__ import annotations

import hashlib
from typing import Protocol

from soveren_agent_platform.sessions.backend import (
    CaptureResult,
    ConversationBoundResource,
    TenantBoundResource,
)
from soveren_agent_platform.sessions.contracts import RuntimeSession, SessionInspection


class CodexInspectionBackend(Protocol):
    name: str

    async def capture_thread_history(self, thread_id: str) -> CaptureResult:
        ...


class CodexThreadInspector:
    """Read Codex thread context without exposing Codex-specific APIs to routers."""

    tenant_id: str
    source_id: str

    def __init__(self, backend: CodexInspectionBackend) -> None:
        self.backend = backend
        if isinstance(backend, TenantBoundResource):
            self.tenant_id = backend.tenant_id
        if isinstance(backend, ConversationBoundResource):
            self.source_id = backend.source_id

    async def inspect(self, session: RuntimeSession) -> SessionInspection | None:
        if session.backend != self.backend.name:
            return None
        capture = await self.backend.capture_thread_history(session.backend_session_id)
        text = capture.text.strip()
        if not text:
            return None
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]
        return SessionInspection(
            session_id=session.id,
            direction="output",
            payload_text=text,
            marker=f"codex-thread:{session.backend_session_id}:{digest}",
            metadata={"backend_session_id": session.backend_session_id},
        )
