"""Codex app-server inspector for generalized session indexing."""
from __future__ import annotations

import hashlib

from soveren_agent_platform.sessions.backends.codex_app_server import CodexAppServerBackend
from soveren_agent_platform.sessions.contracts import RuntimeSession, SessionInspection


class CodexThreadInspector:
    """Read Codex thread context without exposing Codex-specific APIs to routers."""

    def __init__(self, backend: CodexAppServerBackend) -> None:
        self.backend = backend

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
