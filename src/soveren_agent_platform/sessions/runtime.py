"""Typed composition for opening durable execution sessions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.sessions.backend import OpenResult, OpenSpec, ensure_conversation_boundary
from soveren_agent_platform.sessions.contracts import SessionStore
from soveren_agent_platform.sessions.registry import SessionBackendMapping, normalize_session_backends


@dataclass(frozen=True, slots=True)
class SessionOpenRequest:
    tenant_id: str
    source_id: str
    kind: str
    backend: str
    cwd: str
    owner_id: str | None = None
    title: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SessionOpenResult:
    session_id: str
    backend_session_id: str
    session_handle: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class SessionRuntime:
    """Open a backend session and persist its generalized platform handle."""

    def __init__(self, store: SessionStore, backends: SessionBackendMapping) -> None:
        self.store = store
        self.backends = normalize_session_backends(backends)

    async def open_session(self, request: SessionOpenRequest) -> SessionOpenResult:
        backend = self.backends.get(request.backend)
        if backend is None:
            raise KeyError(f"no session backend registered for {request.backend!r}")
        ensure_conversation_boundary(
            backend,
            request.tenant_id,
            request.source_id,
            resource_name=f"session backend {request.backend!r}",
        )
        opened = await backend.open(
            OpenSpec(
                kind=request.kind,
                cwd=request.cwd,
                title=request.title,
                metadata=request.metadata,
                conversation_scope=ConversationScope(
                    tenant_id=request.tenant_id,
                    source_id=request.source_id,
                ),
            )
        )
        try:
            session_id = await self._persist(request, opened)
        except BaseException as persist_error:
            try:
                await backend.close(opened.backend_session_id)
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "runtime session persistence and backend cleanup failed",
                    [persist_error, cleanup_error],
                ) from persist_error
            raise
        return SessionOpenResult(
            session_id=session_id,
            backend_session_id=opened.backend_session_id,
            session_handle=opened.session_handle,
            metadata=opened.metadata or {},
        )

    async def _persist(self, request: SessionOpenRequest, opened: OpenResult) -> str:
        metadata = {**request.metadata, **(opened.metadata or {})}
        return await self.store.create(
            tenant_id=request.tenant_id,
            source_id=request.source_id,
            owner_id=request.owner_id,
            kind=request.kind,
            backend=request.backend,
            backend_session_id=opened.backend_session_id,
            title=request.title,
            cwd=str(metadata.get("sandbox_cwd") or metadata.get("cwd") or request.cwd),
            status="idle",
            metadata=metadata,
        )
