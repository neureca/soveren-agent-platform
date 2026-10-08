"""Business-facing sandboxed Codex runtime for planner and durable sessions."""

from __future__ import annotations

import asyncio
import posixpath
import threading
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.json_types import JsonObject, require_json_object
from soveren_agent_platform.llm.backends.session import SessionLlmBackend
from soveren_agent_platform.llm.contracts import LlmBackend, LlmRequest, LlmResponse
from soveren_agent_platform.sandbox import (
    CredentialBindingScope,
    CredentialBrokerCapability,
    HttpCredentialBinding,
    SandboxEgressUpstream,
    resolve_sandbox_resource_profile,
)
from soveren_agent_platform.sandbox.contracts import (
    DEFAULT_MAX_ACTIVE_SANDBOXES,
    SandboxHandle,
    SandboxManager,
)
from soveren_agent_platform.sessions.backend import TenantBoundaryError
from soveren_agent_platform.sessions.backends.codex_app_server import CodexCollaborationMode
from soveren_agent_platform.sessions.backends.codex_tools import DynamicToolRegistry
from soveren_agent_platform.sessions.backends.sandboxed_codex import (
    SandboxedCodexAppServerBackend,
    _validate_idle_stop_after_s,
)
from soveren_agent_platform.sessions.codex_credentials import (
    CodexCredentialProvider,
    CodexCredentialProvisioning,
)
from soveren_agent_platform.sessions.lifecycle import (
    CloseSessionResult,
    SessionLifecyclePolicy,
    SQLiteSessionLifecycle,
)
from soveren_agent_platform.sessions.registry import SessionBackendRegistry
from soveren_agent_platform.sessions.runtime import (
    SessionOpenRequest,
    SessionRuntime,
)
from soveren_agent_platform.sessions.sandboxing import (
    CODEX_BACKEND_PREFIX,
    _create_sandbox_manager,
    _create_sandboxed_codex_backend,
)
from soveren_agent_platform.sessions.sqlite import (
    SQLiteSessionMailboxStore,
    SQLiteSessionStore,
)

type ConversationToolRegistryFactory = Callable[
    [ConversationScope],
    DynamicToolRegistry | None,
]
type TenantCodexCredentialResolver = Callable[
    [str],
    Awaitable[CodexCredentialProvider],
]


@dataclass(frozen=True, slots=True)
class CodexSessionOpenRequest:
    """Open one durable Codex thread inside a private conversation runtime."""

    tenant_id: str
    source_id: str
    owner_id: str | None = None
    title: str = ""
    workspace_path: str = "/workspace"
    metadata: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CodexSessionOpenResult:
    """Platform identity of an opened durable Codex session."""

    session_id: str


@dataclass(frozen=True, slots=True)
class CodexSessionPrompt:
    """Durably enqueue one prompt for an existing Codex session."""

    session_id: str
    tenant_id: str
    source_id: str
    prompt: str
    action_id: str | None = None
    source_event_id: str | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class CodexSessionPromptReceipt:
    mailbox_id: str
    created: bool

_ACTIVE_RUNTIME_LOCK = threading.Lock()
_ACTIVE_RUNTIME: weakref.ReferenceType[_DefaultSandboxedCodexRuntime] | None = None


class SandboxedCodexRuntime(LlmBackend, Protocol):
    """Conversation-routing Codex backend with platform-owned lifecycle."""

    async def open_session(
        self,
        request: CodexSessionOpenRequest,
    ) -> CodexSessionOpenResult: ...

    async def enqueue_prompt(
        self,
        request: CodexSessionPrompt,
    ) -> CodexSessionPromptReceipt: ...

    async def close_session(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
        reason: str = "session closed by application",
        force: bool = False,
    ) -> CloseSessionResult: ...

    async def close_idle_sessions(
        self,
        *,
        tenant_id: str,
        policy: SessionLifecyclePolicy,
        source_id: str | None = None,
    ) -> list[CloseSessionResult]: ...

    async def provision_http_credential(
        self,
        *,
        tenant_id: str,
        source_id: str,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability: ...

    async def revoke_http_credential(
        self,
        *,
        tenant_id: str,
        source_id: str,
        name: str,
        scope: CredentialBindingScope = "conversation",
    ) -> None: ...

    async def shutdown(self) -> None: ...


class _ManagedSandboxedCodexRuntime(SandboxedCodexRuntime, Protocol):
    """Internal lifecycle bridge used by AgentPlatformApp."""

    def _mailbox_backends(self) -> SessionBackendRegistry: ...

    def _mailbox_backend_prefix(self) -> str: ...

    async def _restore_sessions(self, tenant_id: str) -> None: ...


def _create_sandboxed_codex_runtime(
    *,
    db_path: Path,
    credentials_for_tenant: TenantCodexCredentialResolver,
    model: str,
    resources: str = "small",
    max_active_sandboxes: int = DEFAULT_MAX_ACTIVE_SANDBOXES,
    egress_upstream: SandboxEgressUpstream | None = None,
    developer_instructions: str | None = None,
    tool_registry_factory: ConversationToolRegistryFactory | None = None,
    output_schema: JsonObject | None = None,
    collaboration_mode: CodexCollaborationMode | None = None,
    idle_stop_after_s: float | None = 300.0,
) -> _ManagedSandboxedCodexRuntime:
    """Create the supported Codex runtime for externally triggered work."""
    normalized_model = _non_empty_string(model, name="model")
    resolve_sandbox_resource_profile(resources)
    if developer_instructions is not None:
        _non_empty_string(developer_instructions, name="developer_instructions")
    if tool_registry_factory is not None and not callable(tool_registry_factory):
        raise TypeError("tool_registry_factory must be callable")
    if collaboration_mode is not None:
        if not isinstance(collaboration_mode, CodexCollaborationMode):
            raise TypeError("collaboration_mode must be a CodexCollaborationMode")
        if collaboration_mode.model != normalized_model:
            raise ValueError("collaboration_mode model must match the runtime model")
    normalized_idle_stop_after_s = _validate_idle_stop_after_s(
        idle_stop_after_s
    )
    normalized_schema = (
        None
        if output_schema is None
        else require_json_object(output_schema, label="Codex output schema")
    )
    if not callable(credentials_for_tenant):
        raise TypeError("credentials_for_tenant must be callable")
    runtime = _DefaultSandboxedCodexRuntime(
        db_path=db_path,
        credentials_for_tenant=credentials_for_tenant,
        model=normalized_model,
        resources=resources,
        developer_instructions=developer_instructions,
        tool_registry_factory=tool_registry_factory,
        output_schema=normalized_schema,
        collaboration_mode=collaboration_mode,
        idle_stop_after_s=normalized_idle_stop_after_s,
        max_active_sandboxes=max_active_sandboxes,
        egress_upstream=egress_upstream,
    )
    _claim_process_runtime(runtime)
    return runtime


class _DefaultSandboxedCodexRuntime:
    name = "sandboxed_codex"
    version = "1"

    def __init__(
        self,
        *,
        db_path: Path,
        credentials_for_tenant: TenantCodexCredentialResolver,
        model: str,
        resources: str,
        developer_instructions: str | None,
        tool_registry_factory: ConversationToolRegistryFactory | None,
        output_schema: JsonObject | None,
        collaboration_mode: CodexCollaborationMode | None,
        idle_stop_after_s: float | None,
        max_active_sandboxes: int,
        egress_upstream: SandboxEgressUpstream | None,
    ) -> None:
        self._db_path = db_path
        self._credentials_for_tenant = credentials_for_tenant
        self._model = model
        self._resources = resources
        self._developer_instructions = developer_instructions
        self._tool_registry_factory = tool_registry_factory
        self._output_schema = output_schema
        self._collaboration_mode = collaboration_mode
        self._idle_stop_after_s = idle_stop_after_s
        self._sandbox_manager = _create_sandbox_manager(
            max_active_sandboxes=max_active_sandboxes,
            egress_upstream=egress_upstream,
        )
        self._session_backends = SessionBackendRegistry()
        self._conversation_backends: dict[
            ConversationScope,
            SandboxedCodexAppServerBackend,
        ] = {}
        self._llm_backends: dict[ConversationScope, SessionLlmBackend] = {}
        self._shutdown_backends: set[int] = set()
        self._shutdown_lock = asyncio.Lock()
        self._session_services_lock = asyncio.Lock()
        self._session_store: SQLiteSessionStore | None = None
        self._mailbox_store: SQLiteSessionMailboxStore | None = None
        self._session_lifecycle: SQLiteSessionLifecycle | None = None
        self._session_runtime: SessionRuntime | None = None
        self._closing = False
        self._closed = False

    async def run(self, request: LlmRequest) -> LlmResponse:
        self._ensure_running()
        if request.conversation_scope is None:
            raise TenantBoundaryError(
                "sandboxed Codex runtime requires a trusted conversation scope"
            )
        if request.model != self._model:
            raise ValueError(
                f"planner model {request.model!r} does not match configured "
                f"Codex model {self._model!r}"
            )
        backend = self._llm_backend_for(request.conversation_scope)
        return await backend.run(request)

    async def open_session(
        self,
        request: CodexSessionOpenRequest,
    ) -> CodexSessionOpenResult:
        self._ensure_running()
        workspace_path = _sandbox_workspace_path(request.workspace_path)
        scope = ConversationScope(
            tenant_id=request.tenant_id,
            source_id=request.source_id,
        )
        backend = self._session_backend_for(scope)
        session_runtime, _, _ = await self._ensure_session_services()
        metadata = require_json_object(
            {
                **request.metadata,
                "sandbox_cwd": workspace_path,
            },
            label="Codex session metadata",
        )
        opened = await session_runtime.open_session(
            SessionOpenRequest(
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                owner_id=request.owner_id,
                kind="codex_cli",
                backend=backend.name,
                cwd=workspace_path,
                title=request.title,
                metadata=metadata,
            )
        )
        return CodexSessionOpenResult(session_id=opened.session_id)

    async def enqueue_prompt(
        self,
        request: CodexSessionPrompt,
    ) -> CodexSessionPromptReceipt:
        self._ensure_running()
        scope = ConversationScope(
            tenant_id=request.tenant_id,
            source_id=request.source_id,
        )
        session_runtime, mailbox_store, _ = await self._ensure_session_services()
        await self._ensure_owned_session(
            session_runtime,
            session_id=request.session_id,
            scope=scope,
        )
        mailbox_id, created = await mailbox_store.enqueue_prompt(
            session_id=request.session_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            prompt=request.prompt,
            action_id=request.action_id,
            source_event_id=request.source_event_id,
            idempotency_key=request.idempotency_key,
        )
        return CodexSessionPromptReceipt(
            mailbox_id=mailbox_id,
            created=created,
        )

    async def close_session(
        self,
        session_id: str,
        *,
        tenant_id: str,
        source_id: str,
        reason: str = "session closed by application",
        force: bool = False,
    ) -> CloseSessionResult:
        self._ensure_running()
        scope = ConversationScope(tenant_id=tenant_id, source_id=source_id)
        session_runtime, _, lifecycle = await self._ensure_session_services()
        await self._ensure_owned_session(
            session_runtime,
            session_id=session_id,
            scope=scope,
        )
        return await lifecycle.close_session(
            session_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            reason=reason,
            force=force,
        )

    async def close_idle_sessions(
        self,
        *,
        tenant_id: str,
        policy: SessionLifecyclePolicy,
        source_id: str | None = None,
    ) -> list[CloseSessionResult]:
        self._ensure_running()
        if not tenant_id.strip():
            raise ValueError("tenant_id must be non-empty")
        if source_id is not None:
            ConversationScope(tenant_id=tenant_id, source_id=source_id)
        await self._restore_sessions(tenant_id)
        _, _, lifecycle = await self._ensure_session_services()
        return await lifecycle.close_idle_sessions(
            tenant_id=tenant_id,
            source_id=source_id,
            policy=policy,
            backend_prefix=CODEX_BACKEND_PREFIX,
        )

    async def provision_http_credential(
        self,
        *,
        tenant_id: str,
        source_id: str,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        self._ensure_running()
        backend = self._session_backend_for(
            ConversationScope(tenant_id=tenant_id, source_id=source_id),
        )
        return await backend.provision_http_credential(credential, binding)

    async def revoke_http_credential(
        self,
        *,
        tenant_id: str,
        source_id: str,
        name: str,
        scope: CredentialBindingScope = "conversation",
    ) -> None:
        self._ensure_running()
        backend = self._session_backend_for(
            ConversationScope(tenant_id=tenant_id, source_id=source_id),
        )
        await backend.revoke_http_credential(name, scope=scope)

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if self._closed:
                return
            self._closing = True
            errors: list[BaseException] = []
            for backend in reversed(tuple(self._conversation_backends.values())):
                identity = id(backend)
                if identity in self._shutdown_backends:
                    continue
                try:
                    await backend.shutdown()
                except BaseException as exc:
                    errors.append(exc)
                else:
                    self._shutdown_backends.add(identity)
            if self._session_store is not None:
                try:
                    await self._session_store.close()
                except BaseException as exc:
                    errors.append(exc)
            if errors:
                raise BaseExceptionGroup(
                    "sandboxed Codex runtime shutdown failed",
                    errors,
                )
            self._closed = True
            _release_process_runtime(self)

    def _mailbox_backends(self) -> SessionBackendRegistry:
        return self._session_backends

    def _mailbox_backend_prefix(self) -> str:
        return CODEX_BACKEND_PREFIX

    async def _restore_sessions(self, tenant_id: str) -> None:
        self._ensure_running()
        if not tenant_id.strip():
            raise ValueError("tenant_id must be non-empty")
        session_runtime, _, _ = await self._ensure_session_services()
        after_session_id: str | None = None
        while True:
            sessions = await session_runtime.store.list_active(
                tenant_id=tenant_id,
                limit=100,
                after_session_id=after_session_id,
            )
            for session in sessions:
                if not session.backend.startswith(CODEX_BACKEND_PREFIX):
                    continue
                backend = self._session_backend_for(
                    ConversationScope(
                        tenant_id=session.tenant_id,
                        source_id=session.source_id,
                    )
                )
                if backend.name != session.backend:
                    raise RuntimeError(
                        "persisted Codex session backend does not match its "
                        "conversation boundary"
                    )
            if len(sessions) < 100:
                return
            after_session_id = sessions[-1].id

    def _llm_backend_for(self, scope: ConversationScope) -> SessionLlmBackend:
        existing = self._llm_backends.get(scope)
        if existing is not None:
            return existing
        session_backend = self._session_backend_for(scope)
        llm_backend = SessionLlmBackend(
            backend=session_backend,
            kind="codex_cli",
            name=self.name,
            version=self.version,
            output_schema=self._output_schema,
        )
        self._llm_backends[scope] = llm_backend
        return llm_backend

    def _session_backend_for(
        self,
        scope: ConversationScope,
    ) -> SandboxedCodexAppServerBackend:
        existing = self._conversation_backends.get(scope)
        if existing is not None:
            return existing
        tools = (
            None
            if self._tool_registry_factory is None
            else self._tool_registry_factory(scope)
        )
        if tools is not None and not isinstance(tools, DynamicToolRegistry):
            raise TypeError(
                "tool_registry_factory must return DynamicToolRegistry or None"
            )
        session_backend = _create_sandboxed_codex_backend(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            credentials=_TenantResolvedCodexCredentials(
                tenant_id=scope.tenant_id,
                resolver=self._credentials_for_tenant,
            ),
            sandbox_manager=self._sandbox_manager,
            session_backends=self._session_backends,
            resources=self._resources,
            model=self._model,
            developer_instructions=self._developer_instructions,
            dynamic_tools=tools,
            collaboration_mode=self._collaboration_mode,
            idle_stop_after_s=self._idle_stop_after_s,
        )
        self._conversation_backends[scope] = session_backend
        return session_backend

    async def _ensure_session_services(
        self,
    ) -> tuple[SessionRuntime, SQLiteSessionMailboxStore, SQLiteSessionLifecycle]:
        if (
            self._session_runtime is not None
            and self._mailbox_store is not None
            and self._session_lifecycle is not None
        ):
            return (
                self._session_runtime,
                self._mailbox_store,
                self._session_lifecycle,
            )
        async with self._session_services_lock:
            if self._session_store is None:
                self._session_store = await SQLiteSessionStore.open(self._db_path)
            if self._mailbox_store is None:
                self._mailbox_store = SQLiteSessionMailboxStore._from_connection(
                    self._session_store._conn,
                )
            if self._session_lifecycle is None:
                self._session_lifecycle = SQLiteSessionLifecycle._from_connection(
                    self._session_store._conn,
                    session_backends=self._session_backends,
                )
            if self._session_runtime is None:
                self._session_runtime = SessionRuntime(
                    self._session_store,
                    self._session_backends,
                )
            return (
                self._session_runtime,
                self._mailbox_store,
                self._session_lifecycle,
            )

    async def _ensure_owned_session(
        self,
        session_runtime: SessionRuntime,
        *,
        session_id: str,
        scope: ConversationScope,
    ) -> None:
        session = await session_runtime.store.get(
            session_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
        )
        if session is None:
            return
        expected_backend = self._session_backend_for(scope)
        if session.backend != expected_backend.name:
            raise RuntimeError(
                "runtime session is not owned by the sandboxed Codex runtime"
            )

    def _ensure_running(self) -> None:
        if self._closing or self._closed:
            raise RuntimeError("sandboxed Codex runtime is shutting down")


def _non_empty_string(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _sandbox_workspace_path(value: object) -> str:
    path = _non_empty_string(value, name="workspace_path")
    if not path.startswith("/"):
        raise ValueError("workspace_path must be an absolute sandbox path")
    normalized = posixpath.normpath(path)
    if normalized != "/workspace" and not normalized.startswith("/workspace/"):
        raise ValueError("workspace_path must stay inside /workspace")
    return normalized


@dataclass(frozen=True, slots=True)
class _TenantResolvedCodexCredentials:
    tenant_id: str
    resolver: TenantCodexCredentialResolver

    async def provision(
        self,
        manager: SandboxManager,
        handle: SandboxHandle,
    ) -> CodexCredentialProvisioning:
        if handle.tenant_id != self.tenant_id:
            raise TenantBoundaryError(
                "Codex credential resolver cannot provision another tenant"
            )
        provider = await self.resolver(self.tenant_id)
        if not isinstance(provider, CodexCredentialProvider):
            raise TypeError(
                "credentials_for_tenant must resolve to CodexCredentialProvider"
            )
        return await provider.provision(manager, handle)


def _claim_process_runtime(runtime: _DefaultSandboxedCodexRuntime) -> None:
    global _ACTIVE_RUNTIME
    with _ACTIVE_RUNTIME_LOCK:
        active = None if _ACTIVE_RUNTIME is None else _ACTIVE_RUNTIME()
        if active is not None and not active._closed:
            raise RuntimeError(
                "one sandboxed Codex runtime is already configured in this process"
            )
        _ACTIVE_RUNTIME = weakref.ref(runtime)


def _release_process_runtime(runtime: _DefaultSandboxedCodexRuntime) -> None:
    global _ACTIVE_RUNTIME
    with _ACTIVE_RUNTIME_LOCK:
        active = None if _ACTIVE_RUNTIME is None else _ACTIVE_RUNTIME()
        if active is runtime:
            _ACTIVE_RUNTIME = None
