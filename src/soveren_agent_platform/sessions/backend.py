"""Backend-neutral execution session protocol."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from soveren_agent_platform.conversation import ConversationScope


@dataclass(slots=True)
class OpenSpec:
    kind: str
    cwd: str
    title: str = ""
    metadata: dict[str, Any] | None = None
    conversation_scope: ConversationScope | None = None


@dataclass(slots=True)
class OpenResult:
    backend_session_id: str
    session_handle: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class CaptureResult:
    text: str
    timed_out: bool


@dataclass(frozen=True, slots=True)
class SendReceipt:
    backend_operation_id: str | None = None
    metadata: dict[str, Any] | None = None


class TenantBoundaryError(PermissionError):
    """Raised when a tenant-bound runtime resource is used for another tenant."""


@runtime_checkable
class TenantBoundResource(Protocol):
    @property
    def tenant_id(self) -> str:
        ...


@runtime_checkable
class ConversationBoundResource(Protocol):
    @property
    def tenant_id(self) -> str:
        ...

    @property
    def source_id(self) -> str:
        ...


@runtime_checkable
class ConversationScopeProvider(Protocol):
    @property
    def conversation_scope(self) -> ConversationScope | None:
        ...


def bound_conversation_scope(resource: object) -> ConversationScope | None:
    if isinstance(resource, ConversationBoundResource):
        return ConversationScope(tenant_id=resource.tenant_id, source_id=resource.source_id)
    if isinstance(resource, ConversationScopeProvider):
        return resource.conversation_scope
    return None


def ensure_tenant_boundary(resource: object, tenant_id: str, *, resource_name: str) -> None:
    bound_tenant_id: str | None = None
    if isinstance(resource, TenantBoundResource):
        bound_tenant_id = resource.tenant_id
    else:
        scope = bound_conversation_scope(resource)
        if scope is not None:
            bound_tenant_id = scope.tenant_id
    if bound_tenant_id is not None and bound_tenant_id != tenant_id:
        raise TenantBoundaryError(
            f"{resource_name} is bound to tenant {bound_tenant_id!r}, not {tenant_id!r}"
        )


def ensure_conversation_boundary(
    resource: object,
    tenant_id: str,
    source_id: str,
    *,
    resource_name: str,
) -> None:
    ensure_tenant_boundary(resource, tenant_id, resource_name=resource_name)
    scope = bound_conversation_scope(resource)
    if scope is not None and scope.source_id != source_id:
        raise TenantBoundaryError(
            f"{resource_name} is bound to conversation {scope.source_id!r}, not {source_id!r}"
        )


def ensure_conversation_scope(
    resource: object,
    scope: ConversationScope | None,
    *,
    resource_name: str,
) -> None:
    bound_scope = bound_conversation_scope(resource)
    if scope is None:
        if bound_scope is not None:
            raise TenantBoundaryError(f"{resource_name} requires a trusted conversation scope")
        return
    ensure_conversation_boundary(
        resource,
        scope.tenant_id,
        scope.source_id,
        resource_name=resource_name,
    )


class SessionBackend(Protocol):
    name: str

    async def open(self, spec: OpenSpec) -> OpenResult:
        ...

    async def send(self, backend_session_id: str, prompt: str) -> SendReceipt | None:
        ...

    async def capture(self, backend_session_id: str) -> CaptureResult:
        ...

    async def close(self, backend_session_id: str) -> None:
        ...


@runtime_checkable
class DeliveryCaptureBackend(Protocol):
    """Optional backend capability for recovering one accepted delivery."""

    async def capture_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> CaptureResult:
        ...


@runtime_checkable
class DeliveryAbortBackend(Protocol):
    """Optional backend capability for stopping one accepted delivery."""

    async def abort_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> None:
        ...
