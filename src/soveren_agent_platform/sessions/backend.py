"""Backend-neutral execution session protocol."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(slots=True)
class OpenSpec:
    kind: str
    cwd: str
    title: str = ""
    metadata: dict[str, Any] | None = None


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


def ensure_tenant_boundary(resource: object, tenant_id: str, *, resource_name: str) -> None:
    if isinstance(resource, TenantBoundResource) and resource.tenant_id != tenant_id:
        raise TenantBoundaryError(
            f"{resource_name} is bound to tenant {resource.tenant_id!r}, not {tenant_id!r}"
        )


def ensure_conversation_boundary(
    resource: object,
    tenant_id: str,
    source_id: str,
    *,
    resource_name: str,
) -> None:
    ensure_tenant_boundary(resource, tenant_id, resource_name=resource_name)
    if isinstance(resource, ConversationBoundResource) and resource.source_id != source_id:
        raise TenantBoundaryError(
            f"{resource_name} is bound to conversation {resource.source_id!r}, not {source_id!r}"
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
