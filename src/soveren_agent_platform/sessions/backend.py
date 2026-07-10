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
