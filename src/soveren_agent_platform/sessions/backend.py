"""Backend-neutral execution session protocol."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


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


class SessionBackend(Protocol):
    name: str

    async def open(self, spec: OpenSpec) -> OpenResult:
        ...

    async def send(self, backend_session_id: str, prompt: str) -> None:
        ...

    async def capture(self, backend_session_id: str) -> CaptureResult:
        ...

    async def close(self, backend_session_id: str) -> None:
        ...

