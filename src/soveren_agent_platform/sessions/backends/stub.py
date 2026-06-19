"""Deterministic in-process session backend for tests and local wiring."""
from __future__ import annotations

import uuid

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec


class StubBackend:
    name = "stub"

    def __init__(self) -> None:
        self._buffers: dict[str, str] = {}

    async def open(self, spec: OpenSpec) -> OpenResult:
        backend_session_id = f"stub-{spec.kind.replace('_cli', '')}-{uuid.uuid4().hex[:8]}"
        self._buffers[backend_session_id] = (
            f"[stub-{spec.kind}] session ready in cwd={spec.cwd}\n"
        )
        return OpenResult(
            backend_session_id=backend_session_id,
            session_handle=backend_session_id,
            metadata={"kind": spec.kind, "cwd": spec.cwd},
        )

    async def send(self, backend_session_id: str, prompt: str) -> None:
        current = self._buffers.setdefault(backend_session_id, "")
        self._buffers[backend_session_id] = current + (
            f"\n> {prompt}\n[stub] ack: received prompt length={len(prompt)}\n"
        )

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return CaptureResult(
            text=self._buffers.get(backend_session_id, ""),
            timed_out=False,
        )

    async def close(self, backend_session_id: str) -> None:
        self._buffers.pop(backend_session_id, None)

