"""LLM backend implemented over reusable execution session backends."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.sessions.backend import OpenSpec, SessionBackend
from soveren_agent_platform.sessions.backends.codex_app_server import CodexAppServerBackend
from soveren_agent_platform.sessions.backends.tmux import TmuxBackend


@dataclass(slots=True)
class SessionLlmBackend:
    backend: SessionBackend
    kind: str
    name: str = "session_llm"
    version: str = "1"
    title: str = "planner"
    metadata: dict[str, Any] = field(default_factory=dict)

    async def run(self, request: LlmRequest) -> LlmResponse:
        opened = None
        try:
            async with asyncio.timeout(request.timeout_s):
                opened = await self.backend.open(
                    OpenSpec(
                        kind=self.kind,
                        cwd=str(request.cwd),
                        title=self.title,
                        metadata={
                            **self.metadata,
                            "model": request.model,
                            "env_home": str(request.env_home),
                        },
                    )
                )
                prompt = _framed_prompt(request)
                await self.backend.send(opened.backend_session_id, prompt)
                capture = await self.backend.capture(opened.backend_session_id)
            if capture.timed_out:
                raise TimeoutError(f"session backend timed out for {opened.backend_session_id}")
            return LlmResponse(
                text=capture.text,
                session_id=opened.backend_session_id,
                metadata={
                    "timed_out": capture.timed_out,
                    "backend_metadata": opened.metadata or {},
                },
            )
        finally:
            if opened is not None:
                await self.backend.close(opened.backend_session_id)


class ClaudeTmuxLlmBackend(SessionLlmBackend):
    def __init__(
        self,
        *,
        socket: str,
        home: Path,
        command: list[str] | None = None,
        kind: str = "claude_cli",
        session_prefix: str = "soveren-agent-platform-llm",
        **kwargs: Any,
    ) -> None:
        backend = TmuxBackend(
            socket=socket,
            home=home,
            command_for_kind={kind: command or ["claude"]},
            session_prefix=session_prefix,
        )
        super().__init__(backend=backend, kind=kind, name="claude_tmux", version="1", **kwargs)


class CodexAppServerLlmBackend(SessionLlmBackend):
    def __init__(
        self,
        *,
        codex_home: Path | None = None,
        model: str | None = None,
        kind: str = "codex_cli",
        **kwargs: Any,
    ) -> None:
        backend = CodexAppServerBackend(
            codex_home=codex_home,
            model=model,
            approval_policy="never",
            dynamic_tools=None,
        )
        super().__init__(backend=backend, kind=kind, name="codex_app_server", version="1", **kwargs)


def _framed_prompt(request: LlmRequest) -> str:
    return (
        f"{request.system_prompt.rstrip()}\n\n"
        "--- USER REQUEST ---\n"
        f"{request.prompt.rstrip()}\n"
        "--- END USER REQUEST ---\n"
    )
