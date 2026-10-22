"""LLM backend implemented over reusable execution session backends."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import validate_dynamic_tool_allowlist
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.runtime.latency import measure_phase
from soveren_agent_platform.sessions.backend import (
    ContextualSessionBackend,
    OpenSpec,
    OutputSchemaSessionBackend,
    SessionBackend,
    TenantBoundaryError,
    UnsupportedContextualSessionError,
    bound_conversation_scope,
    ensure_conversation_scope,
)


@dataclass(slots=True)
class SessionLlmBackend:
    backend: SessionBackend
    kind: str
    name: str = "session_llm"
    version: str = "1"
    title: str = "planner"
    metadata: dict[str, Any] = field(default_factory=dict)
    output_schema: JsonObject | None = None

    @property
    def conversation_scope(self) -> ConversationScope | None:
        return bound_conversation_scope(self.backend)

    async def run(self, request: LlmRequest) -> LlmResponse:
        ensure_conversation_scope(
            self.backend,
            request.conversation_scope,
            resource_name=f"session LLM backend {self.name!r}",
        )
        allowlist = validate_dynamic_tool_allowlist(request.dynamic_tool_allowlist)
        if (
            request.execution_context is not None
            and request.conversation_scope != request.execution_context.scope
        ):
            raise TenantBoundaryError(
                "LLM execution context scope does not match the trusted conversation scope"
            )
        started = time.monotonic()
        opened = None
        try:
            async with asyncio.timeout(request.timeout_s):
                with measure_phase("session_open"):
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
                            conversation_scope=request.conversation_scope,
                            dynamic_tool_allowlist=allowlist,
                        )
                    )
                prompt = _framed_prompt(request)
                output_schema = (
                    request.output_schema
                    if request.output_schema is not None
                    else self.output_schema
                )
                with measure_phase("codex_turn"):
                    if request.execution_context is not None or allowlist is not None:
                        if not isinstance(self.backend, ContextualSessionBackend):
                            raise UnsupportedContextualSessionError(
                                "session backend does not support trusted execution context"
                            )
                        await self.backend.send_with_context(
                            opened.backend_session_id,
                            prompt,
                            execution_context=request.execution_context,
                            output_schema=output_schema,
                            dynamic_tool_allowlist=allowlist,
                        )
                    elif output_schema is None:
                        await self.backend.send(opened.backend_session_id, prompt)
                    else:
                        if not isinstance(self.backend, OutputSchemaSessionBackend):
                            raise TypeError(
                                "session LLM backend does not support output schemas"
                            )
                        await self.backend.send_with_output_schema(
                            opened.backend_session_id,
                            prompt,
                            output_schema,
                        )
                    capture = await self.backend.capture(opened.backend_session_id)
            if capture.timed_out:
                raise TimeoutError(f"session backend timed out for {opened.backend_session_id}")
            response = LlmResponse(
                text=capture.text,
                session_id=opened.backend_session_id,
                duration_ms=round((time.monotonic() - started) * 1000),
                metadata={
                    "timed_out": capture.timed_out,
                    "backend_metadata": opened.metadata or {},
                },
            )
        except BaseException as run_error:
            if opened is None:
                raise
            try:
                await self.backend.close(opened.backend_session_id)
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "session LLM request and cleanup failed",
                    [run_error, cleanup_error],
                ) from None
            raise
        else:
            await self.backend.close(opened.backend_session_id)
            return response


def _framed_prompt(request: LlmRequest) -> str:
    return (
        f"{request.system_prompt.rstrip()}\n\n"
        "--- USER REQUEST ---\n"
        f"{request.prompt.rstrip()}\n"
        "--- END USER REQUEST ---\n"
    )
