"""Backend-neutral LLM transport contracts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import DynamicToolAllowlist, ToolExecutionContext
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.llm.checkpoint import LlmExecutionJournal


@dataclass(slots=True)
class LlmRequest:
    prompt: str
    system_prompt: str
    cwd: Path
    env_home: Path
    model: str
    session_id: str | None = None
    resume: bool = False
    timeout_s: int = 120
    metadata: dict[str, Any] | None = None
    conversation_scope: ConversationScope | None = None
    output_schema: JsonObject | None = None
    execution_context: ToolExecutionContext | None = None
    dynamic_tool_allowlist: DynamicToolAllowlist | None = None
    journal: LlmExecutionJournal | None = None


@dataclass(slots=True)
class LlmResponse:
    text: str
    session_id: str
    cost_usd: float | None = None
    duration_ms: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    metadata: dict[str, Any] | None = None


class LlmBackend(Protocol):
    name: str
    version: str

    async def run(self, request: LlmRequest) -> LlmResponse:
        ...
