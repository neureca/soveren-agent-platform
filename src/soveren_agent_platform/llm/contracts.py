"""Backend-neutral LLM transport contracts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


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


@dataclass(slots=True)
class LlmResponse:
    text: str
    session_id: str
    cost_usd: float = 0.0
    duration_ms: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    metadata: dict[str, Any] | None = None


class LlmBackend(Protocol):
    name: str
    version: str

    async def run(self, request: LlmRequest) -> LlmResponse:
        ...

