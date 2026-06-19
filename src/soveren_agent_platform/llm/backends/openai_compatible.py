"""OpenAI-compatible chat completions LLM backend."""
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse

JsonTransport = Callable[[str, dict[str, str], dict[str, Any], float], dict[str, Any]]


@dataclass(slots=True)
class OpenAICompatibleBackend:
    base_url: str
    api_key: str | None = None
    name: str = "openai_compatible"
    version: str = "chat-completions"
    default_timeout_s: float = 120.0
    transport: JsonTransport | None = None

    async def run(self, request: LlmRequest) -> LlmResponse:
        payload = {
            "model": request.model,
            "messages": [
                {"role": "system", "content": request.system_prompt},
                {"role": "user", "content": request.prompt},
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        timeout = float(request.timeout_s or self.default_timeout_s)
        transport = self.transport or _post_json
        result = await asyncio.to_thread(
            transport,
            self.base_url.rstrip("/") + "/chat/completions",
            headers,
            payload,
            timeout,
        )
        choice = ((result.get("choices") or [{}])[0] or {})
        message = choice.get("message") or {}
        usage = result.get("usage") or {}
        return LlmResponse(
            text=str(message.get("content") or ""),
            session_id=str(result.get("id") or "chatcmpl_" + uuid.uuid4().hex),
            input_tokens=int(usage.get("prompt_tokens") or 0),
            output_tokens=int(usage.get("completion_tokens") or 0),
            metadata={"raw": result},
        )


def _post_json(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"OpenAI-compatible backend HTTP {exc.code}: {body[:1000]}") from exc
