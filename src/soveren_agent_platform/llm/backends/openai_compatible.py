"""OpenAI-compatible chat completions LLM backend."""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse

JsonTransport = Callable[[str, dict[str, str], dict[str, Any], float], dict[str, Any]]


@dataclass(slots=True)
class OpenAICompatibleBackend:
    base_url: str
    api_key: str | None = field(default=None, repr=False)
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
            _chat_completions_url(self.base_url),
            headers,
            payload,
            timeout,
        )
        choice = (result.get("choices") or [{}])[0] or {}
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
        # The caller validates the URL scheme before this transport is invoked.
        with urllib.request.urlopen(req, timeout=timeout) as response:  # nosec B310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"OpenAI-compatible backend HTTP {exc.code}: {body[:1000]}") from exc


def _chat_completions_url(base_url: str) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError("OpenAI-compatible base_url must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OpenAI-compatible base_url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("OpenAI-compatible base_url must not contain a query or fragment")
    path = parsed.path.rstrip("/") + "/chat/completions"
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
