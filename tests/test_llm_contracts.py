from pathlib import Path

import asyncio

from agent_platform.llm.backends import OpenAICompatibleBackend, SessionLlmBackend
from agent_platform.llm.contracts import LlmRequest, LlmResponse
from agent_platform.sessions.backend import CaptureResult, OpenResult


def test_llm_contracts_are_backend_neutral():
    request = LlmRequest(
        prompt="hello",
        system_prompt="system",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
        model="test-model",
        metadata={"app": "test"},
    )
    response = LlmResponse(text="{}", session_id="session-1")

    assert request.resume is False
    assert request.metadata == {"app": "test"}
    assert response.cost_usd == 0.0


def test_openai_compatible_backend_uses_chat_completions_contract():
    captured = {}

    def transport(url, headers, payload, timeout):
        captured.update({"url": url, "headers": headers, "payload": payload, "timeout": timeout})
        return {
            "id": "cmpl_1",
            "choices": [{"message": {"content": "{\"kind\":\"reply\"}"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5},
        }

    backend = OpenAICompatibleBackend(
        base_url="https://llm.example/v1",
        api_key="token",
        transport=transport,
    )

    response = asyncio.run(backend.run(_request()))

    assert captured["url"] == "https://llm.example/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer token"
    assert captured["payload"]["messages"][0]["role"] == "system"
    assert response.text == "{\"kind\":\"reply\"}"
    assert response.input_tokens == 10
    assert response.output_tokens == 5


class FakeSessionBackend:
    name = "fake_session"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.closed: list[str] = []

    async def open(self, spec):
        return OpenResult(backend_session_id="backend-1", metadata={"kind": spec.kind})

    async def send(self, backend_session_id: str, prompt: str) -> None:
        self.sent.append((backend_session_id, prompt))

    async def capture(self, backend_session_id: str):
        return CaptureResult(text="{\"kind\":\"reply\"}", timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        self.closed.append(backend_session_id)


def test_session_llm_backend_opens_sends_captures_and_closes():
    session_backend = FakeSessionBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="claude_cli")

    response = asyncio.run(backend.run(_request()))

    assert response.text == "{\"kind\":\"reply\"}"
    assert session_backend.sent[0][0] == "backend-1"
    assert "--- USER REQUEST ---" in session_backend.sent[0][1]
    assert session_backend.closed == ["backend-1"]


def _request() -> LlmRequest:
    return LlmRequest(
        prompt="hello",
        system_prompt="system",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
        model="test-model",
    )
