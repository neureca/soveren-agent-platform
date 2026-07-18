import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from soveren_agent_platform.llm import ConversationScope
from soveren_agent_platform.llm.backends import OpenAICompatibleBackend, SessionLlmBackend
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, TenantBoundaryError


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
    assert request.conversation_scope is None
    assert request.metadata == {"app": "test"}
    assert response.cost_usd == 0.0


def test_llm_request_preserves_v022_positional_arguments():
    request = LlmRequest(
        "hello",
        "system",
        Path("/tmp/work"),
        Path("/tmp/home"),
        "test-model",
        "session-1",
        True,
        45,
        {"app": "test"},
    )

    assert request.session_id == "session-1"
    assert request.resume is True
    assert request.timeout_s == 45
    assert request.metadata == {"app": "test"}
    assert request.conversation_scope is None


def test_openai_compatible_backend_uses_chat_completions_contract():
    captured = {}

    def transport(url, headers, payload, timeout):
        captured.update({"url": url, "headers": headers, "payload": payload, "timeout": timeout})
        return {
            "id": "cmpl_1",
            "choices": [{"message": {"content": '{"kind":"reply"}'}}],
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
    assert set(captured["payload"]) == {"model", "messages"}
    assert response.text == '{"kind":"reply"}'
    assert response.input_tokens == 10
    assert response.output_tokens == 5
    assert "token" not in repr(backend)


@pytest.mark.parametrize(
    "base_url",
    [
        "file:///tmp/llm",
        "llm.example/v1",
        "https://user:password@llm.example/v1",
        "https://llm.example/v1?tenant=one",
        "https://llm.example/v1#fragment",
    ],
)
def test_openai_compatible_backend_rejects_unsafe_base_urls(base_url: str):
    backend = OpenAICompatibleBackend(base_url=base_url, transport=lambda *_args: {})

    with pytest.raises(ValueError, match="base_url"):
        asyncio.run(backend.run(_request()))


class FakeSessionBackend:
    name = "fake_session"

    def __init__(self) -> None:
        self.opened = 0
        self.sent: list[tuple[str, str]] = []
        self.closed: list[str] = []

    async def open(self, spec):
        self.opened += 1
        return OpenResult(backend_session_id="backend-1", metadata={"kind": spec.kind})

    async def send(self, backend_session_id: str, prompt: str) -> None:
        self.sent.append((backend_session_id, prompt))

    async def capture(self, backend_session_id: str):
        return CaptureResult(text='{"kind":"reply"}', timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        self.closed.append(backend_session_id)


def test_session_llm_backend_opens_sends_captures_and_closes():
    session_backend = FakeSessionBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="claude_cli")

    response = asyncio.run(backend.run(_request()))

    assert response.text == '{"kind":"reply"}'
    assert session_backend.sent[0][0] == "backend-1"
    assert "--- USER REQUEST ---" in session_backend.sent[0][1]
    assert session_backend.closed == ["backend-1"]


class BoundFakeSessionBackend(FakeSessionBackend):
    tenant_id = "tenant-a"
    source_id = "chat-a"


def test_session_llm_backend_requires_scope_for_bound_backend_before_io():
    session_backend = BoundFakeSessionBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="codex_cli")

    with pytest.raises(TenantBoundaryError, match="requires a trusted conversation scope"):
        asyncio.run(backend.run(_request()))

    assert session_backend.opened == 0


def test_session_llm_backend_rejects_mismatched_scope_before_io():
    session_backend = BoundFakeSessionBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="codex_cli")
    request = replace(
        _request(),
        conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-b"),
    )

    with pytest.raises(TenantBoundaryError, match="chat-a.*chat-b"):
        asyncio.run(backend.run(request))

    assert session_backend.opened == 0


def test_session_llm_backend_accepts_matching_trusted_scope():
    session_backend = BoundFakeSessionBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="codex_cli")
    request = replace(
        _request(),
        conversation_scope=ConversationScope(tenant_id="tenant-a", source_id="chat-a"),
    )

    response = asyncio.run(backend.run(request))

    assert response.text == '{"kind":"reply"}'
    assert backend.conversation_scope == request.conversation_scope
    assert session_backend.opened == 1


def test_session_llm_backend_rejects_partial_timeout_and_closes():
    class TimedOutBackend(FakeSessionBackend):
        async def capture(self, backend_session_id: str):
            return CaptureResult(text="partial", timed_out=True)

    session_backend = TimedOutBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="claude_cli")

    with pytest.raises(TimeoutError, match="session backend timed out"):
        asyncio.run(backend.run(_request()))

    assert session_backend.closed == ["backend-1"]


def test_session_llm_backend_enforces_request_deadline_and_closes():
    class SlowBackend(FakeSessionBackend):
        async def capture(self, backend_session_id: str):
            await asyncio.sleep(1)
            return CaptureResult(text="late", timed_out=False)

    session_backend = SlowBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="claude_cli")

    with pytest.raises(TimeoutError):
        asyncio.run(backend.run(replace(_request(), timeout_s=0.01)))

    assert session_backend.closed == ["backend-1"]


def test_session_llm_backend_preserves_request_and_cleanup_failures():
    class FailingBackend(FakeSessionBackend):
        async def capture(self, backend_session_id: str):
            raise ValueError("capture failed")

        async def close(self, backend_session_id: str) -> None:
            raise RuntimeError("close failed")

    backend = SessionLlmBackend(backend=FailingBackend(), kind="claude_cli")

    with pytest.raises(BaseExceptionGroup, match="session LLM request and cleanup failed") as raised:
        asyncio.run(backend.run(_request()))

    assert [type(exc) for exc in raised.value.exceptions] == [ValueError, RuntimeError]
    assert [str(exc) for exc in raised.value.exceptions] == ["capture failed", "close failed"]


def test_session_llm_backend_preserves_timeout_and_cleanup_failures():
    class TimedOutBackend(FakeSessionBackend):
        async def capture(self, backend_session_id: str):
            return CaptureResult(text="partial", timed_out=True)

        async def close(self, backend_session_id: str) -> None:
            raise RuntimeError("close failed")

    backend = SessionLlmBackend(backend=TimedOutBackend(), kind="claude_cli")

    with pytest.raises(BaseExceptionGroup, match="session LLM request and cleanup failed") as raised:
        asyncio.run(backend.run(_request()))

    assert [type(exc) for exc in raised.value.exceptions] == [TimeoutError, RuntimeError]
    assert "session backend timed out" in str(raised.value.exceptions[0])
    assert str(raised.value.exceptions[1]) == "close failed"


def test_session_llm_backend_propagates_cleanup_failure_after_success():
    class CloseFailingBackend(FakeSessionBackend):
        async def close(self, backend_session_id: str) -> None:
            raise RuntimeError("close failed")

    backend = SessionLlmBackend(backend=CloseFailingBackend(), kind="claude_cli")

    with pytest.raises(RuntimeError, match="close failed"):
        asyncio.run(backend.run(_request()))


def test_session_llm_backend_enforces_request_deadline_while_opening():
    class SlowOpenBackend(FakeSessionBackend):
        async def open(self, spec):
            await asyncio.sleep(1)
            return await super().open(spec)

    session_backend = SlowOpenBackend()
    backend = SessionLlmBackend(backend=session_backend, kind="claude_cli")

    with pytest.raises(TimeoutError):
        asyncio.run(backend.run(replace(_request(), timeout_s=0.01)))

    assert session_backend.sent == []
    assert session_backend.closed == []


def _request() -> LlmRequest:
    return LlmRequest(
        prompt="hello",
        system_prompt="system",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
        model="test-model",
    )
