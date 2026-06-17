import asyncio
from types import SimpleNamespace

import pytest

from agent_platform.sessions import CaptureResult, CodexAppServerBackend, CodexAppServerError, OpenSpec
from agent_platform.sessions.backends.codex_app_server import (
    TurnState,
    extract_thread_text,
    parse_codex_version,
)


def test_parse_codex_app_server_version():
    assert parse_codex_version("Codex Desktop/0.130.0-alpha.5 (Mac OS)") == (0, 130, 0)
    assert parse_codex_version("Codex CLI/1.2.3") == (1, 2, 3)
    assert parse_codex_version("no-version") is None


def test_codex_backend_env_filters_product_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENAI_API_KEY", "should-not-leak")
    monkeypatch.setenv("CODEX_API_KEY", "should-not-leak")
    monkeypatch.setenv("TG_BOT_TOKEN", "should-not-leak")
    monkeypatch.setenv("CLICKUP_API_TOKEN", "should-not-leak")
    monkeypatch.setenv("OPENROUTER_API_KEY", "should-not-leak")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy")
    backend = CodexAppServerBackend(codex_home=tmp_path)

    env = backend.env()

    assert env["CODEX_HOME"] == str(tmp_path)
    assert env["HTTPS_PROXY"] == "http://proxy"
    assert "OPENAI_API_KEY" not in env
    assert "CODEX_API_KEY" not in env
    assert "TG_BOT_TOKEN" not in env
    assert "CLICKUP_API_TOKEN" not in env
    assert "OPENROUTER_API_KEY" not in env


def test_extract_thread_text_only_returns_agent_messages():
    payload = {
        "thread": {
            "items": [
                {"role": "user", "text": "ignore me"},
                {"role": "assistant", "text": "answer one"},
                {"type": "agent_message", "content": "answer two"},
            ]
        }
    }

    assert extract_thread_text(payload) == "answer one\nanswer two"


class FakeCodexClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.last_turns: dict[str, object] = {}
        self.closed = False

    async def request(self, method: str, params: dict):
        self.calls.append((method, params))
        if method == "initialize":
            return {"userAgent": "Codex CLI/0.130.0"}
        if method == "thread/start":
            return {
                "thread": {"id": "thread_new"},
                "model": params.get("model"),
                "modelProvider": "openai",
                "cwd": params.get("cwd"),
            }
        if method == "turn/start":
            return {"turn": {"id": "turn_1"}}
        if method == "thread/read":
            return {"thread": {"items": [{"role": "assistant", "text": "restored answer"}]}}
        return {}

    async def close(self) -> None:
        self.closed = True

    def set_last_turn(self, thread_id: str, turn_id: str):
        state = SimpleNamespace(turn_id=turn_id)
        self.last_turns[thread_id] = state
        return state

    def last_turn(self, thread_id: str):
        return self.last_turns.get(thread_id)


def test_codex_backend_open_initializes_and_starts_thread(tmp_path):
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake, model="gpt-5.4")
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))
        return fake, opened

    fake, opened = asyncio.run(run())

    assert opened.backend_session_id == "thread_new"
    assert opened.metadata["model"] == "gpt-5.4"
    assert fake.calls == [
        (
            "thread/start",
            {
                "cwd": str(tmp_path / "work"),
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
                "ephemeral": False,
                "threadSource": "user",
                "sessionStartSource": "startup",
                "model": "gpt-5.4",
            },
        )
    ]


def test_codex_backend_rejects_non_codex_kind(tmp_path):
    async def run():
        await CodexAppServerBackend(client=FakeCodexClient()).open(
            OpenSpec(kind="claude_cli", cwd=str(tmp_path))
        )

    with pytest.raises(CodexAppServerError, match="cannot open"):
        asyncio.run(run())


def test_codex_backend_resumes_persisted_thread_before_send():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.send("thread_existing", "hello")
        return fake

    fake = asyncio.run(run())

    assert fake.calls[0] == ("thread/resume", {"threadId": "thread_existing"})
    assert fake.calls[1] == (
        "turn/start",
        {"threadId": "thread_existing", "input": [{"type": "text", "text": "hello"}]},
    )


def test_codex_backend_capture_after_restart_reads_thread_history():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture("thread_existing")
        return fake, result

    fake, result = asyncio.run(run())

    assert isinstance(result, CaptureResult)
    assert result.text == "restored answer"
    assert fake.calls == [
        ("thread/resume", {"threadId": "thread_existing"}),
        ("thread/read", {"threadId": "thread_existing", "includeTurns": True}),
    ]


def test_codex_backend_capture_waits_for_last_turn():
    async def run():
        fake = FakeCodexClient()
        state = TurnState(turn_id="turn_1")
        state.text_parts.append("done")
        state.done.set()
        fake.last_turns["thread_existing"] = state
        backend = CodexAppServerBackend(client=fake)
        result = await backend.capture("thread_existing")
        return result

    result = asyncio.run(run())

    assert result.text == "done"
    assert result.timed_out is False


def test_codex_backend_close_resumes_and_archives_thread():
    async def run():
        fake = FakeCodexClient()
        backend = CodexAppServerBackend(client=fake)
        await backend.close("thread_existing")
        return fake

    fake = asyncio.run(run())

    assert fake.calls == [
        ("thread/resume", {"threadId": "thread_existing"}),
        ("thread/archive", {"threadId": "thread_existing"}),
    ]

