import asyncio
import shutil

import pytest

from agent_platform.sessions import OpenSpec, StubBackend, TmuxBackend


def test_stub_backend_roundtrip():
    async def run():
        backend = StubBackend()
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd="/tmp/work"))
        await backend.send(opened.backend_session_id, "hello")
        captured = await backend.capture(opened.backend_session_id)
        await backend.close(opened.backend_session_id)
        after_close = await backend.capture(opened.backend_session_id)
        return opened, captured, after_close

    opened, captured, after_close = asyncio.run(run())

    assert opened.backend_session_id.startswith("stub-codex-")
    assert "hello" in captured.text
    assert captured.timed_out is False
    assert after_close.text == ""


def test_tmux_backend_env_and_command_helpers(tmp_path, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy")
    backend = TmuxBackend(
        socket="agent-test",
        home=tmp_path / "home",
        command_for_kind={"codex_cli": ["codex"]},
    )

    assert backend.tmux("list-sessions") == ["tmux", "-L", "agent-test", "list-sessions"]
    env = backend.env()
    assert env["HOME"] == str(tmp_path / "home")
    assert env["HTTPS_PROXY"] == "http://proxy"


def test_tmux_backend_open_requires_tmux_binary(tmp_path, monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda name: None)
    backend = TmuxBackend(
        socket="agent-test",
        home=tmp_path / "home",
        command_for_kind={"codex_cli": ["codex"]},
    )

    async def run():
        await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path / "work")))

    with pytest.raises(RuntimeError, match="tmux binary not found"):
        asyncio.run(run())


def test_tmux_backend_send_uses_load_and_paste_buffer(tmp_path):
    class FakeTmuxBackend(TmuxBackend):
        def __init__(self) -> None:
            super().__init__(
                socket="agent-test",
                home=tmp_path / "home",
                command_for_kind={"codex_cli": ["codex"]},
            )
            self.commands: list[tuple[list[str], str | None]] = []

        async def run_command(self, argv, *, input_text=None):
            self.commands.append((argv, input_text))
            return 0, "", ""

    backend = FakeTmuxBackend()

    asyncio.run(backend.send("session-1", "hello"))

    assert backend.commands[0][0][:4] == ["tmux", "-L", "agent-test", "load-buffer"]
    assert backend.commands[0][1] == "hello\n"
    assert backend.commands[1][0][:4] == ["tmux", "-L", "agent-test", "paste-buffer"]
    assert backend.commands[1][0][-1] == "session-1"


def test_tmux_backend_capture_until_marker(tmp_path):
    class FakeTmuxBackend(TmuxBackend):
        def __init__(self) -> None:
            super().__init__(
                socket="agent-test",
                home=tmp_path / "home",
                command_for_kind={"codex_cli": ["codex"]},
                poll_interval_s=0.001,
                hard_timeout_s=0.1,
            )
            self.outputs = iter(["working", "working\nDONE\n"])

        async def capture_text(self, backend_session_id: str) -> str:
            return next(self.outputs)

    result = asyncio.run(FakeTmuxBackend().capture_until("session-1", "DONE", timeout_s=0.1))

    assert result.timed_out is False
    assert "DONE" in result.text

