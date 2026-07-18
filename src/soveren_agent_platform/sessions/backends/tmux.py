"""Low-level tmux command-session utility with explicit completion markers."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import uuid
from pathlib import Path

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec

log = logging.getLogger(__name__)

POLL_INTERVAL_S = 0.5
HARD_TIMEOUT_S = 90.0


class TmuxCommandSession:
    """Run a CLI in tmux without claiming implicit command completion."""

    name = "tmux"

    def __init__(
        self,
        *,
        socket: str,
        home: Path,
        command_for_kind: dict[str, list[str]],
        session_prefix: str = "soveren-agent-platform",
        poll_interval_s: float = POLL_INTERVAL_S,
        hard_timeout_s: float = HARD_TIMEOUT_S,
    ) -> None:
        self.socket = socket
        self.home = home
        self.command_for_kind = command_for_kind
        self.session_prefix = session_prefix
        self.poll_interval_s = poll_interval_s
        self.hard_timeout_s = hard_timeout_s

    def tmux(self, *args: str) -> list[str]:
        return ["tmux", "-L", self.socket, *args]

    def env(self) -> dict[str, str]:
        env = {
            "HOME": str(self.home),
            "PATH": os.environ.get("PATH", ""),
            "LANG": os.environ.get("LANG", "en_US.UTF-8"),
            "TERM": "xterm-256color",
        }
        for key in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy"):
            value = os.environ.get(key)
            if value:
                env[key] = value
        return env

    async def open(self, spec: OpenSpec) -> OpenResult:
        if shutil.which("tmux") is None:
            raise RuntimeError("tmux binary not found in PATH")
        command = self.command_for_kind.get(spec.kind)
        if not command:
            raise RuntimeError(f"no command registered for kind={spec.kind!r}")

        short_id = uuid.uuid4().hex[:8]
        backend_session_id = f"{self.session_prefix}-{spec.kind.replace('_cli', '')}-{short_id}"
        Path(spec.cwd).mkdir(parents=True, exist_ok=True)
        self.home.mkdir(parents=True, exist_ok=True)

        rc, _, err = await self.run_command(
            self.tmux("new-session", "-d", "-s", backend_session_id, "-c", spec.cwd, *command)
        )
        if rc != 0:
            raise RuntimeError(f"tmux new-session failed rc={rc} err={err.strip()!r}")

        rc, pane_id, _ = await self.run_command(
            self.tmux("display-message", "-p", "-t", backend_session_id, "#{pane_id}")
        )
        pane_id_s = pane_id.strip() if rc == 0 else None
        log.info("tmux open kind=%s session=%s pane=%s cwd=%s", spec.kind, backend_session_id, pane_id_s, spec.cwd)
        return OpenResult(
            backend_session_id=backend_session_id,
            session_handle=backend_session_id,
            metadata={"pane_id": pane_id_s},
        )

    async def send(self, backend_session_id: str, prompt: str) -> None:
        buffer_name = f"soveren-agent-platform-{uuid.uuid4().hex}"
        rc, _, err = await self.run_command(
            self.tmux("load-buffer", "-b", buffer_name, "-"),
            input_text=prompt.rstrip("\n") + "\n",
        )
        if rc != 0:
            raise RuntimeError(f"tmux load-buffer failed rc={rc} err={err.strip()!r}")
        rc, _, err = await self.run_command(
            self.tmux("paste-buffer", "-d", "-b", buffer_name, "-t", backend_session_id)
        )
        if rc != 0:
            raise RuntimeError(f"tmux paste-buffer failed rc={rc} err={err.strip()!r}")

    async def capture_until(
        self,
        backend_session_id: str,
        marker: str,
        *,
        timeout_s: float | None = None,
    ) -> CaptureResult:
        deadline = asyncio.get_event_loop().time() + (timeout_s if timeout_s is not None else self.hard_timeout_s)
        marker_line = re.compile(rf"(?m)^\s*{re.escape(marker)}\s*$")
        out = ""
        while True:
            out = await self.capture_text(backend_session_id)
            if marker_line.search(out):
                return CaptureResult(text=out, timed_out=False)
            if asyncio.get_event_loop().time() >= deadline:
                return CaptureResult(text=out, timed_out=True)
            await asyncio.sleep(self.poll_interval_s)

    async def close(self, backend_session_id: str) -> None:
        rc, _, err = await self.run_command(self.tmux("kill-session", "-t", backend_session_id))
        if rc != 0:
            normalized = err.lower()
            if "can't find session" in normalized or "no server running" in normalized:
                return
            raise RuntimeError(f"tmux kill-session failed rc={rc} err={err.strip()!r}")

    async def capture_text(self, backend_session_id: str) -> str:
        rc, out, err = await self.run_command(self.tmux("capture-pane", "-t", backend_session_id, "-p", "-S", "-3000"))
        if rc != 0:
            raise RuntimeError(f"tmux capture-pane failed rc={rc} err={err.strip()!r}")
        return out

    async def run_command(
        self,
        argv: list[str],
        *,
        input_text: str | None = None,
    ) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE if input_text is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env(),
        )
        stdin = input_text.encode("utf-8") if input_text is not None else None
        stdout, stderr = await proc.communicate(stdin)
        return proc.returncode or 0, stdout.decode("utf-8", "replace"), stderr.decode("utf-8", "replace")
