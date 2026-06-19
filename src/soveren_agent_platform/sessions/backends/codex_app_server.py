"""Codex app-server execution session backend."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolRegistry,
    DynamicToolSpec,
    normalize_dynamic_tool_specs,
)

log = logging.getLogger(__name__)

MIN_APP_SERVER_VERSION = (0, 125, 0)


class CodexAppServerError(RuntimeError):
    pass


@dataclass(slots=True)
class TurnState:
    turn_id: str
    done: asyncio.Event = field(default_factory=asyncio.Event)
    text_parts: list[str] = field(default_factory=list)
    timed_out: bool = False
    error: str | None = None

    @property
    def text(self) -> str:
        return "".join(self.text_parts).strip()


class CodexJsonRpcClient(Protocol):
    async def request(self, method: str, params: dict[str, Any]) -> Any:
        ...

    async def close(self) -> None:
        ...

    def set_last_turn(self, thread_id: str, turn_id: str) -> TurnState:
        ...

    def last_turn(self, thread_id: str) -> TurnState | None:
        ...


class JsonRpcStdioClient:
    def __init__(
        self,
        *,
        command: list[str],
        cwd: Path | None,
        env: dict[str, str],
        request_timeout_s: float,
        dynamic_tools: DynamicToolRegistry | None = None,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = env
        self.request_timeout_s = request_timeout_s
        self.dynamic_tools = dynamic_tools
        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._turns: dict[tuple[str, str], TurnState] = {}
        self._last_turn_by_thread: dict[str, TurnState] = {}

    async def start(self) -> None:
        if self._proc and self._proc.returncode is None:
            return
        self._proc = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(self.cwd) if self.cwd else None,
            env=self.env,
        )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())

    async def close(self) -> None:
        proc = self._proc
        if proc is None:
            return
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        for task in (self._reader_task, self._stderr_task):
            if task:
                task.cancel()

    async def request(self, method: str, params: dict[str, Any]) -> Any:
        await self.start()
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise CodexAppServerError("codex app-server process is not writable")
        request_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
        await proc.stdin.drain()
        try:
            return await asyncio.wait_for(future, timeout=self.request_timeout_s)
        finally:
            self._pending.pop(request_id, None)

    def set_last_turn(self, thread_id: str, turn_id: str) -> TurnState:
        state = self._turns.get((thread_id, turn_id)) or TurnState(turn_id=turn_id)
        self._turns[(thread_id, turn_id)] = state
        self._last_turn_by_thread[thread_id] = state
        return state

    def last_turn(self, thread_id: str) -> TurnState | None:
        return self._last_turn_by_thread.get(thread_id)

    async def _read_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        while True:
            raw = await self._proc.stdout.readline()
            if not raw:
                self._fail_pending("codex app-server stdout closed")
                return
            try:
                message = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                log.warning("codex app-server returned non-json line: %r", raw[:200])
                continue
            if "method" in message and "id" in message:
                await self._handle_server_request(message)
            elif "id" in message:
                self._handle_response(message)
            elif "method" in message:
                self._handle_notification(message)

    async def _read_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while True:
            raw = await self._proc.stderr.readline()
            if not raw:
                return
            log.info("codex app-server stderr: %s", raw.decode("utf-8", "replace").rstrip())

    def _handle_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        if not isinstance(request_id, int):
            return
        future = self._pending.get(request_id)
        if future is None or future.done():
            return
        if "error" in message:
            future.set_exception(CodexAppServerError(str(message["error"])[:1000]))
        else:
            future.set_result(message.get("result"))

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        params = message.get("params") or {}
        if method == "item/tool/call":
            result = await self._call_dynamic_tool(params)
            await self._send_response(message.get("id"), result)
            return
        await self._send_error(message.get("id"), code=-32601, message=f"unsupported server request: {method}")

    async def _call_dynamic_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        if self.dynamic_tools is None:
            return {
                "success": False,
                "contentItems": [
                    {"type": "inputText", "text": "Dynamic tools are not configured for this client."},
                ],
            }
        return await self.dynamic_tools.call(params)

    async def _send_response(self, request_id: Any, result: Any) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise CodexAppServerError("codex app-server process is not writable")
        payload = {"jsonrpc": "2.0", "id": request_id, "result": result}
        proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
        await proc.stdin.drain()

    async def _send_error(self, request_id: Any, *, code: int, message: str) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise CodexAppServerError("codex app-server process is not writable")
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
        proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
        await proc.stdin.drain()

    def _handle_notification(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params") or {}
        if method == "item/agentMessage/delta":
            thread_id = params.get("threadId")
            turn_id = params.get("turnId")
            if isinstance(thread_id, str) and isinstance(turn_id, str):
                key = (thread_id, turn_id)
                state = self._turns.setdefault(key, TurnState(turn_id=turn_id))
                state.text_parts.append(str(params.get("delta") or ""))
        elif method == "turn/completed":
            turn = params.get("turn") or {}
            thread_id = params.get("threadId")
            turn_id = turn.get("id")
            if isinstance(thread_id, str) and isinstance(turn_id, str):
                key = (thread_id, turn_id)
                state = self._turns.setdefault(key, TurnState(turn_id=turn_id))
                if turn.get("status") == "failed":
                    state.error = str(turn.get("error") or "turn failed")
                state.done.set()
        elif method == "error":
            log.warning("codex app-server error notification: %s", params)

    def _fail_pending(self, error: str) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(CodexAppServerError(error))


class CodexAppServerBackend:
    """SessionBackend implementation backed by Codex app-server threads."""

    name = "codex_app_server"

    def __init__(
        self,
        *,
        command: list[str] | None = None,
        codex_home: Path | None = None,
        model: str | None = None,
        sandbox: str = "workspace-write",
        approval_policy: str = "never",
        developer_instructions: str | None = None,
        dynamic_tools: DynamicToolRegistry | list[DynamicToolSpec | dict[str, Any]] | None = None,
        output_schema: dict[str, Any] | None = None,
        collaboration_mode: str | None = None,
        request_timeout_s: float = 15.0,
        turn_timeout_s: float = 180.0,
        client: CodexJsonRpcClient | None = None,
    ) -> None:
        self.command = command or ["codex", "app-server", "--listen", "stdio://"]
        self.codex_home = codex_home
        self.model = model
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.developer_instructions = developer_instructions
        self.dynamic_tools = dynamic_tools
        self.output_schema = output_schema
        self.collaboration_mode = collaboration_mode
        self.request_timeout_s = request_timeout_s
        self.turn_timeout_s = turn_timeout_s
        self._client: CodexJsonRpcClient | None = client
        self._initialized = client is not None
        self._loaded_thread_ids: set[str] = set()

    def env(self) -> dict[str, str]:
        allowed = {
            "PATH", "LANG", "LC_ALL", "LC_CTYPE", "TERM",
            "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY",
            "https_proxy", "http_proxy", "no_proxy",
        }
        env = {key: value for key, value in os.environ.items() if key in allowed}
        if self.codex_home is not None:
            env["CODEX_HOME"] = str(self.codex_home)
        return env

    async def open(self, spec: OpenSpec) -> OpenResult:
        if spec.kind not in ("codex", "codex_cli"):
            raise CodexAppServerError(f"Codex app-server cannot open kind={spec.kind!r}")
        Path(spec.cwd).mkdir(parents=True, exist_ok=True)
        await self.ensure_initialized()
        assert self._client is not None
        params: dict[str, Any] = {
            "cwd": spec.cwd,
            "approvalPolicy": self.approval_policy,
            "sandbox": self.sandbox,
            "ephemeral": False,
            "threadSource": "user",
            "sessionStartSource": "startup",
        }
        if self.model:
            params["model"] = self.model
        if self.developer_instructions:
            params["developerInstructions"] = self.developer_instructions
        dynamic_tools = self._dynamic_tool_specs()
        if dynamic_tools:
            params["dynamicTools"] = dynamic_tools
        result = await self._client.request("thread/start", params)
        thread = (result or {}).get("thread") or {}
        thread_id = thread.get("id")
        if not thread_id:
            raise CodexAppServerError("thread/start did not return thread.id")
        self._loaded_thread_ids.add(str(thread_id))
        return OpenResult(
            backend_session_id=str(thread_id),
            session_handle=str(thread_id),
            metadata={
                "thread_id": str(thread_id),
                "model": (result or {}).get("model"),
                "model_provider": (result or {}).get("modelProvider"),
                "cwd": (result or {}).get("cwd"),
                "runtime": self.name,
            },
        )

    async def send(self, backend_session_id: str, prompt: str) -> None:
        await self.ensure_thread(backend_session_id)
        assert self._client is not None
        params: dict[str, Any] = {
            "threadId": backend_session_id,
            "input": [{"type": "text", "text": prompt}],
        }
        if self.output_schema is not None:
            params["outputSchema"] = self.output_schema
        if self.collaboration_mode is not None:
            params["collaborationMode"] = self.collaboration_mode
        result = await self._client.request("turn/start", params)
        turn = (result or {}).get("turn") or {}
        turn_id = turn.get("id")
        if not turn_id:
            raise CodexAppServerError("turn/start did not return turn.id")
        self._client.set_last_turn(backend_session_id, str(turn_id))

    async def capture(self, backend_session_id: str) -> CaptureResult:
        await self.ensure_thread(backend_session_id)
        assert self._client is not None
        state = self._client.last_turn(backend_session_id)
        if state is None:
            return await self.capture_thread_history(backend_session_id)
        try:
            await asyncio.wait_for(state.done.wait(), timeout=self.turn_timeout_s)
        except asyncio.TimeoutError:
            state.timed_out = True
            return CaptureResult(text=state.text, timed_out=True)
        if state.error:
            raise CodexAppServerError(state.error)
        return CaptureResult(text=state.text, timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        await self.ensure_thread(backend_session_id)
        assert self._client is not None
        await self._client.request("thread/archive", {"threadId": backend_session_id})

    async def shutdown(self) -> None:
        if self._client is not None:
            await self._client.close()
        self._client = None
        self._initialized = False
        self._loaded_thread_ids.clear()

    async def ensure_thread(self, thread_id: str) -> None:
        await self.ensure_initialized()
        assert self._client is not None
        if thread_id in self._loaded_thread_ids:
            return
        params: dict[str, Any] = {"threadId": thread_id}
        if self.developer_instructions:
            params["developerInstructions"] = self.developer_instructions
        await self._client.request("thread/resume", params)
        self._loaded_thread_ids.add(thread_id)

    async def capture_thread_history(self, thread_id: str) -> CaptureResult:
        await self.ensure_thread(thread_id)
        assert self._client is not None
        result = await self._client.request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": True},
        )
        return CaptureResult(text=extract_thread_text(result), timed_out=False)

    async def ensure_initialized(self) -> None:
        if self._initialized:
            return
        self._client = JsonRpcStdioClient(
            command=self.command,
            cwd=None,
            env=self.env(),
            request_timeout_s=self.request_timeout_s,
            dynamic_tools=(
                self.dynamic_tools
                if isinstance(self.dynamic_tools, DynamicToolRegistry)
                else None
            ),
        )
        result = await self._client.request("initialize", {
            "clientInfo": {"name": "soveren-agent-platform", "version": "0.1.0"},
            "capabilities": {"experimentalApi": True, "optOutNotificationMethods": []},
        })
        user_agent = str((result or {}).get("userAgent") or "")
        version = parse_codex_version(user_agent)
        if version is not None and version < MIN_APP_SERVER_VERSION:
            await self.shutdown()
            raise CodexAppServerError(
                f"codex app-server version {version!r} is below required {MIN_APP_SERVER_VERSION!r}"
            )
        self._initialized = True

    def _dynamic_tool_specs(self) -> list[dict[str, Any]]:
        if self.dynamic_tools is None:
            return []
        if isinstance(self.dynamic_tools, DynamicToolRegistry):
            return self.dynamic_tools.app_server_specs()
        return normalize_dynamic_tool_specs(self.dynamic_tools)


def extract_thread_text(value: Any) -> str:
    parts: list[str] = []

    def visit(node: Any, *, in_agent_message: bool = False) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("type") or node.get("kind") or node.get("role") or "")
            is_agent = in_agent_message or node_type in {
                "agent_message", "agentMessage", "assistant",
            }
            for key in ("text", "content", "message", "delta"):
                text = node.get(key)
                if is_agent and isinstance(text, str):
                    parts.append(text)
            for child in node.values():
                visit(child, in_agent_message=is_agent)
        elif isinstance(node, list):
            for item in node:
                visit(item, in_agent_message=in_agent_message)

    visit(value)
    return "\n".join(part.strip() for part in parts if part.strip()).strip()


def parse_codex_version(user_agent: str) -> tuple[int, int, int] | None:
    match = re.search(r"/(\d+)\.(\d+)\.(\d+)", user_agent)
    if not match:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)
