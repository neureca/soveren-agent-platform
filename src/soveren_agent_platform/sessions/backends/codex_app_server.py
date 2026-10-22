"""Codex app-server execution session backend."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from soveren_agent_platform import __version__
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import (
    DynamicToolAllowlist,
    ToolExecutionContext,
    validate_dynamic_tool_allowlist,
)
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.runtime.failures import NonRetryableEventError
from soveren_agent_platform.runtime.latency import CodexTurnTiming, measure_codex_runtime, record_codex_turn_timing
from soveren_agent_platform.sessions.backend import (
    CaptureResult,
    OpenResult,
    OpenSpec,
    SendReceipt,
    TenantBoundaryError,
    UnsupportedContextualSessionError,
    ensure_conversation_scope,
)
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolRegistry,
    DynamicToolSpec,
    normalize_dynamic_tool_specs,
)
from soveren_agent_platform.sessions.codex_credentials import CodexClientInfo

log = logging.getLogger(__name__)

MIN_APP_SERVER_VERSION = (0, 125, 0)
DEFAULT_MAX_CONCURRENT_DYNAMIC_TOOL_CALLS = 8
DEFAULT_MAX_JSON_RPC_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_TURN_OUTPUT_BYTES = 1024 * 1024
STDERR_CHUNK_BYTES = 8 * 1024
STDERR_LOG_BUDGET_BYTES = 64 * 1024
TURN_RECOVERY_PAGE_SIZE = 1


class CodexAppServerError(RuntimeError):
    pass


class CodexTurnFailure(CodexAppServerError, NonRetryableEventError):
    """An accepted Codex turn must not be replayed by the outer event worker."""

    def __init__(
        self, turn_id: str, *, reason: str, http_status_code: int | None = None, detail: str | None = None,
    ) -> None:
        self.reason = reason
        self.http_status_code = http_status_code
        self.interrupt_failed = False
        status_detail = f" (HTTP {http_status_code})" if http_status_code is not None else ""
        message_detail = f": {detail[:1000]}" if detail else ""
        super().__init__(f"Codex turn {turn_id} failed: {reason}{status_detail}{message_detail}")


class CodexJsonRpcFrameTooLargeError(CodexAppServerError):
    pass


class CodexTurnOutputLimitError(CodexAppServerError):
    pass


@dataclass(frozen=True, slots=True)
class CodexCollaborationMode:
    """Typed Codex collaboration preset sent with a turn."""

    mode: Literal["default", "plan"]
    model: str
    reasoning_effort: str | None = None
    developer_instructions: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in ("default", "plan"):
            raise ValueError("Codex collaboration mode must be 'default' or 'plan'")
        if not isinstance(self.model, str):
            raise TypeError("Codex collaboration mode model must be a string")
        model = self.model.strip()
        if not model:
            raise ValueError("Codex collaboration mode model must be non-empty")
        object.__setattr__(self, "model", model)
        if self.reasoning_effort is not None:
            if not isinstance(self.reasoning_effort, str):
                raise TypeError("Codex collaboration mode reasoning_effort must be a string")
            reasoning_effort = self.reasoning_effort.strip()
            if not reasoning_effort:
                raise ValueError("Codex collaboration mode reasoning_effort must be non-empty")
            object.__setattr__(self, "reasoning_effort", reasoning_effort)
        if self.developer_instructions is not None and not isinstance(self.developer_instructions, str):
            raise TypeError("Codex collaboration mode developer_instructions must be a string")

    def app_server_payload(self) -> dict[str, Any]:
        settings: dict[str, Any] = {"model": self.model}
        if self.reasoning_effort is not None:
            settings["reasoning_effort"] = self.reasoning_effort
        if self.developer_instructions is not None:
            settings["developer_instructions"] = self.developer_instructions
        return {"mode": self.mode, "settings": settings}


@dataclass(slots=True)
class TurnState:
    turn_id: str
    done: asyncio.Event = field(default_factory=asyncio.Event)
    text_parts: list[str] = field(default_factory=list)
    text_bytes: int = 0
    output_limit_exceeded: bool = False
    interrupt_requested: bool = False
    timed_out: bool = False
    error: str | None = None
    failure: CodexTurnFailure | None = None
    reconnects: int = 0
    timing_key: str = field(default_factory=lambda: uuid.uuid4().hex)
    start_requested_at: float | None = None
    start_ack_at: float | None = None
    first_output_at: float | None = None
    completed_at: float | None = None
    notification_tracking: bool = False
    execution_context: ToolExecutionContext | None = None
    dynamic_tool_allowlist: DynamicToolAllowlist | None = None
    start_acknowledged: bool = False

    @property
    def text(self) -> str:
        return "".join(self.text_parts).strip()


@dataclass(slots=True)
class PreparedTurnContext:
    thread_id: str
    execution_context: ToolExecutionContext | None
    dynamic_tool_allowlist: DynamicToolAllowlist | None
    acknowledged: asyncio.Event = field(default_factory=asyncio.Event)
    turn_id: str | None = None
    failed: bool = False


def _record_turn_timing(state: object) -> None:
    if not isinstance(state, TurnState):
        return
    requested = state.start_requested_at
    first = state.first_output_at
    completed = state.completed_at
    ack = state.start_ack_at
    reason = "no_agent_message_delta" if first is None else "completion_not_observed"
    if requested is None:
        reason = "turn_start_not_observed"
    record_codex_turn_timing(state.timing_key, CodexTurnTiming(
        start_ack_s=ack - requested if ack is not None and requested is not None else None,
        start_to_first_output_s=first - requested if first is not None and requested is not None else None,
        first_output_to_completion_s=completed - first if first is not None and completed is not None else None,
        reconnects=state.reconnects if state.notification_tracking else None,
        unavailable_reason=reason,
        request_started_at=requested,
        observed_until=max((point for point in (ack, first, completed) if point is not None), default=None),
    ))


class CodexJsonRpcClient(Protocol):
    async def request(self, method: str, params: dict[str, Any]) -> Any:
        ...

    async def close(self) -> None:
        ...

    def set_last_turn(self, thread_id: str, turn_id: str) -> TurnState:
        ...

    def last_turn(self, thread_id: str) -> TurnState | None:
        ...

    def release_turn(self, thread_id: str, turn_id: str) -> None:
        ...

    def release_thread(self, thread_id: str) -> None:
        ...

    def prepare_turn_context(
        self,
        thread_id: str,
        *,
        execution_context: ToolExecutionContext | None,
        dynamic_tool_allowlist: DynamicToolAllowlist | None,
    ) -> PreparedTurnContext:
        ...

    def acknowledge_turn_context(
        self,
        thread_id: str,
        turn_id: str,
        state: TurnState,
    ) -> None:
        ...

    def fail_turn_context(self, thread_id: str) -> None:
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
        max_concurrent_dynamic_tool_calls: int = DEFAULT_MAX_CONCURRENT_DYNAMIC_TOOL_CALLS,
        max_json_rpc_frame_bytes: int = DEFAULT_MAX_JSON_RPC_FRAME_BYTES,
        max_turn_output_bytes: int = DEFAULT_MAX_TURN_OUTPUT_BYTES,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = env
        self.request_timeout_s = request_timeout_s
        self.dynamic_tools = dynamic_tools
        self.max_concurrent_dynamic_tool_calls = _positive_int(
            max_concurrent_dynamic_tool_calls,
            name="max_concurrent_dynamic_tool_calls",
        )
        self.max_json_rpc_frame_bytes = _positive_int(
            max_json_rpc_frame_bytes,
            name="max_json_rpc_frame_bytes",
        )
        self.max_turn_output_bytes = _positive_int(
            max_turn_output_bytes,
            name="max_turn_output_bytes",
        )
        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._server_request_tasks: set[asyncio.Task[None]] = set()
        self._turn_interrupt_tasks: set[asyncio.Task[Any]] = set()
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._turns: dict[tuple[str, str], TurnState] = {}
        self._last_turn_by_thread: dict[str, TurnState] = {}
        self._failed_turn_by_thread: dict[str, str] = {}
        self._prepared_turn_contexts: dict[str, PreparedTurnContext] = {}
        self._start_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._terminal_error: str | None = None

    @property
    def failed(self) -> bool:
        return self._terminal_error is not None

    async def start(self) -> None:
        if self._terminal_error is not None:
            raise CodexAppServerError(self._terminal_error)
        if self._proc and self._proc.returncode is None:
            return
        async with self._start_lock:
            if self._terminal_error is not None:
                raise CodexAppServerError(self._terminal_error)
            if self._proc and self._proc.returncode is None:
                return
            self._proc = await asyncio.create_subprocess_exec(
                *self.command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.cwd) if self.cwd else None,
                env=self.env,
                limit=self.max_json_rpc_frame_bytes,
            )
            self._reader_task = asyncio.create_task(self._read_stdout())
            self._stderr_task = asyncio.create_task(self._read_stderr())

    async def close(self) -> None:
        proc = self._proc
        if proc is None:
            self._turns.clear()
            self._last_turn_by_thread.clear()
            self._failed_turn_by_thread.clear()
            self._fail_prepared_turn_contexts()
            return
        self._mark_failed(self._terminal_error or "codex app-server client is closed")
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        current_task = asyncio.current_task()
        tasks = [
            task
            for task in (
                self._reader_task,
                self._stderr_task,
                *self._server_request_tasks,
                *self._turn_interrupt_tasks,
            )
            if task is not None and task is not current_task
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._server_request_tasks.clear()
        self._turn_interrupt_tasks.clear()
        self._reader_task = None
        self._stderr_task = None
        self._turns.clear()
        self._last_turn_by_thread.clear()
        self._failed_turn_by_thread.clear()
        self._fail_prepared_turn_contexts()

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
        try:
            await self._write_message(payload)
            return await asyncio.wait_for(future, timeout=self.request_timeout_s)
        finally:
            self._pending.pop(request_id, None)

    def set_last_turn(self, thread_id: str, turn_id: str) -> TurnState:
        state = self._turns.get((thread_id, turn_id)) or TurnState(turn_id=turn_id)
        state.notification_tracking = True
        self._turns[(thread_id, turn_id)] = state
        self._last_turn_by_thread[thread_id] = state
        return state

    def prepare_turn_context(
        self,
        thread_id: str,
        *,
        execution_context: ToolExecutionContext | None,
        dynamic_tool_allowlist: DynamicToolAllowlist | None,
    ) -> PreparedTurnContext:
        if thread_id in self._prepared_turn_contexts:
            raise CodexAppServerError("a turn is already starting for this thread")
        binding = PreparedTurnContext(
            thread_id=thread_id,
            execution_context=execution_context,
            dynamic_tool_allowlist=dynamic_tool_allowlist,
        )
        self._prepared_turn_contexts[thread_id] = binding
        return binding

    def acknowledge_turn_context(self, thread_id: str, turn_id: str, state: TurnState) -> None:
        binding = self._prepared_turn_contexts.pop(thread_id, None)
        if binding is None:
            raise CodexAppServerError("contextual turn binding was lost before turn/start ACK")
        binding.turn_id = turn_id
        state.execution_context = binding.execution_context
        state.dynamic_tool_allowlist = binding.dynamic_tool_allowlist
        state.start_acknowledged = True
        binding.acknowledged.set()

    def fail_turn_context(self, thread_id: str) -> None:
        binding = self._prepared_turn_contexts.pop(thread_id, None)
        if binding is not None:
            binding.failed = True
            binding.acknowledged.set()

    async def contextual_turn_for_tool_call(
        self,
        thread_id: str,
        turn_id: str,
    ) -> TurnState:
        binding = self._prepared_turn_contexts.get(thread_id)
        if binding is not None:
            try:
                await asyncio.wait_for(
                    binding.acknowledged.wait(),
                    timeout=self.request_timeout_s,
                )
            except TimeoutError as exc:
                raise CodexAppServerError("contextual turn start was not acknowledged") from exc
            if binding.failed or binding.turn_id != turn_id:
                raise CodexAppServerError("tool call does not match the acknowledged contextual turn")
        state = self._turns.get((thread_id, turn_id))
        if (
            state is None
            or not state.start_acknowledged
            or state.done.is_set()
            or state.failure is not None
            or state.error is not None
        ):
            raise CodexAppServerError("tool call has no active trusted execution context")
        return state

    def last_turn(self, thread_id: str) -> TurnState | None:
        return self._last_turn_by_thread.get(thread_id)

    def release_turn(self, thread_id: str, turn_id: str) -> None:
        state = self._turns.pop((thread_id, turn_id), None)
        if state is not None and self._last_turn_by_thread.get(thread_id) is state:
            self._last_turn_by_thread.pop(thread_id, None)
        if state is not None and state.failure is not None:
            self._failed_turn_by_thread[thread_id] = turn_id
        if state is not None:
            state.execution_context = None
            state.dynamic_tool_allowlist = None

    def release_thread(self, thread_id: str) -> None:
        self.fail_turn_context(thread_id)
        self._last_turn_by_thread.pop(thread_id, None)
        self._failed_turn_by_thread.pop(thread_id, None)
        for key in [key for key in self._turns if key[0] == thread_id]:
            state = self._turns.pop(key, None)
            if state is not None:
                state.execution_context = None
                state.dynamic_tool_allowlist = None

    async def _read_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        try:
            while True:
                raw = await self._read_json_rpc_frame()
                if not raw:
                    self._mark_failed("codex app-server stdout closed")
                    return
                message = json.loads(raw.decode("utf-8"))
                if not isinstance(message, dict):
                    raise CodexAppServerError("codex app-server returned a non-object JSON-RPC frame")
                if "method" in message and "id" in message:
                    if not self._start_server_request(message):
                        await self._reject_server_request(message)
                elif "id" in message:
                    self._handle_response(message)
                elif "method" in message:
                    self._handle_notification(message)
        except asyncio.CancelledError:
            raise
        except json.JSONDecodeError as exc:
            log.error("codex app-server returned invalid JSON", exc_info=exc)
            self._mark_failed("codex app-server returned invalid JSON")
        except CodexJsonRpcFrameTooLargeError as exc:
            log.error("codex app-server JSON-RPC frame exceeded the configured limit", exc_info=exc)
            self._mark_failed(str(exc))
        except Exception as exc:
            log.error("codex app-server reader failed", exc_info=exc)
            self._mark_failed(str(exc) if isinstance(exc, CodexAppServerError) else "codex app-server reader failed")

    async def _read_json_rpc_frame(self) -> bytes:
        assert self._proc is not None and self._proc.stdout is not None
        try:
            raw = await self._proc.stdout.readuntil(b"\n")
        except asyncio.IncompleteReadError as exc:
            if not exc.partial:
                return b""
            raise CodexAppServerError("codex app-server stdout closed mid-frame") from exc
        except asyncio.LimitOverrunError as exc:
            raise CodexJsonRpcFrameTooLargeError(
                f"codex app-server JSON-RPC frame exceeds {self.max_json_rpc_frame_bytes} bytes"
            ) from exc
        frame = raw[:-1] if raw.endswith(b"\n") else raw
        if frame.endswith(b"\r"):
            frame = frame[:-1]
        if len(frame) > self.max_json_rpc_frame_bytes:
            raise CodexJsonRpcFrameTooLargeError(
                f"codex app-server JSON-RPC frame exceeds {self.max_json_rpc_frame_bytes} bytes"
            )
        return frame

    async def _read_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        logged_bytes = 0
        suppression_logged = False
        try:
            while True:
                raw = await self._proc.stderr.read(STDERR_CHUNK_BYTES)
                if not raw:
                    return
                remaining = max(0, STDERR_LOG_BUDGET_BYTES - logged_bytes)
                if remaining:
                    visible = raw[:remaining]
                    logged_bytes += len(visible)
                    log.info("codex app-server stderr: %s", visible.decode("utf-8", "replace").rstrip())
                if len(raw) > remaining and not suppression_logged:
                    suppression_logged = True
                    log.warning(
                        "codex app-server stderr logging suppressed after %d bytes",
                        STDERR_LOG_BUDGET_BYTES,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("codex app-server stderr reader failed", exc_info=exc)

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
            thread_id, turn_id = params.get("threadId"), params.get("turnId")
            try:
                if not isinstance(thread_id, str) or not isinstance(turn_id, str):
                    raise CodexAppServerError("tool call does not name an active native turn")
                state = await self.contextual_turn_for_tool_call(thread_id, turn_id)
            except CodexAppServerError:
                await self._send_response(message.get("id"), {
                    "success": False,
                    "contentItems": [{
                        "type": "inputText",
                        "text": "This tool call has no active trusted execution context.",
                    }],
                })
                return
            result = await self._call_dynamic_tool(
                params,
                execution_context=state.execution_context,
                allowlist=state.dynamic_tool_allowlist,
            )
            await self._send_response(message.get("id"), result)
            return
        await self._send_error(message.get("id"), code=-32601, message=f"unsupported server request: {method}")

    def _start_server_request(self, message: dict[str, Any]) -> bool:
        if len(self._server_request_tasks) >= self.max_concurrent_dynamic_tool_calls:
            return False
        task = asyncio.create_task(
            self._handle_server_request(message),
            name=f"soveren-codex-server-request:{message.get('id')}",
        )
        self._server_request_tasks.add(task)
        task.add_done_callback(self._server_request_finished)
        return True

    async def _reject_server_request(self, message: dict[str, Any]) -> None:
        if message.get("method") == "item/tool/call":
            await self._send_response(
                message.get("id"),
                {
                    "success": False,
                    "contentItems": [
                        {
                            "type": "inputText",
                            "text": "Dynamic tool capacity is exhausted for this conversation.",
                        },
                    ],
                },
            )
            return
        await self._send_error(
            message.get("id"),
            code=-32000,
            message="server request capacity is exhausted",
        )

    def _server_request_finished(self, task: asyncio.Task[None]) -> None:
        self._server_request_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        log.error("codex app-server request handler failed: %s", error)
        self._mark_failed("codex app-server request handler failed")

    async def _call_dynamic_tool(
        self,
        params: dict[str, Any],
        *,
        execution_context: ToolExecutionContext | None = None,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> dict[str, Any]:
        if self.dynamic_tools is None:
            return {
                "success": False,
                "contentItems": [
                    {"type": "inputText", "text": "Dynamic tools are not configured for this client."},
                ],
            }
        return await self.dynamic_tools.call(
            params,
            execution_context=execution_context,
            allowlist=allowlist,
        )

    async def _send_response(self, request_id: Any, result: Any) -> None:
        await self._write_message({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def _send_error(self, request_id: Any, *, code: int, message: str) -> None:
        await self._write_message(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            }
        )

    async def _write_message(self, payload: dict[str, Any]) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise CodexAppServerError("codex app-server process is not writable")
        encoded = (json.dumps(payload, ensure_ascii=False) + "\n").encode()
        if len(encoded) - 1 > self.max_json_rpc_frame_bytes:
            raise CodexJsonRpcFrameTooLargeError(
                f"outbound Codex JSON-RPC frame exceeds {self.max_json_rpc_frame_bytes} bytes"
            )
        async with self._write_lock:
            try:
                proc.stdin.write(encoded)
                await proc.stdin.drain()
            except (BrokenPipeError, ConnectionError) as exc:
                self._mark_failed("codex app-server stdin closed")
                raise CodexAppServerError("codex app-server stdin closed") from exc

    def _handle_notification(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params") or {}
        thread_id = params.get("threadId")
        turn_id = params.get("turnId")
        if method == "turn/completed":
            turn_id = (params.get("turn") or {}).get("id")
        if isinstance(thread_id, str) and self._failed_turn_by_thread.get(thread_id) == turn_id:
            return
        if method == "item/agentMessage/delta":
            thread_id = params.get("threadId")
            turn_id = params.get("turnId")
            if isinstance(thread_id, str) and isinstance(turn_id, str):
                key = (thread_id, turn_id)
                state = self._turns.setdefault(key, TurnState(turn_id=turn_id))
                state.notification_tracking = True
                delta = str(params.get("delta") or "")
                if delta and state.first_output_at is None and not state.done.is_set():
                    state.first_output_at = time.monotonic()
                delta_bytes = len(delta.encode("utf-8"))
                if state.output_limit_exceeded:
                    return
                if state.text_bytes + delta_bytes > self.max_turn_output_bytes:
                    state.output_limit_exceeded = True
                    state.error = f"Codex turn {turn_id} output exceeds {self.max_turn_output_bytes} bytes"
                    self._start_turn_interrupt(thread_id, state)
                    return
                state.text_parts.append(delta)
                state.text_bytes += delta_bytes
        elif method == "turn/completed":
            turn = params.get("turn") or {}
            thread_id = params.get("threadId")
            turn_id = turn.get("id")
            if isinstance(thread_id, str) and isinstance(turn_id, str):
                key = (thread_id, turn_id)
                state = self._turns.setdefault(key, TurnState(turn_id=turn_id))
                state.notification_tracking = True
                if state.completed_at is None:
                    state.completed_at = time.monotonic()
                if state.failure is None and turn.get("status") == "failed":
                    state.failure = codex_turn_failure(turn_id, turn.get("error"))
                if state.failure is None and turn.get("status") == "interrupted":
                    state.failure = CodexTurnFailure(turn_id, reason="turn_interrupted")
                state.error = state.error or terminal_turn_error(turn)
                state.done.set()
        elif method == "error":
            thread_id, turn_id = params.get("threadId"), params.get("turnId")
            if not isinstance(thread_id, str) or not isinstance(turn_id, str):
                return
            state = self._turns.setdefault((thread_id, turn_id), TurnState(turn_id=turn_id))
            state.notification_tracking = True
            if state.done.is_set() or state.failure is not None:
                return
            failure = codex_turn_failure(turn_id, params.get("error"))
            if failure.reason == "authentication_failed" or params.get("willRetry") is False:
                self._start_failure_interrupt(thread_id, state, failure)
            elif params.get("willRetry") is True:
                state.reconnects += 1
            log.warning("Codex error thread=%s turn=%s kind=%s status=%s reconnects=%s",
                        thread_id, turn_id, failure.reason, failure.http_status_code, state.reconnects)

    def _start_failure_interrupt(self, thread_id: str, state: TurnState, failure: CodexTurnFailure) -> None:
        state.failure = failure
        state.error = str(failure)
        if state.interrupt_requested:
            return
        state.interrupt_requested = True
        task = asyncio.create_task(
            self._interrupt_failed_turn(thread_id, state),
            name=f"soveren-codex-failed-turn-interrupt:{state.turn_id}",
        )
        self._turn_interrupt_tasks.add(task)
        task.add_done_callback(self._turn_interrupt_finished)

    async def _interrupt_failed_turn(self, thread_id: str, state: TurnState) -> None:
        try:
            await self.request("turn/interrupt", {"threadId": thread_id, "turnId": state.turn_id})
        except Exception:
            assert state.failure is not None
            state.failure.interrupt_failed = True
            self._mark_failed("Codex turn interruption could not be confirmed")
            proc = self._proc
            if proc is not None and proc.returncode is None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
        finally:
            state.done.set()

    def _start_turn_interrupt(self, thread_id: str, state: TurnState) -> None:
        if state.interrupt_requested:
            return
        state.interrupt_requested = True
        task = asyncio.create_task(
            self.request(
                "turn/interrupt",
                {"threadId": thread_id, "turnId": state.turn_id},
            ),
            name=f"soveren-codex-output-limit-interrupt:{state.turn_id}",
        )
        self._turn_interrupt_tasks.add(task)
        task.add_done_callback(self._turn_interrupt_finished)

    def _turn_interrupt_finished(self, task: asyncio.Task[Any]) -> None:
        self._turn_interrupt_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            log.error("failed to interrupt Codex turn", exc_info=error)
            self._mark_failed("failed to interrupt Codex turn")

    def _fail_pending(self, error: str) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(CodexAppServerError(error))

    def _mark_failed(self, error: str) -> None:
        self._terminal_error = error
        self._fail_pending(error)
        self._fail_prepared_turn_contexts()
        for state in self._turns.values():
            if not state.done.is_set():
                if state.failure is None:
                    state.failure = CodexTurnFailure(state.turn_id, reason="transport_failed", detail=error)
                    state.failure.interrupt_failed = True
                state.error = error
                state.done.set()

    def _fail_prepared_turn_contexts(self) -> None:
        for binding in self._prepared_turn_contexts.values():
            binding.failed = True
            binding.acknowledged.set()
        self._prepared_turn_contexts.clear()


class CodexAppServerBackend:
    """SessionBackend implementation backed by Codex app-server threads."""

    name = "codex_app_server"

    @property
    def conversation_scope(self) -> ConversationScope | None:
        if not isinstance(self.dynamic_tools, DynamicToolRegistry):
            return None
        conversation = self.dynamic_tools.conversation
        if conversation is None:
            return None
        return ConversationScope(tenant_id=conversation[0], source_id=conversation[1])

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
        collaboration_mode: CodexCollaborationMode | None = None,
        create_cwd: bool = True,
        request_timeout_s: float = 15.0,
        turn_timeout_s: float = 180.0,
        max_concurrent_dynamic_tool_calls: int = DEFAULT_MAX_CONCURRENT_DYNAMIC_TOOL_CALLS,
        max_json_rpc_frame_bytes: int = DEFAULT_MAX_JSON_RPC_FRAME_BYTES,
        max_turn_output_bytes: int = DEFAULT_MAX_TURN_OUTPUT_BYTES,
        client: CodexJsonRpcClient | None = None,
        client_info: CodexClientInfo | None = None,
    ) -> None:
        self.command = command or ["codex", "app-server", "--listen", "stdio://"]
        self.codex_home = codex_home
        if client_info is not None and not isinstance(client_info, CodexClientInfo):
            raise TypeError("client_info must be CodexClientInfo")
        self.client_info = client_info or CodexClientInfo(name="soveren-agent-platform", version=__version__)
        self.model = model
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.developer_instructions = developer_instructions
        if dynamic_tools is not None and not isinstance(dynamic_tools, DynamicToolRegistry) and client is None:
            raise ValueError("dynamic tool specs without handlers require an explicit custom Codex client")
        self.dynamic_tools = dynamic_tools
        self.output_schema = output_schema
        if collaboration_mode is not None and not isinstance(collaboration_mode, CodexCollaborationMode):
            raise TypeError("collaboration_mode must be a CodexCollaborationMode")
        self.collaboration_mode = collaboration_mode
        self.create_cwd = create_cwd
        self.request_timeout_s = request_timeout_s
        self.turn_timeout_s = turn_timeout_s
        self.max_concurrent_dynamic_tool_calls = _positive_int(
            max_concurrent_dynamic_tool_calls,
            name="max_concurrent_dynamic_tool_calls",
        )
        self.max_json_rpc_frame_bytes = _positive_int(
            max_json_rpc_frame_bytes,
            name="max_json_rpc_frame_bytes",
        )
        self.max_turn_output_bytes = _positive_int(
            max_turn_output_bytes,
            name="max_turn_output_bytes",
        )
        self._client: CodexJsonRpcClient | None = client
        self._initialized = client is not None
        self._owns_client = client is None
        self._loaded_thread_ids: set[str] = set()
        self._thread_scopes: dict[str, ConversationScope | None] = {}
        self._thread_tool_allowlists: dict[str, DynamicToolAllowlist | None] = {}
        self._lifecycle_lock = asyncio.Lock()

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
        ensure_conversation_scope(
            self,
            spec.conversation_scope,
            resource_name="Codex app-server backend",
        )
        if spec.kind not in ("codex", "codex_cli"):
            raise CodexAppServerError(f"Codex app-server cannot open kind={spec.kind!r}")
        if self.create_cwd:
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
        dynamic_tools = self._dynamic_tool_specs(spec.dynamic_tool_allowlist)
        if dynamic_tools:
            params["dynamicTools"] = dynamic_tools
        result = await self._client.request("thread/start", params)
        thread = (result or {}).get("thread") or {}
        thread_id = thread.get("id")
        if not thread_id:
            raise CodexAppServerError("thread/start did not return thread.id")
        self._loaded_thread_ids.add(str(thread_id))
        self._thread_scopes[str(thread_id)] = spec.conversation_scope
        self._thread_tool_allowlists[str(thread_id)] = spec.dynamic_tool_allowlist
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

    async def send(self, backend_session_id: str, prompt: str) -> SendReceipt:
        return await self._send(
            backend_session_id,
            prompt,
            output_schema=self.output_schema,
            execution_context=None,
            dynamic_tool_allowlist=None,
        )

    async def send_with_output_schema(
        self,
        backend_session_id: str,
        prompt: str,
        output_schema: JsonObject,
    ) -> SendReceipt:
        return await self._send(
            backend_session_id,
            prompt,
            output_schema=output_schema,
            execution_context=None,
            dynamic_tool_allowlist=None,
        )

    async def send_with_context(
        self,
        backend_session_id: str,
        prompt: str,
        *,
        execution_context: ToolExecutionContext | None,
        output_schema: JsonObject | None,
        dynamic_tool_allowlist: DynamicToolAllowlist | None,
    ) -> SendReceipt:
        allowlist = validate_dynamic_tool_allowlist(dynamic_tool_allowlist)
        if execution_context is None and allowlist is None:
            raise ValueError("contextual send requires execution context or a tool allowlist")
        if backend_session_id not in self._thread_scopes:
            raise CodexAppServerError(
                "contextual turns require a thread opened with a trusted execution scope"
            )
        thread_scope = self._thread_scopes[backend_session_id]
        if execution_context is not None and thread_scope != execution_context.scope:
            raise TenantBoundaryError(
                "LLM execution context scope does not match the native thread scope"
            )
        if self._thread_tool_allowlists.get(backend_session_id) != allowlist:
            raise CodexAppServerError(
                "contextual turn tool allowlist does not match tools advertised at thread start"
            )
        return await self._send(
            backend_session_id,
            prompt,
            output_schema=output_schema,
            execution_context=execution_context,
            dynamic_tool_allowlist=allowlist,
        )

    async def _send(
        self,
        backend_session_id: str,
        prompt: str,
        *,
        output_schema: dict[str, Any] | None,
        execution_context: ToolExecutionContext | None,
        dynamic_tool_allowlist: DynamicToolAllowlist | None,
    ) -> SendReceipt:
        advertised_allowlist = self._thread_tool_allowlists.get(backend_session_id)
        requested_allowlist = validate_dynamic_tool_allowlist(dynamic_tool_allowlist)
        if advertised_allowlist != requested_allowlist:
            raise UnsupportedContextualSessionError(
                "thread-local tool allowlists require send_with_context"
            )
        binding = None
        contextual_request = execution_context is not None or dynamic_tool_allowlist is not None
        if self._client is None:
            raise CodexAppServerError("Codex app-server client is unavailable")
        prepare = getattr(self._client, "prepare_turn_context", None)
        acknowledge = getattr(self._client, "acknowledge_turn_context", None)
        fail = getattr(self._client, "fail_turn_context", None)
        if not callable(prepare) or not callable(acknowledge) or not callable(fail):
            if contextual_request:
                raise CodexAppServerError(
                    "Codex app-server client does not support trusted turn context"
                )
        else:
            binding = prepare(
                backend_session_id,
                execution_context=execution_context,
                dynamic_tool_allowlist=dynamic_tool_allowlist,
            )
        with measure_codex_runtime("ensure_thread"):
            try:
                await self.ensure_thread(backend_session_id)
            except BaseException:
                if binding is not None:
                    assert self._client is not None
                    self._client.fail_turn_context(backend_session_id)
                raise
        assert self._client is not None
        params: dict[str, Any] = {
            "threadId": backend_session_id,
            "input": [{"type": "text", "text": prompt}],
        }
        if output_schema is not None:
            params["outputSchema"] = output_schema
        if self.collaboration_mode is not None:
            params["collaborationMode"] = self.collaboration_mode.app_server_payload()
        requested_at = time.monotonic()
        fallback_timing_key = uuid.uuid4().hex
        try:
            result = await self._client.request("turn/start", params)
            ack_at = time.monotonic()
            turn = (result or {}).get("turn") or {}
            turn_id = turn.get("id")
            if not turn_id:
                raise CodexAppServerError("turn/start did not return turn.id")
        except BaseException:
            if binding is not None:
                self._client.fail_turn_context(backend_session_id)
            record_codex_turn_timing(fallback_timing_key, CodexTurnTiming(
                unavailable_reason="turn_start_unacknowledged",
            ))
            raise
        state = self._client.set_last_turn(backend_session_id, str(turn_id))
        if binding is not None:
            self._client.acknowledge_turn_context(
                backend_session_id,
                str(turn_id),
                state,
            )
        elif isinstance(state, TurnState):
            state.start_acknowledged = True
        if isinstance(state, TurnState):
            state.start_requested_at = requested_at
            state.start_ack_at = ack_at
            _record_turn_timing(state)
        else:
            # Injected clients need not implement event clocks; ACK is still observed here.
            record_codex_turn_timing(fallback_timing_key, CodexTurnTiming(start_ack_s=ack_at - requested_at,
                request_started_at=requested_at, observed_until=ack_at))
        return SendReceipt(backend_operation_id=str(turn_id))

    async def capture(self, backend_session_id: str) -> CaptureResult:
        state = self._client.last_turn(backend_session_id) if self._client is not None else None
        if state is None:
            record_codex_turn_timing(uuid.uuid4().hex, CodexTurnTiming(unavailable_reason="history_recovery"))
            return await self.capture_thread_history(backend_session_id)
        try:
            try:
                await asyncio.wait_for(state.done.wait(), timeout=self.turn_timeout_s)
            except asyncio.TimeoutError:
                state.timed_out = True
                return CaptureResult(text=state.text, timed_out=True)
            text = state.text
            error = state.error
            try:
                if state.failure is not None:
                    raise state.failure
                if error:
                    raise CodexAppServerError(error)
                return CaptureResult(text=text, timed_out=False)
            finally:
                assert self._client is not None
                self._client.release_turn(backend_session_id, state.turn_id)
        finally:
            _record_turn_timing(state)

    async def capture_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> CaptureResult:
        """Capture the exact turn acknowledged by ``turn/start``."""
        turn_id = receipt.backend_operation_id
        if not turn_id:
            raise CodexAppServerError("Codex delivery receipt does not contain a turn id")
        state = self._client.last_turn(backend_session_id) if self._client is not None else None
        if state is not None and state.turn_id == turn_id:
            try:
                try:
                    await asyncio.wait_for(state.done.wait(), timeout=self.turn_timeout_s)
                except asyncio.TimeoutError:
                    state.timed_out = True
                    return CaptureResult(text=state.text, timed_out=True)
                text = state.text
                error = state.error
                try:
                    if state.failure is not None:
                        raise state.failure
                    if error:
                        raise CodexAppServerError(error)
                    return CaptureResult(text=text, timed_out=False)
                finally:
                    assert self._client is not None
                    self._client.release_turn(backend_session_id, turn_id)
            finally:
                _record_turn_timing(state)
        record_codex_turn_timing(uuid.uuid4().hex, CodexTurnTiming(unavailable_reason="history_recovery"))
        return await self.capture_thread_turn(backend_session_id, turn_id)

    async def close(self, backend_session_id: str) -> None:
        await self.ensure_thread(backend_session_id)
        assert self._client is not None
        await self._client.request("thread/archive", {"threadId": backend_session_id})
        self._client.release_thread(backend_session_id)
        self._loaded_thread_ids.discard(backend_session_id)
        self._thread_scopes.pop(backend_session_id, None)
        self._thread_tool_allowlists.pop(backend_session_id, None)

    async def abort_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> None:
        turn_id = receipt.backend_operation_id
        if not turn_id:
            raise CodexAppServerError("Codex delivery receipt does not contain a turn id")
        await self.ensure_thread(backend_session_id)
        assert self._client is not None
        errors: list[Exception] = []
        try:
            await self._client.request(
                "turn/interrupt",
                {"threadId": backend_session_id, "turnId": turn_id},
            )
        except Exception as exc:
            errors.append(exc)
        try:
            await self._client.request("thread/archive", {"threadId": backend_session_id})
        except Exception as exc:
            errors.append(exc)
        finally:
            self._client.release_thread(backend_session_id)
            self._loaded_thread_ids.discard(backend_session_id)
            self._thread_scopes.pop(backend_session_id, None)
            self._thread_tool_allowlists.pop(backend_session_id, None)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise ExceptionGroup("Codex delivery abort failed", errors)

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            if self._client is not None:
                await self._client.close()
            self._client = None
            self._initialized = False
            self._thread_scopes.clear()
            self._thread_tool_allowlists.clear()
            self._loaded_thread_ids.clear()

    async def ensure_thread(self, thread_id: str) -> None:
        await self.ensure_initialized()
        assert self._client is not None
        if thread_id in self._loaded_thread_ids:
            return
        params: dict[str, Any] = {
            "threadId": thread_id,
            "approvalPolicy": self.approval_policy,
            "sandbox": self.sandbox,
        }
        if self.model:
            params["model"] = self.model
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

    async def capture_thread_turn(self, thread_id: str, turn_id: str) -> CaptureResult:
        await self.ensure_thread(thread_id)
        assert self._client is not None
        turn = await self._find_thread_turn(thread_id, turn_id)
        if turn is None:
            return CaptureResult(text="", timed_out=True)
        status = str(turn.get("status") or "")
        text = extract_thread_text(turn.get("items") or [])
        if status == "inProgress":
            _require_turn_output_within_limit(
                text,
                turn_id=turn_id,
                max_bytes=self.max_turn_output_bytes,
            )
            return CaptureResult(text=text, timed_out=True)
        try:
            _require_turn_output_within_limit(
                text,
                turn_id=turn_id,
                max_bytes=self.max_turn_output_bytes,
            )
            if status == "completed":
                return CaptureResult(text=text, timed_out=False)
            if status == "failed":
                raise codex_turn_failure(turn_id, turn.get("error"))
            if status == "interrupted":
                raise CodexTurnFailure(turn_id, reason="turn_interrupted")
            error = terminal_turn_error(turn)
            if error is not None:
                raise CodexAppServerError(error)
            raise CodexAppServerError(
                f"Codex turn {turn_id} returned non-terminal status {status!r}"
            )
        finally:
            self._client.release_turn(thread_id, turn_id)

    async def _find_thread_turn(self, thread_id: str, turn_id: str) -> dict[str, Any] | None:
        assert self._client is not None
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "threadId": thread_id,
                "limit": TURN_RECOVERY_PAGE_SIZE,
                "sortDirection": "desc",
                "itemsView": "full",
            }
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._client.request("thread/turns/list", params)
            if not isinstance(result, dict):
                raise CodexAppServerError("thread/turns/list returned an invalid response")
            turns = result.get("data")
            if not isinstance(turns, list):
                raise CodexAppServerError("thread/turns/list did not return data")
            for turn in turns:
                if isinstance(turn, dict) and turn.get("id") == turn_id:
                    return turn
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                return None
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise CodexAppServerError("thread/turns/list returned an invalid pagination cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    async def ensure_initialized(self) -> None:
        if self._initialized and not self._client_failed():
            return
        async with self._lifecycle_lock:
            if self._initialized and not self._client_failed():
                return
            if self._client_failed():
                if not self._owns_client:
                    raise CodexAppServerError("injected Codex client failed and cannot be recreated")
                assert self._client is not None
                await self._client.close()
                self._client = None
                self._initialized = False
                self._loaded_thread_ids.clear()
            client = JsonRpcStdioClient(
                command=self.command,
                cwd=None,
                env=self.env(),
                request_timeout_s=self.request_timeout_s,
                max_concurrent_dynamic_tool_calls=self.max_concurrent_dynamic_tool_calls,
                max_json_rpc_frame_bytes=self.max_json_rpc_frame_bytes,
                max_turn_output_bytes=self.max_turn_output_bytes,
                dynamic_tools=(
                    self.dynamic_tools
                    if isinstance(self.dynamic_tools, DynamicToolRegistry)
                    else None
                ),
            )
            try:
                info = {"name": self.client_info.name, "version": self.client_info.version}
                if self.client_info.title is not None:
                    info["title"] = self.client_info.title
                result = await client.request("initialize", {
                    "clientInfo": info,
                    "capabilities": {"experimentalApi": True, "optOutNotificationMethods": []},
                })
                user_agent = str((result or {}).get("userAgent") or "")
                version = parse_codex_version(user_agent)
                if version is not None and version < MIN_APP_SERVER_VERSION:
                    raise CodexAppServerError(
                        f"codex app-server version {version!r} is below required {MIN_APP_SERVER_VERSION!r}"
                    )
            except BaseException:
                await client.close()
                raise
            self._client = client
            self._initialized = True

    def _client_failed(self) -> bool:
        return bool(self._client is not None and getattr(self._client, "failed", False))

    def _dynamic_tool_specs(
        self,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> list[dict[str, Any]]:
        selected = validate_dynamic_tool_allowlist(allowlist)
        if self.dynamic_tools is None:
            if selected:
                raise ValueError("dynamic tool allowlist requested without a dynamic tool registry")
            return []
        if isinstance(self.dynamic_tools, DynamicToolRegistry):
            return self.dynamic_tools.app_server_specs(allowlist=selected)
        specs = normalize_dynamic_tool_specs(self.dynamic_tools)
        if selected is None:
            return specs
        by_selector = {
            (spec.get("namespace"), spec.get("name")): spec
            for spec in specs
        }
        unknown = set(selected) - by_selector.keys()
        if unknown:
            raise ValueError(
                "dynamic tool allowlist contains unknown selectors: "
                f"{sorted(unknown, key=lambda key: (key[0] or '', key[1]))!r}"
            )
        return [by_selector[selector] for selector in selected]


def _positive_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_turn_output_within_limit(text: str, *, turn_id: str, max_bytes: int) -> None:
    if len(text.encode("utf-8")) > max_bytes:
        raise CodexTurnOutputLimitError(f"Codex turn {turn_id} output exceeds {max_bytes} bytes")


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


def terminal_turn_error(turn: dict[str, Any]) -> str | None:
    turn_id = str(turn.get("id") or "unknown")
    status = str(turn.get("status") or "")
    if status == "completed":
        return None
    if status in {"failed", "interrupted"}:
        detail = turn.get("error")
        if isinstance(detail, dict):
            detail = detail.get("message") or detail
        return f"Codex turn {turn_id} {status}: {detail or 'no details'}"
    return f"Codex turn {turn_id} completed notification returned unknown status {status!r}"


def codex_turn_failure(turn_id: str, error: Any) -> CodexTurnFailure:
    """Prefer typed protocol fields; narrowly adapt the lossy 0.143.0 refresh error."""
    if not isinstance(error, dict):
        return CodexTurnFailure(turn_id, reason="turn_failed")
    info = error.get("codexErrorInfo")
    status = None
    if isinstance(info, dict) and len(info) == 1:
        kind, detail = next(iter(info.items()))
        if kind in {"httpConnectionFailed", "responseStreamConnectionFailed",
                    "responseStreamDisconnected", "responseTooManyFailedAttempts"} and isinstance(detail, dict):
            value = detail.get("httpStatusCode")
            if isinstance(value, int) and not isinstance(value, bool):
                status = value
    if info == "unauthorized" or status in {401, 403}:
        return CodexTurnFailure(turn_id, reason="authentication_failed", http_status_code=status)
    # Pinned Codex login/auth/manager.rs formats this Io error after discarding
    # the JSON error code. notify_stream_error forwards it as additionalDetails
    # with a null HTTP status. No prose/region keyword matching is used.
    for detail in (error.get("additionalDetails"), error.get("message")):
        if isinstance(detail, str):
            match = re.fullmatch(r"Failed to refresh token: ([0-9]{3})(?: [A-Za-z ]+)?: .+", detail, re.DOTALL)
            if match is not None:
                status = int(match.group(1))
                reason = "authentication_failed" if status in {401, 403} else "refresh_failed"
                return CodexTurnFailure(turn_id, reason=reason, http_status_code=status)
    message = error.get("message")
    return CodexTurnFailure(
        turn_id, reason="turn_failed", http_status_code=status,
        detail=message if isinstance(message, str) else None,
    )


def parse_codex_version(user_agent: str) -> tuple[int, int, int] | None:
    match = re.search(r"/(\d+)\.(\d+)\.(\d+)", user_agent)
    if not match:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)
