"""LLM backend implemented over reusable execution session backends."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field, replace
from typing import Any

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import validate_dynamic_tool_allowlist
from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.llm.checkpoint import (
    LlmCheckpointBinding,
    LlmExecutionCheckpoint,
)
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.runtime.failures import (
    NonRetryableEventError,
    is_non_retryable_event_error,
)
from soveren_agent_platform.runtime.latency import measure_phase
from soveren_agent_platform.sessions.backend import (
    ContextualSessionBackend,
    DeliveryAbortBackend,
    DeliveryCaptureBackend,
    DeliveryPauseResult,
    OpenSpec,
    OutputSchemaSessionBackend,
    ResumableSessionBackend,
    SendReceipt,
    SessionBackend,
    TenantBoundaryError,
    UnsupportedContextualSessionError,
    bound_conversation_scope,
    ensure_conversation_scope,
)

_CHECKPOINT_CLEANUP_TIMEOUT_S = 5.0
_CONTINUATION_PROMPT = (
    "Continue the original unfinished request using the existing conversation history and any "
    "completed tool results. Do not restart the request or repeat completed work. Continue from "
    "where you stopped and provide the final response."
)


class _DeliveryStillPendingError(Exception):
    """A retry restored the exact turn, which is not yet ready to continue."""


@dataclass(slots=True)
class SessionLlmBackend:
    backend: SessionBackend
    kind: str
    name: str = "session_llm"
    version: str = "1"
    title: str = "planner"
    metadata: dict[str, Any] = field(default_factory=dict)
    output_schema: JsonObject | None = None

    @property
    def conversation_scope(self) -> ConversationScope | None:
        return bound_conversation_scope(self.backend)

    async def run(self, request: LlmRequest) -> LlmResponse:
        ensure_conversation_scope(
            self.backend,
            request.conversation_scope,
            resource_name=f"session LLM backend {self.name!r}",
        )
        allowlist = validate_dynamic_tool_allowlist(request.dynamic_tool_allowlist)
        if (
            request.execution_context is not None
            and request.conversation_scope != request.execution_context.scope
        ):
            raise TenantBoundaryError(
                "LLM execution context scope does not match the trusted conversation scope"
            )
        if request.journal is not None:
            output_schema = request.output_schema if request.output_schema is not None else self.output_schema
            journal_capable = (
                isinstance(self.backend, ResumableSessionBackend)
                and isinstance(self.backend, DeliveryCaptureBackend)
                and isinstance(self.backend, DeliveryAbortBackend)
                and (
                    (request.execution_context is None and allowlist is None)
                    or isinstance(self.backend, ContextualSessionBackend)
                )
                and (output_schema is None or isinstance(self.backend, OutputSchemaSessionBackend))
            )
            if not journal_capable:
                checkpoint = await request.journal.load()
                if checkpoint is not None:
                    raise NonRetryableEventError(
                        "session backend cannot safely restore the persisted native execution"
                    )
                # Older SessionBackend implementations continue through their established path,
                # but never when a checkpoint says native work may already exist.
                return await self.run(replace(request, journal=None))
            return await self._run_with_journal(request, allowlist)
        started = time.monotonic()
        opened = None
        try:
            async with asyncio.timeout(request.timeout_s):
                with measure_phase("session_open"):
                    opened = await self.backend.open(
                        OpenSpec(
                            kind=self.kind,
                            cwd=str(request.cwd),
                            title=self.title,
                            metadata={
                                **self.metadata,
                                "model": request.model,
                                "env_home": str(request.env_home),
                            },
                            conversation_scope=request.conversation_scope,
                            dynamic_tool_allowlist=allowlist,
                        )
                    )
                prompt = _framed_prompt(request)
                output_schema = (
                    request.output_schema
                    if request.output_schema is not None
                    else self.output_schema
                )
                with measure_phase("codex_turn"):
                    if request.execution_context is not None or allowlist is not None:
                        if not isinstance(self.backend, ContextualSessionBackend):
                            raise UnsupportedContextualSessionError(
                                "session backend does not support trusted execution context"
                            )
                        await self.backend.send_with_context(
                            opened.backend_session_id,
                            prompt,
                            execution_context=request.execution_context,
                            output_schema=output_schema,
                            dynamic_tool_allowlist=allowlist,
                        )
                    elif output_schema is None:
                        await self.backend.send(opened.backend_session_id, prompt)
                    else:
                        if not isinstance(self.backend, OutputSchemaSessionBackend):
                            raise TypeError(
                                "session LLM backend does not support output schemas"
                            )
                        await self.backend.send_with_output_schema(
                            opened.backend_session_id,
                            prompt,
                            output_schema,
                        )
                    capture = await self.backend.capture(opened.backend_session_id)
            if capture.timed_out:
                raise TimeoutError(f"session backend timed out for {opened.backend_session_id}")
            response = LlmResponse(
                text=capture.text,
                session_id=opened.backend_session_id,
                duration_ms=round((time.monotonic() - started) * 1000),
                metadata={
                    "timed_out": capture.timed_out,
                    "backend_metadata": opened.metadata or {},
                },
            )
        except BaseException as run_error:
            if opened is None:
                raise
            try:
                await self.backend.close(opened.backend_session_id)
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "session LLM request and cleanup failed",
                    [run_error, cleanup_error],
                ) from None
            raise
        else:
            await self.backend.close(opened.backend_session_id)
            return response

    async def _run_with_journal(
        self,
        request: LlmRequest,
        allowlist: tuple[tuple[str | None, str], ...] | None,
    ) -> LlmResponse:
        journal = request.journal
        assert journal is not None
        if not isinstance(self.backend, ResumableSessionBackend):
            raise NonRetryableEventError(
                "session backend does not support trusted history resume and pause"
            )
        if not isinstance(self.backend, DeliveryCaptureBackend):
            raise NonRetryableEventError(
                "session backend cannot capture the exact acknowledged delivery"
            )
        if not isinstance(self.backend, DeliveryAbortBackend):
            raise NonRetryableEventError(
                "session backend cannot abort the exact acknowledged delivery"
            )
        resumable = self.backend
        capture_backend = self.backend
        abort_backend = self.backend

        output_schema = request.output_schema if request.output_schema is not None else self.output_schema
        if request.execution_context is not None or allowlist is not None:
            if not isinstance(self.backend, ContextualSessionBackend):
                raise NonRetryableEventError(
                    "session backend cannot restore the required trusted execution context"
                ) from UnsupportedContextualSessionError(
                    "session backend does not support trusted execution context"
                )
        elif output_schema is not None and not isinstance(self.backend, OutputSchemaSessionBackend):
            raise NonRetryableEventError("session backend does not support the required output schema")

        expected_binding = LlmCheckpointBinding.for_request(
            run_id=journal.run_id,
            backend_kind=self.kind,
            model=request.model,
            cwd=str(request.cwd),
            env_home=str(request.env_home),
            conversation_scope=request.conversation_scope,
            execution_context=request.execution_context,
            dynamic_tool_allowlist=allowlist,
            output_schema=output_schema,
        )
        started = time.monotonic()
        checkpoint: LlmExecutionCheckpoint | None = None
        opened_metadata: dict[str, Any] = {}
        checkpoint_write_failed = False
        session_id: str | None = None

        async def save(value: LlmExecutionCheckpoint) -> None:
            nonlocal checkpoint, checkpoint_write_failed
            try:
                await journal.save(value)
            except asyncio.CancelledError:
                # The receipt/state may have committed before cancellation reached the writer.
                # Retain the attempted checkpoint so cancellation cleanup can still abort its
                # exact acknowledged delivery; a lost lease on the follow-up write remains fenced.
                checkpoint = value
                raise
            except BaseException:
                checkpoint_write_failed = True
                raise
            checkpoint = value

        async def timeout_result(timeout_error: TimeoutError) -> LlmResponse:
            nonlocal checkpoint
            if checkpoint is None:
                raise timeout_error
            if checkpoint.state in {"sending"} and checkpoint.receipt is None:
                raise NonRetryableEventError(
                    "native turn start outcome is uncertain; refusing to resend"
                ) from timeout_error
            if checkpoint.state in {"accepted", "pausing"} and checkpoint.receipt is not None:
                receipt = checkpoint.receipt
                if checkpoint.state != "pausing":
                    pausing = replace(checkpoint, state="pausing")
                    await save(pausing)
                    checkpoint = pausing
                try:
                    async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                        result = await resumable.pause_delivery(
                            checkpoint.session_id or "",
                            receipt,
                        )
                except asyncio.CancelledError:
                    raise
                except BaseException as cleanup_error:
                    if is_non_retryable_event_error(cleanup_error):
                        raise
                    raise BaseExceptionGroup(
                        "LLM request timed out and exact delivery pause failed",
                        [timeout_error, cleanup_error],
                    ) from None
                if not isinstance(result, DeliveryPauseResult):
                    raise TypeError("pause_delivery() must return DeliveryPauseResult")
                if result.status == "completed":
                    completed = replace(checkpoint, state="completed", text=result.text)
                    await save(completed)
                    if completed.session_id is not None:
                        async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                            await self.backend.close(completed.session_id)
                    return _response_for_checkpoint(
                        completed,
                        started=started,
                        opened_metadata=opened_metadata,
                    )
                if result.status == "interrupted":
                    await save(replace(checkpoint, state="paused"))
                # Pending stays pausing so the next attempt only reconciles this receipt.
                raise timeout_error
            # An opened or already-paused thread has no active delivery to interrupt.
            raise timeout_error

        try:
            try:
                async with asyncio.timeout(request.timeout_s):
                    loaded = await journal.load()
                    if loaded is not None and loaded.binding != expected_binding:
                        raise NonRetryableEventError(
                            "persisted LLM checkpoint binding does not match current trusted execution inputs"
                        )
                    checkpoint = loaded
                    if checkpoint is not None:
                        if checkpoint.state == "completed":
                            return _response_for_checkpoint(
                                checkpoint,
                                started=started,
                                opened_metadata=opened_metadata,
                            )
                        if checkpoint.state == "cancelled":
                            raise NonRetryableEventError("LLM execution was explicitly cancelled")
                        if checkpoint.state == "failed":
                            raise NonRetryableEventError("LLM execution previously failed after native work")
                        if checkpoint.state == "sending" and checkpoint.receipt is None:
                            raise NonRetryableEventError(
                                "native turn start outcome is uncertain; refusing to resend"
                            )
                        if checkpoint.session_id is None:
                            raise NonRetryableEventError("LLM checkpoint has no trusted native session id")
                        session_id = checkpoint.session_id
                        try:
                            with measure_phase("session_open"):
                                resumed = await resumable.resume(
                                    _open_spec(self, request, allowlist),
                                    session_id,
                                )
                        except NonRetryableEventError:
                            raise
                        except Exception as exc:
                            raise NonRetryableEventError(
                                "native session history could not be restored; refusing a fresh thread"
                            ) from exc
                        if resumed.backend_session_id != session_id:
                            raise NonRetryableEventError(
                                "native session resume returned a different session id"
                            )
                        opened_metadata = resumed.metadata or {}
                        if checkpoint.state in {"accepted", "pausing"}:
                            receipt = checkpoint.receipt
                            assert receipt is not None
                            if checkpoint.state == "accepted":
                                checkpoint = replace(checkpoint, state="pausing")
                                await save(checkpoint)
                            pause_result = await resumable.pause_delivery(
                                session_id,
                                receipt,
                            )
                            if not isinstance(pause_result, DeliveryPauseResult):
                                raise TypeError("pause_delivery() must return DeliveryPauseResult")
                            if pause_result.status == "completed":
                                completed = replace(
                                    checkpoint,
                                    state="completed",
                                    text=pause_result.text,
                                )
                                await save(completed)
                                await self.backend.close(session_id)
                                return _response_for_checkpoint(
                                    completed,
                                    started=started,
                                    opened_metadata=opened_metadata,
                                )
                            if pause_result.status == "pending":
                                raise _DeliveryStillPendingError
                            checkpoint = replace(checkpoint, state="paused")
                            await save(checkpoint)
                        if checkpoint.state not in {"opened", "paused"}:
                            raise NonRetryableEventError(
                                f"unsupported LLM checkpoint recovery state: {checkpoint.state}"
                            )
                    else:
                        with measure_phase("session_open"):
                            opened = await self.backend.open(_open_spec(self, request, allowlist))
                        session_id = opened.backend_session_id
                        opened_metadata = opened.metadata or {}
                        checkpoint = LlmExecutionCheckpoint(
                            binding=expected_binding,
                            state="opened",
                            session_id=session_id,
                        )
                        await save(checkpoint)

                    assert checkpoint is not None and session_id is not None
                    is_continuation = checkpoint.state == "paused"
                    turn_index = checkpoint.turn_index + (1 if is_continuation else 0)
                    sending = LlmExecutionCheckpoint(
                        binding=expected_binding,
                        state="sending",
                        session_id=session_id,
                        turn_index=turn_index,
                    )
                    # This write must complete before any native turn-start request.
                    await save(sending)
                    prompt = _CONTINUATION_PROMPT if is_continuation else _framed_prompt(request)
                    with measure_phase("codex_turn"):
                        try:
                            receipt = await _send(
                                self.backend,
                                session_id,
                                prompt,
                                request=request,
                                output_schema=output_schema,
                                allowlist=allowlist,
                            )
                        except (asyncio.CancelledError, NonRetryableEventError):
                            raise
                        except Exception as exc:
                            raise NonRetryableEventError(
                                "native turn start outcome is uncertain; refusing to resend"
                            ) from exc
                        if receipt is None:
                            raise NonRetryableEventError(
                                "native turn start returned no receipt; refusing automatic resend"
                            )
                        accepted = replace(sending, state="accepted", receipt=receipt)
                        # Persist ACK before exact-turn capture; a failed write remains uncertain.
                        await save(accepted)
                        capture = await capture_backend.capture_delivery(session_id, receipt)
                        if capture.timed_out:
                            raise TimeoutError(f"session backend timed out for {session_id}")
                        completed = replace(accepted, state="completed", text=capture.text)
                        await save(completed)
                        response = _response_for_checkpoint(
                            completed,
                            started=started,
                            opened_metadata=opened_metadata,
                        )
                if checkpoint is not None and checkpoint.state == "completed":
                    await self.backend.close(session_id or checkpoint.session_id or "")
                return response
            except _DeliveryStillPendingError as pending_error:
                raise TimeoutError("acknowledged native delivery is still pending") from pending_error
            except TimeoutError as timeout_error:
                if checkpoint_write_failed:
                    raise
                return await timeout_result(timeout_error)
        except asyncio.CancelledError as cancel_error:
            if checkpoint_write_failed:
                raise
            if checkpoint is not None and checkpoint.state != "completed":
                cancelled = replace(checkpoint, state="cancelled")
                try:
                    async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                        await save(cancelled)
                except BaseException as persist_error:
                    raise BaseExceptionGroup(
                        "LLM request cancelled and terminal checkpoint could not be saved",
                        [cancel_error, persist_error],
                    ) from None
                if cancelled.receipt is not None and cancelled.session_id is not None:
                    try:
                        async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                            await abort_backend.abort_delivery(
                                cancelled.session_id,
                                cancelled.receipt,
                            )
                    except BaseException as abort_error:
                        raise BaseExceptionGroup(
                            "LLM request cancelled and exact delivery abort failed",
                            [cancel_error, abort_error],
                        ) from None
                elif cancelled.session_id is not None:
                    try:
                        async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                            await self.backend.close(cancelled.session_id)
                    except BaseException as close_error:
                        raise BaseExceptionGroup(
                            "LLM request cancelled and unused session close failed",
                            [cancel_error, close_error],
                        ) from None
            raise
        except BaseException as run_error:
            if checkpoint_write_failed:
                raise
            if (
                checkpoint is not None
                and checkpoint.state in {"accepted", "pausing", "paused"}
                and is_non_retryable_event_error(run_error)
            ):
                failed = replace(checkpoint, state="failed")
                try:
                    await save(failed)
                except BaseException as persist_error:
                    raise BaseExceptionGroup(
                        "LLM request failed and terminal checkpoint could not be saved",
                        [run_error, persist_error],
                    ) from None
                if failed.session_id is not None:
                    try:
                        async with asyncio.timeout(_CHECKPOINT_CLEANUP_TIMEOUT_S):
                            await self.backend.close(failed.session_id)
                    except BaseException as cleanup_error:
                        raise BaseExceptionGroup(
                            "LLM request and terminal session cleanup failed",
                            [run_error, cleanup_error],
                        ) from None
            raise


def _open_spec(
    backend: SessionLlmBackend,
    request: LlmRequest,
    allowlist: tuple[tuple[str | None, str], ...] | None,
) -> OpenSpec:
    return OpenSpec(
        kind=backend.kind,
        cwd=str(request.cwd),
        title=backend.title,
        metadata={
            **backend.metadata,
            "model": request.model,
            "env_home": str(request.env_home),
        },
        conversation_scope=request.conversation_scope,
        dynamic_tool_allowlist=allowlist,
    )


async def _send(
    backend: SessionBackend,
    session_id: str,
    prompt: str,
    *,
    request: LlmRequest,
    output_schema: JsonObject | None,
    allowlist: tuple[tuple[str | None, str], ...] | None,
) -> SendReceipt | None:
    if request.execution_context is not None or allowlist is not None:
        assert isinstance(backend, ContextualSessionBackend)
        return await backend.send_with_context(
            session_id,
            prompt,
            execution_context=request.execution_context,
            output_schema=output_schema,
            dynamic_tool_allowlist=allowlist,
        )
    if output_schema is None:
        return await backend.send(session_id, prompt)
    assert isinstance(backend, OutputSchemaSessionBackend)
    return await backend.send_with_output_schema(session_id, prompt, output_schema)


def _response_for_checkpoint(
    checkpoint: LlmExecutionCheckpoint,
    *,
    started: float,
    opened_metadata: dict[str, Any],
) -> LlmResponse:
    assert checkpoint.state == "completed" and checkpoint.text is not None
    assert checkpoint.session_id is not None
    return LlmResponse(
        text=checkpoint.text,
        session_id=checkpoint.session_id,
        duration_ms=round((time.monotonic() - started) * 1000),
        metadata={
            "timed_out": False,
            "backend_metadata": opened_metadata,
            "replayed_checkpoint": True,
            "turn_index": checkpoint.turn_index,
        },
    )




def _framed_prompt(request: LlmRequest) -> str:
    return (
        f"{request.system_prompt.rstrip()}\n\n"
        "--- USER REQUEST ---\n"
        f"{request.prompt.rstrip()}\n"
        "--- END USER REQUEST ---\n"
    )
