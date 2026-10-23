import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import ToolExecutionContext
from soveren_agent_platform.llm.backends.session import SessionLlmBackend
from soveren_agent_platform.llm.checkpoint import (
    LlmCheckpointBinding,
    LlmExecutionCheckpoint,
)
from soveren_agent_platform.llm.contracts import LlmRequest
from soveren_agent_platform.runtime.failures import NonRetryableEventError
from soveren_agent_platform.sessions.backend import (
    CaptureResult,
    DeliveryPauseResult,
    OpenResult,
    OpenSpec,
    SendReceipt,
)


@dataclass
class MemoryJournal:
    run_id: str
    checkpoint: LlmExecutionCheckpoint | None = None

    async def load(self) -> LlmExecutionCheckpoint | None:
        return self.checkpoint

    async def save(self, checkpoint: LlmExecutionCheckpoint) -> None:
        self.checkpoint = checkpoint


class NativeWorld:
    def __init__(self) -> None:
        self.opened: list[OpenSpec] = []
        self.resumed: list[tuple[OpenSpec, str]] = []
        self.prompts: list[tuple[str, str, ToolExecutionContext | None, tuple | None]] = []
        self.aborted: list[tuple[str, SendReceipt]] = []
        self.close_ids: list[str] = []
        self.capture_count = 0
        self.pause_result = DeliveryPauseResult("interrupted")
        self.block_resume = False
        self.resume_started = asyncio.Event()
        self.raise_after_start = False


class FakeResumableBackend:
    name = "fake-session"

    def __init__(self, world: NativeWorld) -> None:
        self.world = world

    async def open(self, spec: OpenSpec) -> OpenResult:
        self.world.opened.append(spec)
        return OpenResult("thread-1", metadata={"restored": False})

    async def resume(self, spec: OpenSpec, backend_session_id: str) -> OpenResult:
        self.world.resumed.append((spec, backend_session_id))
        self.world.resume_started.set()
        if self.world.block_resume:
            await asyncio.Event().wait()
        return OpenResult(backend_session_id, metadata={"restored": True})

    async def send(self, backend_session_id: str, prompt: str) -> SendReceipt:
        self.world.prompts.append((backend_session_id, prompt, None, None))
        if self.world.raise_after_start:
            raise TimeoutError("lost acknowledgement after native turn start")
        return SendReceipt("turn-1")

    async def send_with_context(
        self,
        backend_session_id: str,
        prompt: str,
        *,
        execution_context: ToolExecutionContext | None,
        output_schema,
        dynamic_tool_allowlist,
    ) -> SendReceipt:
        self.world.prompts.append(
            (backend_session_id, prompt, execution_context, dynamic_tool_allowlist)
        )
        if self.world.raise_after_start:
            raise TimeoutError("lost acknowledgement after native turn start")
        return SendReceipt(f"turn-{len(self.world.prompts)}")

    async def capture(self, backend_session_id: str) -> CaptureResult:
        raise AssertionError("journaled execution must capture the exact receipt")

    async def capture_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> CaptureResult:
        self.world.capture_count += 1
        if self.world.capture_count == 1:
            # Model a useful completed dynamic tool result before the turn stalls.
            await asyncio.Event().wait()
        return CaptureResult('{"kind":"reply","text":"continued"}', timed_out=False)

    async def pause_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> DeliveryPauseResult:
        return self.world.pause_result

    async def abort_delivery(
        self,
        backend_session_id: str,
        receipt: SendReceipt,
    ) -> None:
        self.world.aborted.append((backend_session_id, receipt))

    async def close(self, backend_session_id: str) -> None:
        self.world.close_ids.append(backend_session_id)


def _journal() -> tuple[MemoryJournal, ToolExecutionContext]:
    scope = ConversationScope(tenant_id="tenant-a", source_id="chat-1")
    context = ToolExecutionContext(
        scope=scope,
        execution_id="execution-1",
        task_id="task-1",
    )
    return (
        MemoryJournal("run-1"),
        context,
    )


def _request(
    journal: MemoryJournal,
    context: ToolExecutionContext,
    *,
    prompt: str = "original user request",
    timeout_s: float = 0.02,
) -> LlmRequest:
    return LlmRequest(
        prompt=prompt,
        system_prompt="system",
        cwd=Path("/work"),
        env_home=Path("/home/agent"),
        model="model-1",
        timeout_s=timeout_s,
        conversation_scope=context.scope,
        execution_context=context,
        dynamic_tool_allowlist=(("pulsy", "read_task"),),
        journal=journal,
    )


def test_timeout_pauses_then_new_backend_continues_same_thread_and_history():
    async def run() -> None:
        journal, context = _journal()
        world = NativeWorld()
        first_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")

        with pytest.raises(TimeoutError):
            await first_backend.run(_request(journal, context))

        assert journal.checkpoint is not None
        assert journal.checkpoint.state == "paused"
        assert journal.checkpoint.session_id == "thread-1"
        assert journal.checkpoint.receipt == SendReceipt("turn-1")
        assert len(world.opened) == 1
        assert world.prompts[0][1].startswith("system\n\n--- USER REQUEST ---")

        second_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")
        response = await second_backend.run(
            _request(journal, context, prompt="refreshed rendered context grew")
        )

        assert response.text == '{"kind":"reply","text":"continued"}'
        assert [session_id for _, session_id in world.resumed] == ["thread-1"]
        assert len(world.opened) == 1
        assert world.prompts[1] == (
            "thread-1",
            "Continue the original unfinished request using the existing conversation history and any "
            "completed tool results. Do not restart the request or repeat completed work. Continue from "
            "where you stopped and provide the final response.",
            context,
            (("pulsy", "read_task"),),
        )
        assert journal.checkpoint is not None
        assert journal.checkpoint.state == "completed"
        assert journal.checkpoint.turn_index == 1

    asyncio.run(run())


def test_loaded_checkpoint_binding_mismatch_fails_before_native_resume():
    async def run() -> None:
        journal, context = _journal()
        world = NativeWorld()
        first_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")
        with pytest.raises(TimeoutError):
            await first_backend.run(_request(journal, context))
        assert journal.checkpoint is not None and journal.checkpoint.state == "paused"

        request = _request(journal, context, timeout_s=1)
        request.model = "different-model"
        with pytest.raises(NonRetryableEventError, match="binding does not match"):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli").run(request)
        assert world.resumed == []
        assert len(world.opened) == 1
        assert len(world.prompts) == 1

    asyncio.run(run())


def test_timeout_race_completed_result_is_saved_and_replayed_without_new_turn():
    async def run() -> None:
        journal, context = _journal()
        world = NativeWorld()
        world.pause_result = DeliveryPauseResult(
            "completed",
            '{"kind":"reply","text":"finished during pause"}',
        )
        first_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")

        response = await first_backend.run(_request(journal, context))

        assert response.text == '{"kind":"reply","text":"finished during pause"}'
        assert journal.checkpoint is not None
        assert journal.checkpoint.state == "completed"
        assert world.close_ids == ["thread-1"]
        second_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")
        replay = await second_backend.run(_request(journal, context, timeout_s=1))
        assert replay.text == response.text
        assert replay.metadata is not None and replay.metadata["replayed_checkpoint"] is True
        assert len(world.prompts) == 1
        assert len(world.opened) == 1
        assert world.resumed == []

    asyncio.run(run())


def test_uncertain_native_start_ack_is_non_retryable_and_never_resends():
    async def run() -> None:
        journal, context = _journal()
        world = NativeWorld()
        world.raise_after_start = True
        backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")

        with pytest.raises(NonRetryableEventError, match="outcome is uncertain"):
            await backend.run(_request(journal, context, timeout_s=1))

        assert journal.checkpoint is not None
        assert journal.checkpoint.state == "sending"
        assert journal.checkpoint.receipt is None
        world.raise_after_start = False
        with pytest.raises(NonRetryableEventError, match="outcome is uncertain"):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli").run(
                _request(journal, context, timeout_s=1)
            )
        assert len(world.prompts) == 1
        assert len(world.opened) == 1
        assert world.resumed == []

    asyncio.run(run())


def test_cancellation_while_restoring_paused_thread_is_terminal_and_aborts_exact_receipt():
    async def run() -> None:
        journal, context = _journal()
        world = NativeWorld()
        first_backend = SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli")
        with pytest.raises(TimeoutError):
            await first_backend.run(_request(journal, context))
        assert journal.checkpoint is not None and journal.checkpoint.state == "paused"
        receipt = journal.checkpoint.receipt
        assert receipt is not None

        world.block_resume = True
        task = asyncio.create_task(
            SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli").run(
                _request(journal, context, timeout_s=10)
            )
        )
        await world.resume_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert journal.checkpoint is not None
        assert journal.checkpoint.state == "cancelled"
        assert world.aborted == [("thread-1", receipt)]
        assert len(world.prompts) == 1

        world.block_resume = False
        with pytest.raises(NonRetryableEventError, match="explicitly cancelled"):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex_cli").run(
                _request(journal, context, timeout_s=1)
            )
        assert len(world.prompts) == 1

    asyncio.run(run())


def test_legacy_session_backend_without_checkpoint_support_keeps_legacy_path():
    class LegacySessionBackend:
        def __init__(self) -> None:
            self.opened = 0
            self.sent = 0

        async def open(self, spec):
            self.opened += 1
            return OpenResult("legacy-thread")

        async def send(self, backend_session_id, prompt):
            self.sent += 1
            return None

        async def capture(self, backend_session_id):
            return CaptureResult("legacy result", False)

        async def close(self, backend_session_id):
            return None

    async def run() -> None:
        journal, context = _journal()
        legacy = LegacySessionBackend()
        request = _request(journal, context, timeout_s=1)
        request.execution_context = None
        request.dynamic_tool_allowlist = None
        response = await SessionLlmBackend(legacy, kind="codex_cli").run(
            request
        )
        assert response.text == "legacy result"
        assert legacy.opened == 1
        assert legacy.sent == 1
        assert journal.checkpoint is None

        scope = context.scope
        journal.checkpoint = LlmExecutionCheckpoint(
            binding=LlmCheckpointBinding.for_request(
                run_id=journal.run_id,
                backend_kind="codex_cli",
                model="model-1",
                cwd="/work",
                env_home="/home/agent",
                conversation_scope=scope,
                execution_context=context,
                dynamic_tool_allowlist=(("pulsy", "read_task"),),
                output_schema=None,
            ),
            state="paused",
            session_id="existing-thread",
            receipt=SendReceipt("existing-turn"),
        )
        with pytest.raises(NonRetryableEventError, match="cannot safely restore"):
            await SessionLlmBackend(legacy, kind="codex_cli").run(
                _request(journal, context, timeout_s=1)
            )
        assert legacy.opened == 1

    asyncio.run(run())
