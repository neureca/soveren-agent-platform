import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel, Field, computed_field

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context import SQLitePlannerContextBuilder
from soveren_agent_platform.decisions import (
    ActionDecisionHandler,
    BaseDecision,
    CronDecisionHandler,
    DecisionDispatcher,
    DecisionRegistry,
    DispatchResult,
    OutboundDecisionHandler,
    SQLiteDecisionDispatchStore,
)
from soveren_agent_platform.decisions.sqlite import sqlite_decision_effects
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.runs import SQLiteRunStore
from soveren_agent_platform.runtime import ParsedDecision, PlannerRuntime, PlannerRuntimeConfig
from soveren_agent_platform.runtime.planner import (
    DecisionDispatchInProgressError,
    run_planner_dispatch_turn,
)
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class FakeBackend:
    name = "fake"
    version = "1"

    def __init__(self, text: str) -> None:
        self.text = text
        self.request: LlmRequest | None = None
        self.calls = 0

    async def run(self, request: LlmRequest) -> LlmResponse:
        self.calls += 1
        self.request = request
        return LlmResponse(text=self.text, session_id="llm-session")


class ContextPromptBuilder:
    def build_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return f"{event.payload['text']}\ncontext_source={context.trigger['source_id']}"

    def build_system_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return f"sessions={len(session_metadata['sessions'])}"


class ParsedDecisionParser:
    def parse(self, raw_text: str) -> ParsedDecision:
        data = json.loads(raw_text)
        kind = data.pop("kind")
        return ParsedDecision(kind=kind, payload=data)


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class CreateTaskDecision(BaseDecision):
    kind: Literal["create_task"]
    title: str


class ScheduleDecision(BaseDecision):
    kind: Literal["schedule"]
    run_at: datetime
    text: str


class AliasedReplyDecision(BaseModel):
    kind: Literal["aliased_reply"]
    reply_text: str = Field(alias="text")

    @computed_field
    @property
    def uppercase_text(self) -> str:
        return self.reply_text.upper()


def test_fake_planner_dispatch_pipeline_covers_context_outbound_and_actions(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    registry.register("create_task", CreateTaskDecision)

    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )
    dispatcher.register(
        "create_task",
        ActionDecisionHandler(
            approval_policy="auto",
            idempotency_key=lambda decision, context: f"task:{decision.title}",
        ),
    )

    reply_backend = FakeBackend('{"kind":"reply","text":"done"}')
    reply_result = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=_event("evt_reply", "status?", "chat-1"),
            prompt_builder=ContextPromptBuilder(),
            llm_backend=reply_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    outbound = conn.execute(
        "SELECT * FROM outbound_messages WHERE id = ?",
        (reply_result.dispatch.id,),
    ).fetchone()

    assert reply_result.dispatch.target == "outbound"
    assert outbound["channel"] == "telegram"
    assert outbound["destination_id"] == "chat-1"
    assert outbound["text"] == "done"
    assert reply_backend.request is not None
    assert reply_backend.request.metadata["planner_context"]["trigger"]["source_id"] == "[redacted:source_id]"
    assert "context_source=[redacted:source_id]" in reply_backend.request.prompt

    action_backend = FakeBackend('{"kind":"create_task","title":"Call client"}')
    action_result = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=_event("evt_action", "create task", "chat-1"),
            prompt_builder=ContextPromptBuilder(),
            llm_backend=action_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    action = conn.execute("SELECT * FROM actions WHERE id = ?", (action_result.dispatch.id,)).fetchone()
    execute_event = conn.execute(
        "SELECT * FROM event_queue WHERE recipient = 'actions' AND message_type = 'ExecuteAction'"
    ).fetchone()

    assert action_result.dispatch.target == "action"
    assert action["kind"] == "create_task"
    assert action["status"] == "approved"
    assert execute_event is not None
    assert action_backend.request is not None
    assert action_backend.request.metadata["trigger_event_id"] == "evt_action"


def test_dispatch_retry_reuses_durable_planner_decision_without_recalling_llm(tmp_path):
    class FailsOnceHandler:
        def __init__(self) -> None:
            self.calls = 0

        async def dispatch(self, effects, decision, context) -> DispatchResult:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("dispatch unavailable")
            return DispatchResult(target="test", id="effect-1")

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    handler = FailsOnceHandler()
    dispatcher = DecisionDispatcher()
    dispatcher.register("reply", handler)
    backend = FakeBackend('{"kind":"reply","text":"done"}')
    event = _event("evt_retry", "status?", "chat-1")

    with pytest.raises(RuntimeError, match="dispatch unavailable"):
        asyncio.run(
            run_planner_dispatch_turn(
                conn,
                event=event,
                prompt_builder=ContextPromptBuilder(),
                llm_backend=backend,
                decision_parser=registry,
                dispatcher=dispatcher,
                config=_config(),
            )
        )
    result = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )

    assert result.dispatch.id == "effect-1"
    assert backend.calls == 1
    assert handler.calls == 2
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM agent_runs WHERE tenant_id = ? AND trigger_event_id = ?",
            ("tenant-a", "evt_retry"),
        ).fetchone()[0]
        == 1
    )


def test_changed_prompt_and_model_replay_first_accepted_reply_without_llm(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )
    event = _event("evt_first_decision", "status?", "chat-1")
    first_backend = FakeBackend('{"kind":"reply","text":"answer A"}')

    first = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=first_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(model="model-a", prompt_version="v1"),
        )
    )
    changed_backend = FakeBackend('{"kind":"reply","text":"answer B"}')
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=changed_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(model="model-b", prompt_version="v2"),
        )
    )

    assert changed_backend.calls == 0
    assert replay.planner.decision.text == "answer A"
    assert replay.planner.run_id == first.planner.run_id
    assert replay.dispatch == first.dispatch
    assert conn.execute("SELECT COUNT(*) FROM outbound_messages").fetchone()[0] == 1
    receipt = conn.execute(
        "SELECT model, prompt_version, status FROM decision_dispatches"
    ).fetchone()
    assert tuple(receipt) == ("model-a", "v1", "completed")


def test_receipt_replay_preserves_parsed_decision_payload_shape(tmp_path):
    class ParsedReplyHandler:
        async def dispatch(self, effects, decision, context) -> DispatchResult:
            return DispatchResult(target="test", id=decision.payload["text"])

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    dispatcher = DecisionDispatcher()
    dispatcher.register("reply", ParsedReplyHandler())
    event = _event("evt_parsed_decision", "status?", "chat-1")

    first = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"reply","text":"answer"}'),
            decision_parser=ParsedDecisionParser(),
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"reply","text":"changed"}'),
            decision_parser=ParsedDecisionParser(),
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
        )
    )

    assert first.planner.decision.payload == {"text": "answer"}
    assert replay.planner.decision.payload == first.planner.decision.payload
    assert replay.dispatch == first.dispatch
    assert conn.execute("SELECT decision_json FROM decision_dispatches").fetchone()[0] == (
        '{"text":"answer","kind":"reply"}'
    )


def test_receipt_replay_preserves_pydantic_aliases_and_excludes_computed_fields(tmp_path):
    class AliasedReplyHandler:
        async def dispatch(self, effects, decision, context) -> DispatchResult:
            return DispatchResult(target="test", id=decision.reply_text)

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("aliased_reply", AliasedReplyDecision)
    dispatcher = DecisionDispatcher()
    dispatcher.register("aliased_reply", AliasedReplyHandler())
    event = _event("evt_aliased_decision", "status?", "chat-1")

    first = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"aliased_reply","text":"answer"}'),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"aliased_reply","text":"changed"}'),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
        )
    )

    assert isinstance(first.planner.decision, AliasedReplyDecision)
    assert isinstance(replay.planner.decision, AliasedReplyDecision)
    assert replay.planner.decision.reply_text == "answer"
    assert replay.dispatch == first.dispatch
    stored = json.loads(
        conn.execute("SELECT decision_json FROM decision_dispatches").fetchone()[0]
    )
    assert stored == {"kind": "aliased_reply", "text": "answer"}


def test_port_composed_planner_runtime_uses_configured_decision_receipt_store(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry, dispatcher = _reply_runtime()
    receipt_store = SQLiteDecisionDispatchStore._from_connection(conn)
    planner = PlannerRuntime(
        run_store=SQLiteRunStore._from_connection(conn),
        context_builder=SQLitePlannerContextBuilder._from_connection(conn),
        effects=sqlite_decision_effects(conn),
        decision_dispatch_store=receipt_store,
    )
    event = _event("evt_port_runtime", "status?", "chat-1")
    first_backend = FakeBackend('{"kind":"reply","text":"first"}')

    first = asyncio.run(
        planner.run_dispatch_turn(
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=first_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    changed_backend = FakeBackend('{"kind":"reply","text":"changed"}')
    replay = asyncio.run(
        planner.run_dispatch_turn(
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=changed_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
        )
    )

    assert first.dispatch == replay.dispatch
    assert changed_backend.calls == 0


def test_changed_decision_kind_does_not_add_a_second_effect(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    registry.register("schedule", ScheduleDecision)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )
    dispatcher.register(
        "schedule",
        CronDecisionHandler(
            name="test.schedule",
            run_at=lambda decision, context: int(decision.run_at.timestamp()),
            payload=lambda decision, context: {"text": decision.text},
        ),
    )
    event = _event("evt_kind_change", "do it", "chat-1")

    first = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"reply","text":"answer"}'),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v1"),
        )
    )
    changed_backend = FakeBackend(
        '{"kind":"schedule","run_at":"2033-05-18T03:33:20+00:00","text":"later"}'
    )
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=changed_backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
        )
    )

    assert first.dispatch.target == "outbound"
    assert replay.dispatch.target == "outbound"
    assert changed_backend.calls == 0
    assert conn.execute("SELECT COUNT(*) FROM outbound_messages").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM cron_jobs").fetchone()[0] == 0


def test_first_accepted_schedule_with_datetime_replays_from_receipt(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("schedule", ScheduleDecision)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "schedule",
        CronDecisionHandler(
            name="test.schedule",
            run_at=lambda decision, context: int(decision.run_at.timestamp()),
            payload=lambda decision, context: {"text": decision.text},
        ),
    )
    event = _event("evt_schedule", "remind me", "chat-1")
    backend = FakeBackend(
        '{"kind":"schedule","run_at":"2033-05-18T03:33:20+00:00","text":"later"}'
    )

    first = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
        )
    )
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend(
                '{"kind":"schedule","run_at":"2034-01-01T00:00:00+00:00","text":"changed"}'
            ),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
        )
    )

    assert first.dispatch.target == "cron"
    assert replay.dispatch == first.dispatch
    assert conn.execute("SELECT COUNT(*) FROM cron_jobs").fetchone()[0] == 1
    decision_json = conn.execute(
        "SELECT decision_json FROM decision_dispatches"
    ).fetchone()[0]
    assert '"run_at":"2033-05-18T03:33:20Z"' in decision_json


def test_crash_after_decision_acceptance_replays_saved_decision_without_llm(tmp_path):
    class CrashAfterAcceptStore:
        def __init__(self, inner: SQLiteDecisionDispatchStore) -> None:
            self.inner = inner
            self.crashed = False

        async def claim(self, **kwargs):
            return await self.inner.claim(**kwargs)

        async def accept(self, *args, **kwargs):
            accepted = await self.inner.accept(*args, **kwargs)
            if accepted and not self.crashed:
                self.crashed = True
                raise RuntimeError("crash after accepted receipt")
            return accepted

        async def complete(self, *args, **kwargs):
            return await self.inner.complete(*args, **kwargs)

        async def release(self, *args, **kwargs):
            return True

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    receipt_store = SQLiteDecisionDispatchStore._from_connection(conn)
    crashing_store = CrashAfterAcceptStore(receipt_store)
    registry, dispatcher = _reply_runtime()
    backend = FakeBackend('{"kind":"reply","text":"accepted"}')
    event = _event("evt_accept_crash", "status?", "chat-1")

    with pytest.raises(RuntimeError, match="crash after accepted receipt"):
        asyncio.run(
            run_planner_dispatch_turn(
                conn,
                event=event,
                prompt_builder=ContextPromptBuilder(),
                llm_backend=backend,
                decision_parser=registry,
                dispatcher=dispatcher,
                config=_config(),
                decision_dispatch_store=crashing_store,
            )
        )
    conn.execute("UPDATE decision_dispatches SET lease_until = 0")
    replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
            decision_dispatch_store=receipt_store,
        )
    )

    assert backend.calls == 1
    assert replay.planner.decision.text == "accepted"
    assert conn.execute("SELECT COUNT(*) FROM outbound_messages").fetchone()[0] == 1
    assert conn.execute("SELECT status FROM decision_dispatches").fetchone()[0] == "completed"


def test_crash_after_effect_replays_same_effect_then_completes_receipt(tmp_path):
    class CrashBeforeCompleteStore:
        def __init__(self, inner: SQLiteDecisionDispatchStore) -> None:
            self.inner = inner
            self.crashed = False

        async def claim(self, **kwargs):
            return await self.inner.claim(**kwargs)

        async def accept(self, *args, **kwargs):
            return await self.inner.accept(*args, **kwargs)

        async def complete(self, *args, **kwargs):
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("crash before receipt completion")
            return await self.inner.complete(*args, **kwargs)

        async def release(self, *args, **kwargs):
            return True

    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    receipt_store = SQLiteDecisionDispatchStore._from_connection(conn)
    crashing_store = CrashBeforeCompleteStore(receipt_store)
    registry, dispatcher = _reply_runtime()
    backend = FakeBackend('{"kind":"reply","text":"once"}')
    event = _event("evt_effect_crash", "status?", "chat-1")

    with pytest.raises(RuntimeError, match="crash before receipt completion"):
        asyncio.run(
            run_planner_dispatch_turn(
                conn,
                event=event,
                prompt_builder=ContextPromptBuilder(),
                llm_backend=backend,
                decision_parser=registry,
                dispatcher=dispatcher,
                config=_config(),
                decision_dispatch_store=crashing_store,
            )
        )
    effect_id = conn.execute("SELECT id FROM outbound_messages").fetchone()[0]
    conn.execute("UPDATE decision_dispatches SET lease_until = 0")
    recovered = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=backend,
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(),
            decision_dispatch_store=receipt_store,
        )
    )
    exact_replay = asyncio.run(
        run_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=ContextPromptBuilder(),
            llm_backend=FakeBackend('{"kind":"reply","text":"different"}'),
            decision_parser=registry,
            dispatcher=dispatcher,
            config=_config(prompt_version="v2"),
            decision_dispatch_store=receipt_store,
        )
    )

    assert backend.calls == 1
    assert recovered.dispatch.id == effect_id
    assert recovered.dispatch.created is False
    assert exact_replay.dispatch == recovered.dispatch
    assert conn.execute("SELECT COUNT(*) FROM outbound_messages").fetchone()[0] == 1


def test_concurrent_dispatch_claim_allows_only_one_planner(tmp_path):
    class BlockingBackend(FakeBackend):
        def __init__(self) -> None:
            super().__init__('{"kind":"reply","text":"winner"}')
            self.started = asyncio.Event()
            self.resume = asyncio.Event()

        async def run(self, request: LlmRequest) -> LlmResponse:
            self.calls += 1
            self.request = request
            self.started.set()
            await self.resume.wait()
            return LlmResponse(text=self.text, session_id="llm-session")

    async def run() -> tuple[FakeBackend, FakeBackend]:
        db_path = tmp_path / "app.db"
        first_conn = open_sqlite(db_path)
        apply_platform_migrations(first_conn)
        second_conn = open_sqlite(db_path)
        registry, dispatcher = _reply_runtime()
        event = _event("evt_concurrent", "status?", "chat-1")
        winner = BlockingBackend()
        loser = FakeBackend('{"kind":"reply","text":"loser"}')
        winner_task = asyncio.create_task(
            run_planner_dispatch_turn(
                first_conn,
                event=event,
                prompt_builder=ContextPromptBuilder(),
                llm_backend=winner,
                decision_parser=registry,
                dispatcher=dispatcher,
                config=_config(),
            )
        )
        await winner.started.wait()
        with pytest.raises(DecisionDispatchInProgressError):
            await run_planner_dispatch_turn(
                second_conn,
                event=event,
                prompt_builder=ContextPromptBuilder(),
                llm_backend=loser,
                decision_parser=registry,
                dispatcher=dispatcher,
                config=_config(prompt_version="v2"),
            )
        winner.resume.set()
        await winner_task
        first_conn.close()
        second_conn.close()
        return winner, loser

    winner, loser = asyncio.run(run())

    assert winner.calls == 1
    assert loser.calls == 0


def _event(event_id: str, text: str, source_id: str) -> AgentEvent:
    return AgentEvent(
        id=event_id,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"text": text, "source_id": source_id, "user_id": "user-1"},
    )


def _config(
    *,
    model: str = "fake-model",
    prompt_version: str = "v1",
) -> PlannerRuntimeConfig:
    return PlannerRuntimeConfig(
        model=model,
        prompt_version=prompt_version,
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
    )


def _reply_runtime() -> tuple[DecisionRegistry, DecisionDispatcher]:
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )
    return registry, dispatcher
