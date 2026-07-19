import asyncio
import json
from pathlib import Path
from typing import Literal

import pytest

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context import PlannerContext
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.decisions import BaseDecision, DecisionRegistry
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.runs import PlannerRunClaim
from soveren_agent_platform.runtime.planner import (
    PlannerRuntimeConfig,
    run_planner_turn,
)
from soveren_agent_platform.sessions.routing import (
    RouteHint,
    SessionRouteRequest,
    SessionRouteResult,
    SessionSnapshot,
)
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class FakeBackend:
    name = "fake"
    version = "1"

    def __init__(self) -> None:
        self.request: LlmRequest | None = None

    async def run(self, request: LlmRequest) -> LlmResponse:
        self.request = request
        return LlmResponse(text='{"kind":"reply","text":"ok"}', session_id="llm-session")


class FakePromptBuilder:
    def build_prompt(self, *, event: AgentEvent, session_metadata: dict) -> str:
        return f"user: {event.payload['text']}\nsessions: {len(session_metadata['sessions'])}"

    def build_system_prompt(self, *, event: AgentEvent, session_metadata: dict) -> str:
        return "system"


class ContextPromptBuilder:
    def __init__(self) -> None:
        self.context_source_id: str | None = None

    def build_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        self.context_source_id = context.trigger["source_id"]
        return f"source: {context.trigger['source_id']}"

    def build_system_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return f"context keys: {','.join(context.to_dict().keys())}"


class EchoModelBoundaryPromptBuilder:
    def build_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return json.dumps(
            {
                "event_payload": event.payload,
                "session_metadata": session_metadata,
                "context": context.to_dict(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def build_system_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return "system"


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class FakeRouter:
    def __init__(self, session_id: str = "cli_1") -> None:
        self.request: SessionRouteRequest | None = None
        self.session_id = session_id

    async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
        self.request = request
        return SessionRouteResult(
            snapshots=[
                SessionSnapshot(
                    session_id=self.session_id,
                    kind="codex_cli",
                    backend="codex",
                    status="idle",
                    title="soveren-agent-platform",
                    keywords=["batching", "runtime"],
                )
            ],
            hint=RouteHint(
                action="route_existing",
                confidence=0.9,
                session_id=self.session_id,
                reasons=["metadata match"],
            ),
        )


class FakeRunStore:
    def __init__(self) -> None:
        self.finalized: list[tuple[str, str, dict]] = []
        self.claims = 0

    async def claim(
        self,
        *,
        tenant_id,
        source_id,
        trigger_event_id,
        model,
        prompt_version,
        input_summary,
        input_fingerprint,
        stale_after_s,
    ):
        self.claims += 1
        return PlannerRunClaim(
            id="run_fake",
            status="running",
            acquired=True,
            lease_token="run-lease",
            output=None,
        )

    async def finalize(self, run_id: str, *, lease_token: str, status: str, output):
        self.finalized.append((run_id, status, output))
        return True


class FakeContextBuilder:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def build(self, *, event: AgentEvent, route_result: SessionRouteResult) -> PlannerContext:
        self.events.append(event.id)
        return PlannerContext(
            trigger={"event_id": event.id, "source_id": event.payload["source_id"]},
            session_routing={
                "route_hint": {"action": route_result.hint.action},
                "sessions": [],
            },
        )


@pytest.mark.parametrize("source_id", [None, " ", 123, True])
def test_planner_rejects_invalid_source_id_before_claiming_run(source_id):
    run_store = FakeRunStore()
    payload = {"text": "hello"}
    if source_id is not None:
        payload["source_id"] = source_id

    with pytest.raises(ValueError, match="non-empty string source_id"):
        asyncio.run(
            run_planner_turn(
                None,
                event=AgentEvent(
                    id="evt_invalid_source",
                    tenant_id="tenant-a",
                    recipient="agent",
                    message_type="ChatBatchReady",
                    payload=payload,
                ),
                prompt_builder=FakePromptBuilder(),
                llm_backend=FakeBackend(),
                decision_parser=DecisionRegistry(),
                run_store=run_store,
                context_builder=FakeContextBuilder(),
                config=PlannerRuntimeConfig(
                    model="fake-model",
                    prompt_version="v1",
                    cwd=Path("/tmp/work"),
                    env_home=Path("/tmp/home"),
                ),
            )
        )

    assert run_store.claims == 0


def test_planner_turn_includes_session_metadata_in_llm_request(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex",
        backend_session_id="thread-1",
        title="soveren-agent-platform",
    )
    backend = FakeBackend()
    router = FakeRouter(session_id)

    decision_registry = DecisionRegistry()
    decision_registry.register("reply", ReplyDecision)

    result = asyncio.run(
        run_planner_turn(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="ChatBatchReady",
                payload={"text": "продолжи в сессии soveren-agent-platform", "source_id": "chat-1"},
            ),
            prompt_builder=FakePromptBuilder(),
            llm_backend=backend,
            decision_parser=decision_registry,
            session_router=router,
            config=PlannerRuntimeConfig(
                model="fake-model",
                prompt_version="v1",
                cwd=Path("/tmp/work"),
                env_home=Path("/tmp/home"),
            ),
        )
    )

    assert result.decision.kind == "reply"
    assert result.decision.text == "ok"
    assert result.session_metadata["route_hint"]["session_id"] == session_id
    assert result.context.session_routing["route_hint"]["session_id"] == session_id
    assert backend.request is not None
    assert backend.request.conversation_scope == ConversationScope(
        tenant_id="tenant-a",
        source_id="chat-1",
    )
    assert backend.request.metadata is not None
    assert backend.request.metadata["session_routing"]["sessions"][0]["session_id"] == session_id
    assert backend.request.metadata["planner_context"]["trigger"]["source_id"] == "[redacted:source_id]"
    assert result.context.trigger["source_id"] == "chat-1"
    assert router.request is not None
    assert router.request.source_id == "chat-1"


def test_planner_turn_passes_rich_context_to_context_aware_prompt_builder(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    backend = FakeBackend()
    prompt_builder = ContextPromptBuilder()

    decision_registry = DecisionRegistry()
    decision_registry.register("reply", ReplyDecision)

    asyncio.run(
        run_planner_turn(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="TelegramMessageReceived",
                payload={"text": "hello", "source_id": "chat-1"},
            ),
            prompt_builder=prompt_builder,
            llm_backend=backend,
            decision_parser=decision_registry,
            config=PlannerRuntimeConfig(
                model="fake-model",
                prompt_version="v1",
                cwd=Path("/tmp/work"),
                env_home=Path("/tmp/home"),
            ),
        )
    )

    assert prompt_builder.context_source_id == "[redacted:source_id]"
    assert backend.request is not None
    assert backend.request.prompt == "source: [redacted:source_id]"
    assert "context keys:" in backend.request.system_prompt


def test_planner_turn_uses_run_store_port(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    backend = FakeBackend()
    run_store = FakeRunStore()

    decision_registry = DecisionRegistry()
    decision_registry.register("reply", ReplyDecision)

    result = asyncio.run(
        run_planner_turn(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="TelegramMessageReceived",
                payload={"text": "hello", "source_id": "chat-1"},
            ),
            prompt_builder=FakePromptBuilder(),
            llm_backend=backend,
            decision_parser=decision_registry,
            config=PlannerRuntimeConfig(
                model="fake-model",
                prompt_version="v1",
                cwd=Path("/tmp/work"),
                env_home=Path("/tmp/home"),
            ),
            run_store=run_store,
        )
    )

    assert result.run_id == "run_fake"
    assert run_store.finalized[0][0] == "run_fake"
    assert run_store.finalized[0][1] == "completed"


def test_planner_rejects_changed_event_even_when_input_summary_matches(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    config = PlannerRuntimeConfig(
        model="fake-model",
        prompt_version="v1",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
    )
    common = {
        "id": "evt_same",
        "tenant_id": "tenant-a",
        "recipient": "agent",
        "message_type": "TelegramMessageReceived",
    }
    first_event = AgentEvent(
        **common,
        payload={"text": "x" * 500 + " transfer", "source_id": "chat-1"},
    )
    changed_event = AgentEvent(
        **common,
        payload={"text": "x" * 500 + " do not transfer", "source_id": "chat-1"},
    )
    first_backend = FakeBackend()
    asyncio.run(
        run_planner_turn(
            conn,
            event=first_event,
            prompt_builder=FakePromptBuilder(),
            llm_backend=first_backend,
            decision_parser=registry,
            config=config,
            session_router=FakeRouter(),
            context_builder=FakeContextBuilder(),
        )
    )
    changed_backend = FakeBackend()

    with pytest.raises(IdempotencyConflictError, match="planner run idempotency key"):
        asyncio.run(
            run_planner_turn(
                conn,
                event=changed_event,
                prompt_builder=FakePromptBuilder(),
                llm_backend=changed_backend,
                decision_parser=registry,
                config=config,
                session_router=FakeRouter(),
                context_builder=FakeContextBuilder(),
            )
        )

    assert changed_backend.request is None


def test_planner_turn_persists_failed_run_before_propagating_cancellation():
    class BlockingBackend(FakeBackend):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()

        async def run(self, request: LlmRequest) -> LlmResponse:
            self.started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    async def run() -> FakeRunStore:
        backend = BlockingBackend()
        run_store = FakeRunStore()
        registry = DecisionRegistry()
        registry.register("reply", ReplyDecision)
        task = asyncio.create_task(
            run_planner_turn(
                None,
                event=AgentEvent(
                    id="evt_1",
                    tenant_id="tenant-a",
                    recipient="agent",
                    message_type="TelegramMessageReceived",
                    payload={"text": "hello", "source_id": "chat-1"},
                ),
                prompt_builder=FakePromptBuilder(),
                llm_backend=backend,
                decision_parser=registry,
                config=PlannerRuntimeConfig(
                    model="fake-model",
                    prompt_version="v1",
                    cwd=Path("/tmp/work"),
                    env_home=Path("/tmp/home"),
                ),
                run_store=run_store,
                context_builder=FakeContextBuilder(),
            )
        )
        await backend.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return run_store

    run_store = asyncio.run(run())

    assert run_store.finalized[0][1] == "failed"
    assert run_store.finalized[0][2]["error_type"] == "CancelledError"


def test_planner_turn_persists_failed_run_when_router_fails():
    class FailingRouter:
        async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
            raise RuntimeError("router unavailable")

    run_store = FakeRunStore()
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(RuntimeError, match="router unavailable"):
        asyncio.run(
            run_planner_turn(
                None,
                event=AgentEvent(
                    id="evt_1",
                    tenant_id="tenant-a",
                    recipient="agent",
                    message_type="TelegramMessageReceived",
                    payload={"text": "hello", "source_id": "chat-1"},
                ),
                prompt_builder=FakePromptBuilder(),
                llm_backend=FakeBackend(),
                decision_parser=registry,
                config=PlannerRuntimeConfig(
                    model="fake-model",
                    prompt_version="v1",
                    cwd=Path("/tmp/work"),
                    env_home=Path("/tmp/home"),
                ),
                session_router=FailingRouter(),
                run_store=run_store,
                context_builder=FakeContextBuilder(),
            )
        )

    assert len(run_store.finalized) == 1
    assert run_store.finalized[0][1] == "failed"
    assert run_store.finalized[0][2]["error_type"] == "RuntimeError"
    assert run_store.finalized[0][2]["session_routing"] == {}
    assert run_store.finalized[0][2]["planner_context"] is None


def test_planner_turn_persists_nested_failure_details():
    class FailingBackend(FakeBackend):
        async def run(self, request: LlmRequest) -> LlmResponse:
            raise ExceptionGroup(
                "session LLM request and cleanup failed",
                [ValueError("capture failed"), RuntimeError("close failed")],
            )

    run_store = FakeRunStore()
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(ExceptionGroup, match="session LLM request and cleanup failed"):
        asyncio.run(
            run_planner_turn(
                None,
                event=AgentEvent(
                    id="evt_1",
                    tenant_id="tenant-a",
                    recipient="agent",
                    message_type="TelegramMessageReceived",
                    payload={"text": "hello", "source_id": "chat-1"},
                ),
                prompt_builder=FakePromptBuilder(),
                llm_backend=FailingBackend(),
                decision_parser=registry,
                config=PlannerRuntimeConfig(
                    model="fake-model",
                    prompt_version="v1",
                    cwd=Path("/tmp/work"),
                    env_home=Path("/tmp/home"),
                ),
                run_store=run_store,
                context_builder=FakeContextBuilder(),
            )
        )

    failure = run_store.finalized[0][2]
    assert failure["error_type"] == "ExceptionGroup"
    assert failure["errors"] == [
        {"error_type": "ValueError", "error": "capture failed"},
        {"error_type": "RuntimeError", "error": "close failed"},
    ]


def test_planner_turn_persists_failed_run_when_context_builder_fails():
    class FailingContextBuilder:
        async def build(
            self,
            *,
            event: AgentEvent,
            route_result: SessionRouteResult,
        ) -> PlannerContext:
            raise ValueError("context unavailable")

    run_store = FakeRunStore()
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(ValueError, match="context unavailable"):
        asyncio.run(
            run_planner_turn(
                None,
                event=AgentEvent(
                    id="evt_1",
                    tenant_id="tenant-a",
                    recipient="agent",
                    message_type="TelegramMessageReceived",
                    payload={"text": "hello", "source_id": "chat-1"},
                ),
                prompt_builder=FakePromptBuilder(),
                llm_backend=FakeBackend(),
                decision_parser=registry,
                config=PlannerRuntimeConfig(
                    model="fake-model",
                    prompt_version="v1",
                    cwd=Path("/tmp/work"),
                    env_home=Path("/tmp/home"),
                ),
                session_router=FakeRouter(),
                run_store=run_store,
                context_builder=FailingContextBuilder(),
            )
        )

    assert len(run_store.finalized) == 1
    assert run_store.finalized[0][1] == "failed"
    assert run_store.finalized[0][2]["error_type"] == "ValueError"
    assert run_store.finalized[0][2]["session_routing"] == {}
    assert run_store.finalized[0][2]["planner_context"] is None


def test_planner_turn_can_run_without_sqlite_when_ports_are_provided():
    backend = FakeBackend()
    run_store = FakeRunStore()
    context_builder = FakeContextBuilder()

    decision_registry = DecisionRegistry()
    decision_registry.register("reply", ReplyDecision)

    result = asyncio.run(
        run_planner_turn(
            None,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="TelegramMessageReceived",
                payload={"text": "hello", "source_id": "chat-1"},
            ),
            prompt_builder=FakePromptBuilder(),
            llm_backend=backend,
            decision_parser=decision_registry,
            config=PlannerRuntimeConfig(
                model="fake-model",
                prompt_version="v1",
                cwd=Path("/tmp/work"),
                env_home=Path("/tmp/home"),
            ),
            run_store=run_store,
            context_builder=context_builder,
        )
    )

    assert result.run_id == "run_fake"
    assert context_builder.events == ["evt_1"]
    assert backend.request is not None
    assert backend.request.metadata["planner_context"]["trigger"]["source_id"] == "[redacted:source_id]"


def test_planner_turn_redacts_model_boundary_identifiers(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    backend = FakeBackend()
    router = FakeRouter()

    decision_registry = DecisionRegistry()
    decision_registry.register("reply", ReplyDecision)

    result = asyncio.run(
        run_planner_turn(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="ChatBatchReady",
                correlation_id="telegram:123",
                payload={
                    "text": "hello",
                    "source_id": "123",
                    "chat_id": 123,
                    "user_id": 789,
                    "username": "private-user",
                    "batch_messages": [
                        {
                            "text": "hello",
                            "user_id": 789,
                            "raw_event_id": "telegram:123:456",
                            "payload": {"raw": {"secret": "telegram-raw"}},
                        }
                    ],
                },
            ),
            prompt_builder=EchoModelBoundaryPromptBuilder(),
            llm_backend=backend,
            decision_parser=decision_registry,
            session_router=router,
            config=PlannerRuntimeConfig(
                model="fake-model",
                prompt_version="v1",
                cwd=Path("/tmp/work"),
                env_home=Path("/tmp/home"),
                metadata={"chat_id": 123, "safe": "ok"},
            ),
        )
    )

    assert result.context.trigger["source_id"] == "123"
    assert router.request is not None
    assert router.request.user_id == "789"
    assert backend.request is not None
    assert backend.request.metadata is not None
    model_dump = json.dumps(
        {
            "prompt": backend.request.prompt,
            "metadata": backend.request.metadata,
        },
        ensure_ascii=False,
    )
    assert "hello" in model_dump
    assert "telegram-raw" not in model_dump
    assert "private-user" not in model_dump
    assert "telegram:123" not in model_dump
    assert '"789"' not in model_dump
    assert backend.request.metadata["chat_id"] == "[redacted:chat_id]"
    assert backend.request.metadata["safe"] == "ok"
