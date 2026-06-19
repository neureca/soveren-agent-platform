import asyncio
from pathlib import Path
from typing import Literal

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context import PlannerContext
from soveren_agent_platform.decisions import BaseDecision, DecisionRegistry
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
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


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class FakeRouter:
    def __init__(self) -> None:
        self.request: SessionRouteRequest | None = None

    async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
        self.request = request
        return SessionRouteResult(
            snapshots=[
                SessionSnapshot(
                    session_id="cli_1",
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
                session_id="cli_1",
                reasons=["metadata match"],
            ),
        )


class FakeRunStore:
    def __init__(self) -> None:
        self.finalized: list[tuple[str, str, dict]] = []

    async def insert(self, *, tenant_id, trigger_event_id, model, prompt_version, input_summary):
        return "run_fake"

    async def finalize(self, run_id: str, *, status: str, output):
        self.finalized.append((run_id, status, output))


class FakeContextBuilder:
    def __init__(self) -> None:
        self.events: list[str] = []

    def build(self, *, event: AgentEvent, route_result: SessionRouteResult) -> PlannerContext:
        self.events.append(event.id)
        return PlannerContext(
            trigger={"event_id": event.id, "source_id": event.payload["source_id"]},
            session_routing={
                "route_hint": {"action": route_result.hint.action},
                "sessions": [],
            },
        )


def test_planner_turn_includes_session_metadata_in_llm_request(tmp_path):
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
    assert result.session_metadata["route_hint"]["session_id"] == "cli_1"
    assert result.context.session_routing["route_hint"]["session_id"] == "cli_1"
    assert backend.request is not None
    assert backend.request.metadata is not None
    assert backend.request.metadata["session_routing"]["sessions"][0]["session_id"] == "cli_1"
    assert backend.request.metadata["planner_context"]["trigger"]["source_id"] == "chat-1"
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

    assert prompt_builder.context_source_id == "chat-1"
    assert backend.request is not None
    assert backend.request.prompt == "source: chat-1"
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
    assert backend.request.metadata["planner_context"]["trigger"]["source_id"] == "chat-1"
