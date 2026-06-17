import asyncio
from pathlib import Path
from typing import Literal

from agent_platform.agent.contracts import AgentEvent
from agent_platform.decisions import BaseDecision, DecisionRegistry
from agent_platform.llm.contracts import LlmRequest, LlmResponse
from agent_platform.runtime.planner import (
    ParsedDecision,
    PlannerRuntimeConfig,
    run_planner_turn,
)
from agent_platform.sessions.routing import (
    RouteHint,
    SessionRouteRequest,
    SessionRouteResult,
    SessionSnapshot,
)
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


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
                    title="agent-platform",
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
                payload={"text": "продолжи в сессии agent-platform", "source_id": "chat-1"},
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
    assert backend.request is not None
    assert backend.request.metadata is not None
    assert backend.request.metadata["session_routing"]["sessions"][0]["session_id"] == "cli_1"
    assert router.request is not None
    assert router.request.source_id == "chat-1"
