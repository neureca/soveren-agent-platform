import asyncio
from pathlib import Path
from typing import Literal

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.decisions import (
    ActionDecisionHandler,
    BaseDecision,
    DecisionDispatcher,
    DecisionRegistry,
    OutboundDecisionHandler,
)
from soveren_agent_platform.llm.contracts import LlmRequest, LlmResponse
from soveren_agent_platform.runtime import PlannerRuntimeConfig, run_planner_dispatch_turn
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class FakeBackend:
    name = "fake"
    version = "1"

    def __init__(self, text: str) -> None:
        self.text = text
        self.request: LlmRequest | None = None

    async def run(self, request: LlmRequest) -> LlmResponse:
        self.request = request
        return LlmResponse(text=self.text, session_id="llm-session")


class ContextPromptBuilder:
    def build_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return f"{event.payload['text']}\ncontext_source={context.trigger['source_id']}"

    def build_system_prompt(self, *, event: AgentEvent, session_metadata: dict, context) -> str:
        return f"sessions={len(session_metadata['sessions'])}"


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class CreateTaskDecision(BaseDecision):
    kind: Literal["create_task"]
    title: str


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
    assert reply_backend.request.metadata["planner_context"]["trigger"]["source_id"] == "chat-1"

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


def _event(event_id: str, text: str, source_id: str) -> AgentEvent:
    return AgentEvent(
        id=event_id,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"text": text, "source_id": source_id, "user_id": "user-1"},
    )


def _config() -> PlannerRuntimeConfig:
    return PlannerRuntimeConfig(
        model="fake-model",
        prompt_version="v1",
        cwd=Path("/tmp/work"),
        env_home=Path("/tmp/home"),
    )
