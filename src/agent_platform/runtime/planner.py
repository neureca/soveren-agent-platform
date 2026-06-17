"""Platform planner envelope around queue events, sessions, LLM, and decisions."""
from __future__ import annotations

import inspect
import sqlite3
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

from agent_platform.agent.contracts import AgentEvent
from agent_platform.context import ContextLimits, PlannerContext, build_planner_context
from agent_platform.llm.contracts import LlmBackend, LlmRequest
from agent_platform.runs.contracts import RunStore
from agent_platform.runs.sqlite import SQLiteRunStore
from agent_platform.sessions.routing import EmptySessionRouter, SessionRouteRequest, SessionRouter


@dataclass(slots=True)
class ParsedDecision:
    kind: str
    payload: dict[str, Any]


@dataclass(slots=True)
class PlannerResult:
    run_id: str
    decision: ParsedDecision
    llm_text: str
    session_metadata: dict[str, Any]
    context: PlannerContext


class PlannerPromptBuilder(Protocol):
    def build_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: dict[str, Any],
        context: PlannerContext | None = None,
    ) -> str:
        ...

    def build_system_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: dict[str, Any],
        context: PlannerContext | None = None,
    ) -> str:
        ...


class DecisionParser(Protocol):
    def parse(self, raw_text: str) -> Any:
        ...


@dataclass(slots=True)
class PlannerRuntimeConfig:
    model: str
    prompt_version: str
    cwd: Path
    env_home: Path
    timeout_s: int = 120
    metadata: dict[str, Any] = field(default_factory=dict)
    context_limits: ContextLimits = field(default_factory=ContextLimits)


async def run_planner_turn(
    conn: sqlite3.Connection,
    *,
    event: AgentEvent,
    prompt_builder: PlannerPromptBuilder,
    llm_backend: LlmBackend,
    decision_parser: DecisionParser,
    config: PlannerRuntimeConfig,
    session_router: SessionRouter | None = None,
    run_store: RunStore | None = None,
) -> PlannerResult:
    """Run one durable planner turn and include session-routing metadata in the LLM request."""
    router = session_router or EmptySessionRouter()
    route_result = await router.route(_route_request(event))
    context = build_planner_context(
        conn,
        event=event,
        route_result=route_result,
        limits=config.context_limits,
    )
    session_metadata = context.session_routing
    runs = run_store or SQLiteRunStore(conn)
    run_id = await runs.insert(
        tenant_id=event.tenant_id,
        trigger_event_id=event.id,
        model=config.model,
        prompt_version=config.prompt_version,
        input_summary=_input_summary(event),
    )
    try:
        response = await llm_backend.run(
            LlmRequest(
                prompt=_build_prompt(
                    prompt_builder,
                    method_name="build_prompt",
                    event=event,
                    session_metadata=session_metadata,
                    context=context,
                ),
                system_prompt=_build_prompt(
                    prompt_builder,
                    method_name="build_system_prompt",
                    event=event,
                    session_metadata=session_metadata,
                    context=context,
                ),
                cwd=config.cwd,
                env_home=config.env_home,
                model=config.model,
                timeout_s=config.timeout_s,
                metadata={
                    **config.metadata,
                    "trigger_event_id": event.id,
                    "trigger_message_type": event.message_type,
                    "session_routing": session_metadata,
                    "planner_context": context.to_dict(),
                },
            )
        )
        decision = decision_parser.parse(response.text)
        await runs.finalize(
            run_id,
            status="completed",
            output={
                "decision": _serialize_decision(decision),
                "llm": {
                    "session_id": response.session_id,
                    "cost_usd": response.cost_usd,
                    "duration_ms": response.duration_ms,
                    "input_tokens": response.input_tokens,
                    "output_tokens": response.output_tokens,
                    "metadata": response.metadata,
                },
                "session_routing": session_metadata,
                "planner_context": context.to_dict(),
            },
        )
        return PlannerResult(
            run_id=run_id,
            decision=decision,
            llm_text=response.text,
            session_metadata=session_metadata,
            context=context,
        )
    except Exception as exc:
        await runs.finalize(
            run_id,
            status="failed",
            output={
                "error_type": type(exc).__name__,
                "error": str(exc),
                "session_routing": session_metadata,
                "planner_context": context.to_dict(),
            },
        )
        raise


def _route_request(event: AgentEvent) -> SessionRouteRequest:
    text = str(event.payload.get("text") or "")
    source_id = str(event.payload.get("source_id") or event.payload.get("chat_id") or event.correlation_id or "")
    user_id = event.payload.get("user_id")
    return SessionRouteRequest(
        tenant_id=event.tenant_id,
        source_id=source_id,
        text=text,
        user_id=str(user_id) if user_id is not None else None,
        metadata={
            "event_id": event.id,
            "message_type": event.message_type,
            "payload": event.payload,
        },
    )


def _input_summary(event: AgentEvent) -> str:
    text = str(event.payload.get("text") or "")
    return text[:500] if text else event.message_type


def _serialize_decision(decision: Any) -> dict[str, Any]:
    if isinstance(decision, BaseModel):
        return decision.model_dump()
    if is_dataclass(decision):
        return asdict(decision)
    kind = getattr(decision, "kind", None)
    payload = getattr(decision, "payload", None)
    if isinstance(kind, str) and isinstance(payload, dict):
        return {"kind": kind, **payload}
    if isinstance(decision, dict):
        return decision
    raise TypeError(f"cannot serialize decision object: {type(decision).__name__}")


def _build_prompt(
    prompt_builder: PlannerPromptBuilder,
    *,
    method_name: str,
    event: AgentEvent,
    session_metadata: dict[str, Any],
    context: PlannerContext,
) -> str:
    method = getattr(prompt_builder, method_name)
    if "context" in inspect.signature(method).parameters:
        return method(event=event, session_metadata=session_metadata, context=context)
    return method(event=event, session_metadata=session_metadata)
