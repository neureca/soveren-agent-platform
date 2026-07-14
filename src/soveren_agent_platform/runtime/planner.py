"""Platform planner envelope around queue events, sessions, LLM, and decisions."""

from __future__ import annotations

import inspect
import sqlite3
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from pydantic import BaseModel

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context.builder import ContextLimits, SQLitePlannerContextBuilder
from soveren_agent_platform.context.contracts import PlannerContext
from soveren_agent_platform.context.contracts import PlannerContextBuilder as PlannerContextBuilderPort
from soveren_agent_platform.context.redaction import (
    ModelRedactionPolicy,
    redact_agent_event_for_model,
    redact_planner_context_for_model,
    redact_value_for_model,
)
from soveren_agent_platform.decisions.dispatcher import DecisionDispatcher, DispatchContext, DispatchResult
from soveren_agent_platform.decisions.effects import DecisionEffects
from soveren_agent_platform.decisions.sqlite import sqlite_decision_effects
from soveren_agent_platform.llm.contracts import LlmBackend, LlmRequest
from soveren_agent_platform.runs.contracts import RunStore
from soveren_agent_platform.runs.sqlite import SQLiteRunStore
from soveren_agent_platform.sessions.routing import EmptySessionRouter, SessionRouter, SessionRouteRequest


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


@dataclass(slots=True)
class PlannerDispatchResult:
    planner: PlannerResult
    dispatch: DispatchResult


class PlannerRunInProgressError(RuntimeError):
    """Another worker still owns the durable planner run."""


class PlannerRunLeaseLostError(RuntimeError):
    """The planner run was superseded before its result was persisted."""


class PlannerPromptBuilder(Protocol):
    def build_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: dict[str, Any],
        context: PlannerContext | None = None,
    ) -> str: ...

    def build_system_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: dict[str, Any],
        context: PlannerContext | None = None,
    ) -> str: ...


class DecisionParser(Protocol):
    def parse(self, raw_text: str) -> Any: ...


@dataclass(slots=True)
class PlannerRuntimeConfig:
    model: str
    prompt_version: str
    cwd: Path
    env_home: Path
    timeout_s: int = 120
    metadata: dict[str, Any] = field(default_factory=dict)
    context_limits: ContextLimits = field(default_factory=ContextLimits)
    model_redaction_policy: ModelRedactionPolicy = field(default_factory=ModelRedactionPolicy)


@dataclass(slots=True)
class PlannerRuntime:
    """Compose planner ports without exposing storage implementation details."""

    run_store: RunStore
    context_builder: PlannerContextBuilderPort
    session_router: SessionRouter = field(default_factory=EmptySessionRouter)
    effects: DecisionEffects | None = None

    async def run_turn(
        self,
        *,
        event: AgentEvent,
        prompt_builder: PlannerPromptBuilder,
        llm_backend: LlmBackend,
        decision_parser: DecisionParser,
        config: PlannerRuntimeConfig,
    ) -> PlannerResult:
        return await run_planner_turn(
            None,
            event=event,
            prompt_builder=prompt_builder,
            llm_backend=llm_backend,
            decision_parser=decision_parser,
            config=config,
            session_router=self.session_router,
            run_store=self.run_store,
            context_builder=self.context_builder,
        )

    async def run_dispatch_turn(
        self,
        *,
        event: AgentEvent,
        prompt_builder: PlannerPromptBuilder,
        llm_backend: LlmBackend,
        decision_parser: DecisionParser,
        dispatcher: DecisionDispatcher,
        config: PlannerRuntimeConfig,
        dispatch_context: DispatchContext | None = None,
    ) -> PlannerDispatchResult:
        if self.effects is None:
            raise ValueError("planner dispatch requires configured decision effects")
        return await run_planner_dispatch_turn(
            None,
            event=event,
            prompt_builder=prompt_builder,
            llm_backend=llm_backend,
            decision_parser=decision_parser,
            dispatcher=dispatcher,
            config=config,
            session_router=self.session_router,
            run_store=self.run_store,
            context_builder=self.context_builder,
            dispatch_context=dispatch_context,
            effects=self.effects,
        )


async def run_planner_turn(
    conn: sqlite3.Connection | None,
    *,
    event: AgentEvent,
    prompt_builder: PlannerPromptBuilder,
    llm_backend: LlmBackend,
    decision_parser: DecisionParser,
    config: PlannerRuntimeConfig,
    session_router: SessionRouter | None = None,
    run_store: RunStore | None = None,
    context_builder: PlannerContextBuilderPort | None = None,
) -> PlannerResult:
    """Run one durable planner turn and include session-routing metadata in the LLM request."""
    source_id = _source_id(event)
    runs = run_store or SQLiteRunStore._from_connection(_require_conn(conn, "default planner run store"))
    run = await runs.claim(
        tenant_id=event.tenant_id,
        source_id=source_id,
        trigger_event_id=event.id,
        model=config.model,
        prompt_version=config.prompt_version,
        input_summary=_input_summary(event),
        stale_after_s=max(config.timeout_s + 30, 60),
    )
    if not run.acquired:
        if run.output is not None:
            return _restore_planner_result(run.id, run.output, decision_parser)
        raise PlannerRunInProgressError(f"planner run is already active: {run.id}")
    if run.lease_token is None:
        raise RuntimeError(f"acquired planner run has no lease token: {run.id}")

    run_id = run.id
    lease_token = run.lease_token
    session_metadata: dict[str, Any] = {}
    context: PlannerContext | None = None
    try:
        router = session_router or EmptySessionRouter()
        route_result = await router.route(_route_request(event, source_id=source_id))
        builder = context_builder or SQLitePlannerContextBuilder._from_connection(
            _require_conn(conn, "default planner context builder"),
            limits=config.context_limits,
        )
        context = await builder.build(event=event, route_result=route_result)
        session_metadata = context.session_routing
        model_event = redact_agent_event_for_model(event, policy=config.model_redaction_policy)
        model_context = redact_planner_context_for_model(context, policy=config.model_redaction_policy)
        model_session_metadata = model_context.session_routing
        response = await llm_backend.run(
            LlmRequest(
                prompt=_build_prompt(
                    prompt_builder,
                    method_name="build_prompt",
                    event=model_event,
                    session_metadata=model_session_metadata,
                    context=model_context,
                ),
                system_prompt=_build_prompt(
                    prompt_builder,
                    method_name="build_system_prompt",
                    event=model_event,
                    session_metadata=model_session_metadata,
                    context=model_context,
                ),
                cwd=config.cwd,
                env_home=config.env_home,
                model=config.model,
                timeout_s=config.timeout_s,
                metadata={
                    **redact_value_for_model(config.metadata, policy=config.model_redaction_policy),
                    "trigger_event_id": event.id,
                    "trigger_message_type": event.message_type,
                    "session_routing": model_session_metadata,
                    "planner_context": model_context.to_dict(),
                },
            )
        )
        decision = decision_parser.parse(response.text)
        finalized = await runs.finalize(
            run_id,
            lease_token=lease_token,
            status="completed",
            output={
                "decision": _serialize_decision(decision),
                "llm_text": response.text,
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
        if not finalized:
            raise PlannerRunLeaseLostError(f"planner run lease was lost: {run_id}")
        return PlannerResult(
            run_id=run_id,
            decision=decision,
            llm_text=response.text,
            session_metadata=session_metadata,
            context=context,
        )
    except BaseException as exc:
        if isinstance(exc, PlannerRunLeaseLostError):
            raise
        try:
            finalized = await runs.finalize(
                run_id,
                lease_token=lease_token,
                status="failed",
                output={
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "session_routing": session_metadata,
                    "planner_context": context.to_dict() if context is not None else None,
                },
            )
            if not finalized:
                raise PlannerRunLeaseLostError(f"planner run lease was lost: {run_id}")
        except BaseException as finalize_error:
            raise BaseExceptionGroup(
                "planner turn failed and its failed state could not be persisted",
                [exc, finalize_error],
            ) from None
        raise


async def run_planner_dispatch_turn(
    conn: sqlite3.Connection | None,
    *,
    event: AgentEvent,
    prompt_builder: PlannerPromptBuilder,
    llm_backend: LlmBackend,
    decision_parser: DecisionParser,
    dispatcher: DecisionDispatcher,
    config: PlannerRuntimeConfig,
    session_router: SessionRouter | None = None,
    run_store: RunStore | None = None,
    context_builder: PlannerContextBuilderPort | None = None,
    dispatch_context: DispatchContext | None = None,
    effects: DecisionEffects | None = None,
) -> PlannerDispatchResult:
    """Run one planner turn and dispatch the parsed decision into runtime side effects."""
    planner = await run_planner_turn(
        conn,
        event=event,
        prompt_builder=prompt_builder,
        llm_backend=llm_backend,
        decision_parser=decision_parser,
        config=config,
        session_router=session_router,
        run_store=run_store,
        context_builder=context_builder,
    )
    context = dispatch_context or _dispatch_context(event, planner)
    dispatch = await dispatcher.dispatch(
        effects or sqlite_decision_effects(_require_conn(conn, "default decision effects")),
        planner.decision,
        context,
    )
    return PlannerDispatchResult(planner=planner, dispatch=dispatch)


def _route_request(event: AgentEvent, *, source_id: str | None = None) -> SessionRouteRequest:
    text = str(event.payload.get("text") or "")
    exact_source_id = source_id or _source_id(event)
    user_id = event.payload.get("user_id")
    return SessionRouteRequest(
        tenant_id=event.tenant_id,
        source_id=exact_source_id,
        text=text,
        user_id=str(user_id) if user_id is not None else None,
        metadata={
            "event_id": event.id,
            "message_type": event.message_type,
            "payload": event.payload,
        },
    )


def _require_conn(conn: sqlite3.Connection | None, dependency: str) -> sqlite3.Connection:
    if conn is None:
        raise ValueError(f"{dependency} requires a SQLite connection")
    return conn


def _dispatch_context(event: AgentEvent, planner: PlannerResult) -> DispatchContext:
    source_id = _source_id(event)
    user_id = event.payload.get("user_id")
    return DispatchContext(
        tenant_id=event.tenant_id,
        source_id=source_id,
        run_id=planner.run_id,
        source_event_id=event.id,
        actor_id=str(user_id) if user_id is not None else None,
        metadata={
            "session_routing": planner.session_metadata,
            "planner_context": planner.context.to_dict(),
        },
    )


def _source_id(event: AgentEvent) -> str:
    source_id = event.payload.get("source_id")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("planner event must contain a non-empty string source_id")
    return source_id.strip()


def _input_summary(event: AgentEvent) -> str:
    text = str(event.payload.get("text") or "")
    return text[:500] if text else event.message_type


def _serialize_decision(decision: Any) -> dict[str, Any]:
    if isinstance(decision, BaseModel):
        return decision.model_dump()
    if is_dataclass(decision):
        return asdict(cast(Any, decision))
    kind = getattr(decision, "kind", None)
    payload = getattr(decision, "payload", None)
    if isinstance(kind, str) and isinstance(payload, dict):
        return {"kind": kind, **payload}
    if isinstance(decision, dict):
        return decision
    raise TypeError(f"cannot serialize decision object: {type(decision).__name__}")


def _restore_planner_result(
    run_id: str,
    output: dict[str, Any],
    decision_parser: DecisionParser,
) -> PlannerResult:
    llm_text = output.get("llm_text")
    context_data = output.get("planner_context")
    if not isinstance(llm_text, str) or not isinstance(context_data, dict):
        raise ValueError(f"durable planner output is incomplete for run: {run_id}")
    context = PlannerContext(**context_data)
    session_metadata = output.get("session_routing")
    if not isinstance(session_metadata, dict):
        session_metadata = context.session_routing
    return PlannerResult(
        run_id=run_id,
        decision=decision_parser.parse(llm_text),
        llm_text=llm_text,
        session_metadata=session_metadata,
        context=context,
    )


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
