"""Platform planner envelope around queue events, sessions, LLM, and decisions."""

from __future__ import annotations

import inspect
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

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
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.decisions.contracts import (
    Decision,
    DecisionDispatchClaim,
    DecisionDispatchStore,
    PayloadDecision,
)
from soveren_agent_platform.decisions.dispatcher import DecisionDispatcher, DispatchContext, DispatchResult
from soveren_agent_platform.decisions.effects import DecisionEffects
from soveren_agent_platform.decisions.sqlite import (
    SQLiteDecisionDispatchStore,
    sqlite_decision_effects,
)
from soveren_agent_platform.idempotency import idempotency_fingerprint
from soveren_agent_platform.json_types import JsonObject, JsonValue, require_json_object
from soveren_agent_platform.llm.contracts import LlmBackend, LlmRequest
from soveren_agent_platform.runs.contracts import RunStore
from soveren_agent_platform.runs.sqlite import SQLiteRunStore
from soveren_agent_platform.sessions.routing import EmptySessionRouter, SessionRouter, SessionRouteRequest


@dataclass(slots=True)
class ParsedDecision:
    kind: str
    payload: JsonObject


@dataclass(slots=True)
class PlannerResult:
    run_id: str
    decision: Decision
    llm_text: str
    session_metadata: JsonObject
    context: PlannerContext


@dataclass(slots=True)
class PlannerDispatchResult:
    planner: PlannerResult
    dispatch: DispatchResult


class PlannerRunInProgressError(RuntimeError):
    """Another worker still owns the durable planner run."""


class PlannerRunLeaseLostError(RuntimeError):
    """The planner run was superseded before its result was persisted."""


class DecisionDispatchInProgressError(RuntimeError):
    """Another worker still owns the accepted-decision dispatch."""


class DecisionDispatchLeaseLostError(RuntimeError):
    """The accepted-decision dispatch was superseded before persistence."""


class PlannerPromptBuilder(Protocol):
    def build_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: JsonObject,
        context: PlannerContext | None = None,
    ) -> str: ...

    def build_system_prompt(
        self,
        *,
        event: AgentEvent,
        session_metadata: JsonObject,
        context: PlannerContext | None = None,
    ) -> str: ...


class DecisionParser(Protocol):
    def parse(self, raw_text: str) -> Decision: ...


@dataclass(slots=True)
class PlannerRuntimeConfig:
    model: str
    prompt_version: str
    cwd: Path
    env_home: Path
    timeout_s: int = 120
    metadata: JsonObject = field(default_factory=dict)
    context_limits: ContextLimits = field(default_factory=ContextLimits)
    model_redaction_policy: ModelRedactionPolicy = field(default_factory=ModelRedactionPolicy)


@dataclass(slots=True)
class PlannerRuntime:
    """Compose planner ports without exposing storage implementation details."""

    run_store: RunStore
    context_builder: PlannerContextBuilderPort
    session_router: SessionRouter = field(default_factory=EmptySessionRouter)
    effects: DecisionEffects | None = None
    decision_dispatch_store: DecisionDispatchStore | None = None
    decision_dispatch_receipts_enabled: bool | None = None

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
            decision_dispatch_store=self.decision_dispatch_store,
            decision_dispatch_receipts_enabled=self.decision_dispatch_receipts_enabled,
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
        input_fingerprint=_input_fingerprint(event),
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
    session_metadata: JsonObject = {}
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
                conversation_scope=ConversationScope(
                    tenant_id=event.tenant_id,
                    source_id=source_id,
                ),
                timeout_s=config.timeout_s,
                metadata={
                    **require_json_object(
                        redact_value_for_model(config.metadata, policy=config.model_redaction_policy),
                        label="redacted planner metadata",
                    ),
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
                    **_exception_payload(exc),
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
    decision_dispatch_store: DecisionDispatchStore | None = None,
    decision_dispatch_receipts_enabled: bool | None = None,
) -> PlannerDispatchResult:
    """Accept and dispatch at most one planner decision for one source event."""
    source_id = _source_id(event)
    receipt_store = decision_dispatch_store
    receipts_enabled = decision_dispatch_receipts_enabled
    if receipts_enabled is None:
        receipts_enabled = receipt_store is not None or conn is not None
    if receipts_enabled and receipt_store is None and conn is not None:
        receipt_store = SQLiteDecisionDispatchStore._from_connection(conn)
    if receipts_enabled and receipt_store is None:
        raise ValueError("decision dispatch receipts require a DecisionDispatchStore")
    if not receipts_enabled:
        return await _run_unreceipted_planner_dispatch_turn(
            conn,
            event=event,
            prompt_builder=prompt_builder,
            llm_backend=llm_backend,
            decision_parser=decision_parser,
            dispatcher=dispatcher,
            config=config,
            session_router=session_router,
            run_store=run_store,
            context_builder=context_builder,
            dispatch_context=dispatch_context,
            effects=effects,
        )
    assert receipt_store is not None

    claim = await receipt_store.claim(
        tenant_id=event.tenant_id,
        source_id=source_id,
        trigger_event_id=event.id,
        input_fingerprint=_input_fingerprint(event),
        stale_after_s=max(config.timeout_s + 30, 60),
    )
    if not claim.acquired:
        if claim.status == "completed":
            return _restore_completed_dispatch(claim, decision_parser)
        raise DecisionDispatchInProgressError(
            f"decision dispatch is already active: {claim.id}"
        )
    if claim.lease_token is None:
        raise RuntimeError(f"acquired decision dispatch has no lease token: {claim.id}")

    lease_token = claim.lease_token
    try:
        if claim.status == "planning":
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
            _validate_dispatch_context(event, source_id=source_id, context=context)
            accepted = await receipt_store.accept(
                claim.id,
                lease_token=lease_token,
                run_id=planner.run_id,
                model=config.model,
                prompt_version=config.prompt_version,
                decision=_serialize_decision(planner.decision),
                planner_result=_serialize_planner_result(planner),
                dispatch_context=_serialize_dispatch_context(context),
            )
            if not accepted:
                raise DecisionDispatchLeaseLostError(
                    f"decision dispatch lease was lost before acceptance: {claim.id}"
                )
        elif claim.status == "dispatching":
            planner = _restore_receipt_planner_result(claim, decision_parser)
            context = _restore_dispatch_context(claim)
            _validate_dispatch_context(event, source_id=source_id, context=context)
        else:
            raise ValueError(
                f"acquired decision dispatch has unsupported status={claim.status!r}: {claim.id}"
            )

        dispatch = await dispatcher.dispatch(
            effects or sqlite_decision_effects(_require_conn(conn, "default decision effects")),
            planner.decision,
            context,
        )
        completed = await receipt_store.complete(
            claim.id,
            lease_token=lease_token,
            dispatch_result=_serialize_dispatch_result(dispatch),
        )
        if not completed:
            raise DecisionDispatchLeaseLostError(
                f"decision dispatch lease was lost after effect dispatch: {claim.id}"
            )
        return PlannerDispatchResult(planner=planner, dispatch=dispatch)
    except BaseException as exc:
        if isinstance(exc, DecisionDispatchLeaseLostError):
            raise
        try:
            released = await receipt_store.release(claim.id, lease_token=lease_token)
        except BaseException as release_error:
            raise BaseExceptionGroup(
                "decision dispatch failed and its lease could not be released",
                [exc, release_error],
            ) from None
        if not released:
            raise BaseExceptionGroup(
                "decision dispatch failed after losing its lease",
                [
                    exc,
                    DecisionDispatchLeaseLostError(
                        f"decision dispatch lease was lost while handling failure: {claim.id}"
                    ),
                ],
            ) from None
        raise


async def _run_unreceipted_planner_dispatch_turn(
    conn: sqlite3.Connection | None,
    *,
    event: AgentEvent,
    prompt_builder: PlannerPromptBuilder,
    llm_backend: LlmBackend,
    decision_parser: DecisionParser,
    dispatcher: DecisionDispatcher,
    config: PlannerRuntimeConfig,
    session_router: SessionRouter | None,
    run_store: RunStore | None,
    context_builder: PlannerContextBuilderPort | None,
    dispatch_context: DispatchContext | None,
    effects: DecisionEffects | None,
) -> PlannerDispatchResult:
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
    _validate_dispatch_context(event, source_id=_source_id(event), context=context)
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


def _input_fingerprint(event: AgentEvent) -> str:
    return idempotency_fingerprint(
        {
            "id": event.id,
            "tenant_id": event.tenant_id,
            "recipient": event.recipient,
            "message_type": event.message_type,
            "payload": event.payload,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
        }
    )


def _serialize_decision(decision: Decision) -> JsonObject:
    if isinstance(decision, BaseModel):
        return require_json_object(
            decision.model_dump(mode="json", by_alias=True, round_trip=True),
            label="decision",
        )
    if not isinstance(decision, PayloadDecision):
        raise TypeError(f"decision type has no serializable payload: {type(decision).__name__}")
    return require_json_object(
        {**decision.payload, "kind": decision.kind},
        label="decision",
    )


def _serialize_planner_result(planner: PlannerResult) -> JsonObject:
    return require_json_object(
        {
            "run_id": planner.run_id,
            "llm_text": planner.llm_text,
            "session_metadata": planner.session_metadata,
            "context": planner.context.to_dict(),
        },
        label="planner result",
    )


def _serialize_dispatch_context(context: DispatchContext) -> JsonObject:
    return require_json_object(
        {
            "tenant_id": context.tenant_id,
            "source_id": context.source_id,
            "run_id": context.run_id,
            "source_event_id": context.source_event_id,
            "actor_id": context.actor_id,
            "metadata": context.metadata,
        },
        label="dispatch context",
    )


def _serialize_dispatch_result(dispatch: DispatchResult) -> JsonObject:
    return require_json_object(
        {
            "target": dispatch.target,
            "id": dispatch.id,
            "created": dispatch.created,
            "status": dispatch.status,
            "metadata": dispatch.metadata,
        },
        label="dispatch result",
    )


def _restore_receipt_planner_result(
    claim: DecisionDispatchClaim,
    decision_parser: DecisionParser,
) -> PlannerResult:
    if claim.run_id is None or claim.decision is None or claim.planner_result is None:
        raise ValueError(f"accepted decision dispatch is incomplete: {claim.id}")
    context_data = claim.planner_result.get("context")
    llm_text = claim.planner_result.get("llm_text")
    session_metadata = claim.planner_result.get("session_metadata")
    if not isinstance(context_data, dict) or not isinstance(llm_text, str):
        raise ValueError(f"accepted planner result is incomplete: {claim.id}")
    if not isinstance(session_metadata, dict):
        raise ValueError(f"accepted planner session metadata is invalid: {claim.id}")
    if claim.planner_result.get("run_id") != claim.run_id:
        raise ValueError(f"accepted planner run id is inconsistent: {claim.id}")
    return PlannerResult(
        run_id=claim.run_id,
        decision=decision_parser.parse(
            json.dumps(claim.decision, ensure_ascii=False, separators=(",", ":"))
        ),
        llm_text=llm_text,
        session_metadata=session_metadata,
        context=_restore_planner_context(context_data),
    )


def _restore_dispatch_context(claim: DecisionDispatchClaim) -> DispatchContext:
    data = claim.dispatch_context
    if not isinstance(data, dict):
        raise ValueError(f"accepted decision dispatch context is missing: {claim.id}")
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(f"accepted decision dispatch metadata is invalid: {claim.id}")
    tenant_id = data.get("tenant_id")
    source_id = data.get("source_id")
    if not isinstance(tenant_id, str) or not isinstance(source_id, str):
        raise ValueError(f"accepted decision dispatch scope is invalid: {claim.id}")
    return DispatchContext(
        tenant_id=tenant_id,
        source_id=source_id,
        run_id=_optional_string(data.get("run_id")),
        source_event_id=_optional_string(data.get("source_event_id")),
        actor_id=_optional_string(data.get("actor_id")),
        metadata=metadata,
    )


def _restore_dispatch_result(claim: DecisionDispatchClaim) -> DispatchResult:
    data = claim.dispatch_result
    if not isinstance(data, dict):
        raise ValueError(f"completed decision dispatch result is missing: {claim.id}")
    target = data.get("target")
    metadata = data.get("metadata")
    if not isinstance(target, str) or not isinstance(metadata, dict):
        raise ValueError(f"completed decision dispatch result is invalid: {claim.id}")
    created = data.get("created")
    if not isinstance(created, bool):
        raise ValueError(f"completed decision dispatch created flag is invalid: {claim.id}")
    return DispatchResult(
        target=target,
        id=_optional_string(data.get("id")),
        created=created,
        status=_optional_string(data.get("status")),
        metadata=metadata,
    )


def _restore_completed_dispatch(
    claim: DecisionDispatchClaim,
    decision_parser: DecisionParser,
) -> PlannerDispatchResult:
    return PlannerDispatchResult(
        planner=_restore_receipt_planner_result(claim, decision_parser),
        dispatch=_restore_dispatch_result(claim),
    )


def _validate_dispatch_context(
    event: AgentEvent,
    *,
    source_id: str,
    context: DispatchContext,
) -> None:
    if context.tenant_id != event.tenant_id or context.source_id != source_id:
        raise ValueError(
            "decision dispatch context must match the source event tenant and conversation"
        )


def _optional_string(value: JsonValue) -> str | None:
    return None if value is None else str(value)


def _exception_payload(exc: BaseException) -> JsonObject:
    payload: JsonObject = {
        "error_type": type(exc).__name__,
        "error": str(exc),
    }
    if isinstance(exc, BaseExceptionGroup):
        payload["errors"] = [_exception_payload(nested) for nested in exc.exceptions]
    return payload


def _restore_planner_result(
    run_id: str,
    output: JsonObject,
    decision_parser: DecisionParser,
) -> PlannerResult:
    llm_text = output.get("llm_text")
    context_data = output.get("planner_context")
    if not isinstance(llm_text, str) or not isinstance(context_data, dict):
        raise ValueError(f"durable planner output is incomplete for run: {run_id}")
    context = _restore_planner_context(context_data)
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


def _restore_planner_context(data: JsonObject) -> PlannerContext:
    return PlannerContext(
        trigger=_required_json_object(data, "trigger"),
        session_routing=_required_json_object(data, "session_routing"),
        batch=_optional_json_object(data, "batch"),
        sessions=_json_object_list(data, "sessions"),
        mailbox=_json_object_list(data, "mailbox"),
        actions=_json_object_list(data, "actions"),
        outbound=_json_object_list(data, "outbound"),
        cron=_json_object_list(data, "cron"),
    )


def _required_json_object(data: JsonObject, key: str) -> JsonObject:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"planner context {key} must be a JSON object")
    return require_json_object(value, label=f"planner context {key}")


def _optional_json_object(data: JsonObject, key: str) -> JsonObject | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"planner context {key} must be a JSON object or null")
    return require_json_object(value, label=f"planner context {key}")


def _json_object_list(data: JsonObject, key: str) -> list[JsonObject]:
    value = data.get(key)
    if not isinstance(value, list):
        raise ValueError(f"planner context {key} must be a JSON object list")
    return [
        require_json_object(item, label=f"planner context {key}[{index}]")
        for index, item in enumerate(value)
    ]


def _build_prompt(
    prompt_builder: PlannerPromptBuilder,
    *,
    method_name: str,
    event: AgentEvent,
    session_metadata: JsonObject,
    context: PlannerContext,
) -> str:
    method = getattr(prompt_builder, method_name)
    if "context" in inspect.signature(method).parameters:
        return method(event=event, session_metadata=session_metadata, context=context)
    return method(event=event, session_metadata=session_metadata)
