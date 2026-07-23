"""Dispatch typed decisions into platform runtime side effects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Generic, Protocol, TypeVar, cast

from pydantic import BaseModel

from soveren_agent_platform.decisions.contracts import Decision
from soveren_agent_platform.decisions.effects import DecisionEffects
from soveren_agent_platform.json_types import JsonObject, require_json_object

DecisionT = TypeVar("DecisionT", bound=Decision)
DecisionT_contra = TypeVar("DecisionT_contra", bound=Decision, contravariant=True)


@dataclass(slots=True)
class DispatchContext:
    tenant_id: str
    source_id: str
    run_id: str | None = None
    source_event_id: str | None = None
    actor_id: str | None = None
    metadata: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.metadata = require_json_object(self.metadata, label="dispatch context metadata")


@dataclass(slots=True)
class DispatchResult:
    target: str
    id: str | None
    created: bool = True
    status: str | None = None
    metadata: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.metadata = require_json_object(self.metadata, label="dispatch result metadata")


class DecisionHandler(Protocol[DecisionT_contra]):
    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: DecisionT_contra,
        context: DispatchContext,
    ) -> DispatchResult: ...


Resolver = Callable[[Decision, DispatchContext], object]


class _ErasedDecisionHandler(Protocol):
    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult: ...


@dataclass(slots=True)
class _DecisionHandlerAdapter(Generic[DecisionT]):
    handler: DecisionHandler[DecisionT]

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        return await self.handler.dispatch(effects, cast(DecisionT, decision), context)


class DecisionDispatcher:
    def __init__(self) -> None:
        self._handlers: dict[str, _ErasedDecisionHandler] = {}

    def register(self, kind: str, handler: DecisionHandler[DecisionT]) -> None:
        if not kind or not isinstance(kind, str):
            raise ValueError("decision kind must be a non-empty string")
        if kind in self._handlers:
            raise ValueError(f"decision handler already registered for kind={kind!r}")
        self._handlers[kind] = _DecisionHandlerAdapter(handler)

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        kind = _decision_kind(decision)
        handler = self._handlers.get(kind)
        if handler is None:
            raise KeyError(f"no decision handler registered for kind={kind!r}")
        return await handler.dispatch(effects, decision, context)

    def registered_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))


@dataclass(slots=True)
class OutboundDecisionHandler:
    channel: str | Resolver
    destination_id: str | Resolver
    text: str | Resolver = "text"
    payload: JsonObject | Resolver | None = None
    idempotency_key: str | Resolver | None = None

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        channel = str(_resolve(self.channel, decision, context))
        destination_id = str(_resolve(self.destination_id, decision, context))
        text = str(_resolve(self.text, decision, context))
        payload = _resolve_payload(self.payload, decision, context)
        idempotency_key = _idempotency_key(
            self.idempotency_key,
            "outbound",
            decision,
            context,
        )
        correlation_id = context.source_event_id or context.run_id
        enqueue_with_result = getattr(effects.outbound, "enqueue_with_result", None)
        if callable(enqueue_with_result):
            enqueue_result = await enqueue_with_result(
                tenant_id=context.tenant_id,
                source_id=context.source_id,
                channel=channel,
                destination_id=destination_id,
                text=text,
                payload=payload,
                idempotency_key=idempotency_key,
                correlation_id=correlation_id,
            )
            return DispatchResult(
                target="outbound",
                id=enqueue_result.message_id,
                created=enqueue_result.created,
            )
        message_id = await effects.outbound.enqueue(
            tenant_id=context.tenant_id,
            source_id=context.source_id,
            channel=channel,
            destination_id=destination_id,
            text=text,
            payload=payload,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
        )
        return DispatchResult(
            target="outbound",
            id=message_id,
            created=message_id is not None,
        )


@dataclass(slots=True)
class ActionDecisionHandler:
    action_kind: str | Resolver | None = None
    payload: JsonObject | Resolver | None = None
    approval_policy: str | Resolver = "manual"
    idempotency_key: str | Resolver | None = None
    enqueue_when_approved: bool = True

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        if effects.action_dispatch is None:
            raise RuntimeError("ActionDecisionHandler requires DecisionEffects.action_dispatch")
        action_kind = self.action_kind if self.action_kind is not None else _decision_kind(decision)
        result = await effects.action_dispatch.insert_action(
            tenant_id=context.tenant_id,
            kind=str(_resolve(action_kind, decision, context)),
            payload=_resolve_payload(self.payload, decision, context),
            run_id=context.run_id,
            approval_policy=str(_resolve(self.approval_policy, decision, context)),
            source_id=context.source_id,
            source_event_id=context.source_event_id,
            idempotency_key=_idempotency_key(self.idempotency_key, "action", decision, context),
            enqueue_when_approved=self.enqueue_when_approved,
        )
        return DispatchResult(target="action", id=result.action_id, created=result.created, status=result.status)


@dataclass(slots=True)
class SessionMailboxDecisionHandler:
    session_id: str | Resolver
    prompt: str | Resolver = "prompt"
    action_id: str | Resolver | None = None
    idempotency_key: str | Resolver | None = None

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        mailbox_id, created = await effects.session_mailbox.enqueue_prompt(
            session_id=str(_resolve(self.session_id, decision, context)),
            tenant_id=context.tenant_id,
            source_id=context.source_id,
            prompt=str(_resolve(self.prompt, decision, context)),
            action_id=_optional_str(_resolve(self.action_id, decision, context)) if self.action_id else None,
            source_event_id=context.source_event_id,
            idempotency_key=_idempotency_key(
                self.idempotency_key,
                "session-mailbox",
                decision,
                context,
            ),
        )
        return DispatchResult(target="session_mailbox", id=mailbox_id, created=created)


@dataclass(slots=True)
class CronDecisionHandler:
    name: str | Resolver
    run_at: str | Resolver
    payload: JsonObject | Resolver | None = None
    rrule: str | Resolver | None = None
    timezone: str | Resolver = "UTC"
    idempotency_key: str | Resolver | None = None

    async def dispatch(
        self,
        effects: DecisionEffects,
        decision: Decision,
        context: DispatchContext,
    ) -> DispatchResult:
        job_id, created = await effects.cron.insert(
            tenant_id=context.tenant_id,
            source_id=context.source_id,
            name=str(_resolve(self.name, decision, context)),
            payload=_resolve_payload(self.payload, decision, context),
            run_at=_resolved_int(self.run_at, decision, context),
            rrule=_optional_str(_resolve(self.rrule, decision, context)) if self.rrule else None,
            timezone=str(_resolve(self.timezone, decision, context)),
            idempotency_key=_idempotency_key(
                self.idempotency_key,
                "cron",
                decision,
                context,
            ),
        )
        return DispatchResult(target="cron", id=job_id, created=created, status="pending")


def _decision_kind(decision: Decision) -> str:
    kind = decision.kind
    if not isinstance(kind, str) or not kind:
        raise ValueError("decision object must expose non-empty string `kind`")
    return kind


def _resolve(value: object, decision: Decision, context: DispatchContext) -> object:
    if callable(value):
        return value(decision, context)
    if isinstance(value, str) and _has_field(decision, value):
        return _field(decision, value)
    return value


def _resolve_payload(
    value: JsonObject | Resolver | None,
    decision: Decision,
    context: DispatchContext,
) -> JsonObject:
    if value is None:
        return _decision_payload(decision)
    resolved = _resolve(value, decision, context)
    return require_json_object(resolved, label="resolved decision payload")


def _decision_payload(decision: Decision) -> JsonObject:
    return require_json_object(decision.payload, label="decision payload")


def _has_field(decision: Decision, name: str) -> bool:
    if isinstance(decision, BaseModel):
        return name in decision.model_fields_set or hasattr(decision, name)
    return hasattr(decision, name)


def _field(decision: Decision, name: str) -> object:
    return getattr(decision, name)


def _idempotency_key(
    resolver: str | Resolver | None,
    prefix: str,
    decision: Decision,
    context: DispatchContext,
) -> str:
    if resolver is not None:
        return str(_resolve(resolver, decision, context))
    source = context.source_event_id or context.run_id or context.source_id
    return f"{prefix}:{_decision_kind(decision)}:{source}"


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _resolved_int(value: object, decision: Decision, context: DispatchContext) -> int:
    resolved = _resolve(value, decision, context)
    if isinstance(resolved, bool) or not isinstance(resolved, (int, float, str)):
        raise TypeError("resolved value must be an integer or numeric string")
    return int(resolved)
