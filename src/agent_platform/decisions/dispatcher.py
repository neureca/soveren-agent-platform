"""Dispatch typed decisions into platform runtime side effects."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from pydantic import BaseModel

from agent_platform.actions.store import insert_action
from agent_platform.cron.store import insert_job
from agent_platform.outbound.store import enqueue_outbound
from agent_platform.queue.durable import enqueue as enqueue_event
from agent_platform.sessions.mailbox import enqueue_prompt


@dataclass(slots=True)
class DispatchContext:
    tenant_id: str
    source_id: str
    run_id: str | None = None
    source_event_id: str | None = None
    actor_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DispatchResult:
    target: str
    id: str | None
    created: bool = True
    status: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DecisionHandler(Protocol):
    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        ...


Resolver = Callable[[Any, DispatchContext], Any]


class DecisionDispatcher:
    def __init__(self) -> None:
        self._handlers: dict[str, DecisionHandler] = {}

    def register(self, kind: str, handler: DecisionHandler) -> None:
        if not kind or not isinstance(kind, str):
            raise ValueError("decision kind must be a non-empty string")
        if kind in self._handlers:
            raise ValueError(f"decision handler already registered for kind={kind!r}")
        self._handlers[kind] = handler

    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        kind = _decision_kind(decision)
        handler = self._handlers.get(kind)
        if handler is None:
            raise KeyError(f"no decision handler registered for kind={kind!r}")
        return handler.dispatch(conn, decision, context)

    def registered_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))


@dataclass(slots=True)
class OutboundDecisionHandler:
    channel: str | Resolver
    destination_id: str | Resolver
    text: str | Resolver = "text"
    payload: dict[str, Any] | Resolver | None = None
    idempotency_key: str | Resolver | None = None

    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        message_id = enqueue_outbound(
            conn,
            tenant_id=context.tenant_id,
            channel=str(_resolve(self.channel, decision, context)),
            destination_id=str(_resolve(self.destination_id, decision, context)),
            text=str(_resolve(self.text, decision, context)),
            payload=_resolve_payload(self.payload, decision, context),
            idempotency_key=_idempotency_key(self.idempotency_key, "outbound", decision, context),
            correlation_id=context.source_event_id or context.run_id,
        )
        return DispatchResult(target="outbound", id=message_id, created=message_id is not None)


@dataclass(slots=True)
class ActionDecisionHandler:
    action_kind: str | Resolver | None = None
    payload: dict[str, Any] | Resolver | None = None
    approval_policy: str | Resolver = "manual"
    idempotency_key: str | Resolver | None = None
    enqueue_when_approved: bool = True

    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        action_kind = self.action_kind if self.action_kind is not None else _decision_kind(decision)
        action_id, created = insert_action(
            conn,
            tenant_id=context.tenant_id,
            kind=str(_resolve(action_kind, decision, context)),
            payload=_resolve_payload(self.payload, decision, context),
            run_id=context.run_id,
            approval_policy=str(_resolve(self.approval_policy, decision, context)),
            source_id=context.source_id,
            source_event_id=context.source_event_id,
            idempotency_key=_idempotency_key(self.idempotency_key, "action", decision, context),
        )
        action = conn.execute("SELECT status FROM actions WHERE id = ?", (action_id,)).fetchone()
        status = action["status"] if action is not None else None
        if self.enqueue_when_approved and status == "approved":
            enqueue_event(
                conn,
                tenant_id=context.tenant_id,
                recipient="actions",
                message_type="ExecuteAction",
                payload={"action_id": action_id},
                idempotency_key=f"execute-action:{action_id}",
                correlation_id=action_id,
                causation_id=context.source_event_id,
            )
        return DispatchResult(target="action", id=action_id, created=created, status=status)


@dataclass(slots=True)
class SessionMailboxDecisionHandler:
    session_id: str | Resolver
    prompt: str | Resolver = "prompt"
    action_id: str | Resolver | None = None

    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        mailbox_id, created = enqueue_prompt(
            conn,
            session_id=str(_resolve(self.session_id, decision, context)),
            tenant_id=context.tenant_id,
            source_id=context.source_id,
            prompt=str(_resolve(self.prompt, decision, context)),
            action_id=_optional_str(_resolve(self.action_id, decision, context)) if self.action_id else None,
            source_event_id=context.source_event_id,
        )
        return DispatchResult(target="session_mailbox", id=mailbox_id, created=created)


@dataclass(slots=True)
class CronDecisionHandler:
    name: str | Resolver
    run_at: str | Resolver
    payload: dict[str, Any] | Resolver | None = None
    rrule: str | Resolver | None = None
    timezone: str | Resolver = "UTC"

    def dispatch(
        self,
        conn: sqlite3.Connection,
        decision: Any,
        context: DispatchContext,
    ) -> DispatchResult:
        job_id = insert_job(
            conn,
            tenant_id=context.tenant_id,
            name=str(_resolve(self.name, decision, context)),
            payload=_resolve_payload(self.payload, decision, context),
            run_at=int(_resolve(self.run_at, decision, context)),
            rrule=_optional_str(_resolve(self.rrule, decision, context)) if self.rrule else None,
            timezone=str(_resolve(self.timezone, decision, context)),
        )
        return DispatchResult(target="cron", id=job_id, created=True, status="pending")


def _decision_kind(decision: Any) -> str:
    kind = getattr(decision, "kind", None)
    if not isinstance(kind, str) or not kind:
        raise ValueError("decision object must expose non-empty string `kind`")
    return kind


def _resolve(value: Any, decision: Any, context: DispatchContext) -> Any:
    if callable(value):
        return value(decision, context)
    if isinstance(value, str) and _has_field(decision, value):
        return _field(decision, value)
    return value


def _resolve_payload(
    value: dict[str, Any] | Resolver | None,
    decision: Any,
    context: DispatchContext,
) -> dict[str, Any]:
    if value is None:
        return _decision_payload(decision)
    resolved = _resolve(value, decision, context)
    if not isinstance(resolved, dict):
        raise TypeError("resolved payload must be a dict")
    return resolved


def _decision_payload(decision: Any) -> dict[str, Any]:
    payload = getattr(decision, "payload", None)
    if isinstance(payload, dict):
        return payload
    if isinstance(decision, BaseModel):
        return decision.model_dump(exclude={"kind"})
    if isinstance(decision, dict):
        return {k: v for k, v in decision.items() if k != "kind"}
    raise TypeError(f"cannot derive payload from decision type: {type(decision).__name__}")


def _has_field(decision: Any, name: str) -> bool:
    if isinstance(decision, BaseModel):
        return name in decision.model_fields_set or hasattr(decision, name)
    if isinstance(decision, dict):
        return name in decision
    return hasattr(decision, name)


def _field(decision: Any, name: str) -> Any:
    if isinstance(decision, dict):
        return decision[name]
    return getattr(decision, name)


def _idempotency_key(
    resolver: str | Resolver | None,
    prefix: str,
    decision: Any,
    context: DispatchContext,
) -> str:
    if resolver is not None:
        return str(_resolve(resolver, decision, context))
    source = context.source_event_id or context.run_id or context.source_id
    return f"{prefix}:{_decision_kind(decision)}:{source}"


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)

