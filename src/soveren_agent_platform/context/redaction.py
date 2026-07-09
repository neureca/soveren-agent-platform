"""Redaction helpers for data crossing the model boundary."""
from __future__ import annotations

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context.contracts import PlannerContext
from soveren_agent_platform.model_boundary import (
    DEFAULT_MODEL_REDACT_KEYS,
    ModelRedactionPolicy,
    redact_value_for_model,
)

__all__ = [
    "DEFAULT_MODEL_REDACT_KEYS",
    "ModelRedactionPolicy",
    "redact_agent_event_for_model",
    "redact_planner_context_for_model",
    "redact_value_for_model",
]


def redact_agent_event_for_model(
    event: AgentEvent,
    *,
    policy: ModelRedactionPolicy | None = None,
) -> AgentEvent:
    active_policy = policy or ModelRedactionPolicy()
    return AgentEvent(
        id=event.id,
        tenant_id=event.tenant_id,
        recipient=event.recipient,
        message_type=event.message_type,
        payload=redact_value_for_model(event.payload, policy=active_policy),
        correlation_id=active_policy.replacement("correlation_id") if event.correlation_id else None,
        causation_id=active_policy.replacement("causation_id") if event.causation_id else None,
    )


def redact_planner_context_for_model(
    context: PlannerContext,
    *,
    policy: ModelRedactionPolicy | None = None,
) -> PlannerContext:
    active_policy = policy or ModelRedactionPolicy()
    data = redact_value_for_model(context.to_dict(), policy=active_policy)
    return PlannerContext(
        trigger=data["trigger"],
        session_routing=data["session_routing"],
        batch=data.get("batch"),
        sessions=data.get("sessions") or [],
        mailbox=data.get("mailbox") or [],
        actions=data.get("actions") or [],
        outbound=data.get("outbound") or [],
        cron=data.get("cron") or [],
    )
