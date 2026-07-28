"""Conversation-bound dynamic tools for scheduled jobs."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from soveren_agent_platform.cron.contracts import ScheduledJobStore
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)

SCHEDULE_TOOL_NAMESPACE = "platform.schedules"


def register_scheduled_job_tools(
    registry: DynamicToolRegistry,
    store: ScheduledJobStore,
    *,
    tenant_id: str,
    source_id: str,
) -> None:
    """Register list/cancel tools fixed to one private conversation."""
    if not tenant_id.strip() or not source_id.strip():
        raise ValueError("tenant_id and source_id must be non-empty")
    registry.bind_conversation(tenant_id=tenant_id, source_id=source_id)
    registry.register(
        DynamicToolSpec(
            name="list_scheduled_jobs",
            namespace=SCHEDULE_TOOL_NAMESPACE,
            description="List active scheduled jobs in the current conversation.",
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
            },
        ),
        lambda call: _list_scheduled_jobs_tool(
            store,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="cancel_scheduled_job",
            namespace=SCHEDULE_TOOL_NAMESPACE,
            description=(
                "Cancel one scheduled job in the current conversation. "
                "A due event that was already dispatched may still be handled."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["job_id"],
                "properties": {
                    "job_id": {"type": "string", "minLength": 1},
                },
            },
        ),
        lambda call: _cancel_scheduled_job_tool(
            store,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
        ),
    )


async def _list_scheduled_jobs_tool(
    store: ScheduledJobStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
) -> DynamicToolResult:
    jobs = await store.list_jobs(
        tenant_id=tenant_id,
        source_id=source_id,
        limit=_limit(_args(call).get("limit"), default=20),
    )
    return DynamicToolResult.json({"jobs": [asdict(job) for job in jobs]})


async def _cancel_scheduled_job_tool(
    store: ScheduledJobStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
) -> DynamicToolResult:
    cancellation = await store.cancel_job(
        _job_id(_args(call).get("job_id")),
        tenant_id=tenant_id,
        source_id=source_id,
    )
    return DynamicToolResult.json(asdict(cancellation))


def _args(call: DynamicToolCall) -> dict[str, Any]:
    return call.arguments if isinstance(call.arguments, dict) else {}


def _job_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("job_id must be a non-empty string")
    return value


def _limit(value: Any, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return max(1, min(value, 50))
    return default
