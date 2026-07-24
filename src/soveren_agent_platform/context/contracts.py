"""Serializable rich context passed to planner LLM calls."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.json_types import JsonObject, require_json_object
from soveren_agent_platform.sessions.routing import SessionRouteResult


@dataclass(slots=True)
class PlannerContext:
    trigger: JsonObject
    session_routing: JsonObject
    batch: JsonObject | None = None
    sessions: list[JsonObject] = field(default_factory=list)
    mailbox: list[JsonObject] = field(default_factory=list)
    actions: list[JsonObject] = field(default_factory=list)
    outbound: list[JsonObject] = field(default_factory=list)
    cron: list[JsonObject] = field(default_factory=list)

    def to_dict(self) -> JsonObject:
        return require_json_object(
            {
                "trigger": self.trigger,
                "session_routing": self.session_routing,
                "batch": self.batch,
                "sessions": self.sessions,
                "mailbox": self.mailbox,
                "actions": self.actions,
                "outbound": self.outbound,
                "cron": self.cron,
            },
            label="planner context",
        )


class PlannerContextBuilder(Protocol):
    async def build(self, *, event: AgentEvent, route_result: SessionRouteResult) -> PlannerContext: ...
