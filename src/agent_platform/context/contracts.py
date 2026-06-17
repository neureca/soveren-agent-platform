"""Serializable rich context passed to planner LLM calls."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class PlannerContext:
    trigger: dict[str, Any]
    session_routing: dict[str, Any]
    batch: dict[str, Any] | None = None
    sessions: list[dict[str, Any]] = field(default_factory=list)
    mailbox: list[dict[str, Any]] = field(default_factory=list)
    actions: list[dict[str, Any]] = field(default_factory=list)
    outbound: list[dict[str, Any]] = field(default_factory=list)
    cron: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
