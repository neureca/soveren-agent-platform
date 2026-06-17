"""Optional prompt formatter for rich planner context."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agent_platform.context.contracts import PlannerContext


@dataclass(frozen=True, slots=True)
class ContextFormattingLimits:
    max_items_per_section: int = 8
    max_text_chars: int = 500


class PlannerContextFormatter:
    """Render platform context into compact app-neutral prompt text."""

    def __init__(self, *, limits: ContextFormattingLimits | None = None) -> None:
        self.limits = limits or ContextFormattingLimits()

    def format(self, context: PlannerContext) -> str:
        lines = ["PLATFORM CONTEXT"]
        lines.extend(_trigger_lines(context.trigger))
        if context.batch:
            lines.extend(_batch_lines(context.batch, limits=self.limits))
        lines.extend(_session_routing_lines(context.session_routing, limits=self.limits))
        lines.extend(_items_section("Sessions", context.sessions, limits=self.limits))
        lines.extend(_items_section("Mailbox", context.mailbox, limits=self.limits))
        lines.extend(_items_section("Actions", context.actions, limits=self.limits))
        lines.extend(_items_section("Outbound", context.outbound, limits=self.limits))
        lines.extend(_items_section("Cron", context.cron, limits=self.limits))
        return "\n".join(lines).strip()


def format_planner_context(
    context: PlannerContext,
    *,
    limits: ContextFormattingLimits | None = None,
) -> str:
    return PlannerContextFormatter(limits=limits).format(context)


def _trigger_lines(trigger: dict[str, Any]) -> list[str]:
    return [
        "",
        "Trigger:",
        f"- event_id: {trigger.get('event_id')}",
        f"- message_type: {trigger.get('message_type')}",
        f"- source_id: {trigger.get('source_id')}",
        f"- channel: {trigger.get('channel')}",
    ]


def _batch_lines(batch: dict[str, Any], *, limits: ContextFormattingLimits) -> list[str]:
    lines = [
        "",
        "Inbound batch:",
        f"- batch_id: {batch.get('batch_id')}",
        f"- message_count: {batch.get('message_count')}",
    ]
    text = _clip(str(batch.get("text") or ""), limits.max_text_chars)
    if text:
        lines.append(f"- text: {text}")
    messages = batch.get("messages")
    if isinstance(messages, list) and messages:
        lines.append("- messages:")
        for message in messages[:limits.max_items_per_section]:
            lines.append(f"  - {_compact_json(message, limits=limits)}")
    return lines


def _session_routing_lines(
    session_routing: dict[str, Any],
    *,
    limits: ContextFormattingLimits,
) -> list[str]:
    hint = session_routing.get("route_hint") or {}
    if not isinstance(hint, dict):
        return []
    return [
        "",
        "Session routing:",
        f"- action: {hint.get('action')}",
        f"- session_id: {hint.get('session_id')}",
        f"- confidence: {hint.get('confidence')}",
        f"- reasons: {_clip(_compact_json(hint.get('reasons') or [], limits=limits), limits.max_text_chars)}",
    ]


def _items_section(
    title: str,
    items: list[dict[str, Any]],
    *,
    limits: ContextFormattingLimits,
) -> list[str]:
    if not items:
        return []
    lines = ["", f"{title}:"]
    for item in items[:limits.max_items_per_section]:
        lines.append(f"- {_compact_json(item, limits=limits)}")
    if len(items) > limits.max_items_per_section:
        lines.append(f"- ... {len(items) - limits.max_items_per_section} more")
    return lines


def _compact_json(value: Any, *, limits: ContextFormattingLimits) -> str:
    return _clip(json.dumps(value, ensure_ascii=False, sort_keys=True), limits.max_text_chars)


def _clip(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    if max_chars <= 3:
        return "." * max_chars
    return value[:max_chars - 3] + "..."
