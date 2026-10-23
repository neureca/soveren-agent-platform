"""Trusted context for one model execution and its dynamic tools."""

from __future__ import annotations

from dataclasses import dataclass

from soveren_agent_platform.conversation import ConversationScope

DynamicToolSelector = tuple[str | None, str]
DynamicToolAllowlist = tuple[DynamicToolSelector, ...]


@dataclass(frozen=True, slots=True)
class ToolExecutionContext:
    """Host-authored identity attached to one model turn and its tool calls."""

    scope: ConversationScope
    execution_id: str
    task_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ConversationScope):
            raise TypeError("scope must be a ConversationScope")
        if not isinstance(self.execution_id, str) or not self.execution_id.strip():
            raise ValueError("execution_id must be a non-empty string")
        if self.task_id is not None and (
            not isinstance(self.task_id, str) or not self.task_id.strip()
        ):
            raise ValueError("task_id must be a non-empty string when provided")


def validate_dynamic_tool_allowlist(
    allowlist: DynamicToolAllowlist | None,
) -> DynamicToolAllowlist | None:
    """Validate the immutable, exact selectors used for one native turn."""
    if allowlist is None:
        return None
    if not isinstance(allowlist, tuple):
        raise TypeError("dynamic_tool_allowlist must be an immutable tuple or None")
    normalized: list[DynamicToolSelector] = []
    seen: set[DynamicToolSelector] = set()
    for selector in allowlist:
        if not isinstance(selector, tuple) or len(selector) != 2:
            raise TypeError("each dynamic tool selector must be a (namespace, name) tuple")
        namespace, name = selector
        if namespace is not None and (
            not isinstance(namespace, str) or not namespace.strip()
        ):
            raise ValueError("dynamic tool selector namespace must be non-empty or None")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("dynamic tool selector name must be non-empty")
        if selector in seen:
            raise ValueError(f"duplicate dynamic tool selector: {selector!r}")
        seen.add(selector)
        normalized.append(selector)
    return tuple(normalized)
