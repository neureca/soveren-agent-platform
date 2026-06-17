"""Registry of app-provided action executors."""
from __future__ import annotations

from dataclasses import dataclass, field

from agent_platform.actions.contracts import ActionExecutor


@dataclass(slots=True)
class ActionRegistry:
    executors: dict[str, ActionExecutor] = field(default_factory=dict)

    def register(self, kind: str, executor: ActionExecutor) -> None:
        self.executors[kind] = executor

    def get(self, kind: str) -> ActionExecutor:
        try:
            return self.executors[kind]
        except KeyError as exc:
            raise KeyError(f"no action executor registered for kind={kind!r}") from exc

