"""Planner runtime orchestration.

Runtime exports are loaded lazily so importing a port package does not pull the
planner and worker graph back through that package while it is still being
initialized.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from soveren_agent_platform.runtime.planner import (
        DecisionParser,
        ParsedDecision,
        PlannerDispatchResult,
        PlannerPromptBuilder,
        PlannerResult,
        PlannerRunInProgressError,
        PlannerRunLeaseLostError,
        PlannerRuntime,
        PlannerRuntimeConfig,
    )
    from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker

_EXPORT_MODULES = {
    "DecisionParser": "soveren_agent_platform.runtime.planner",
    "ParsedDecision": "soveren_agent_platform.runtime.planner",
    "PlannerDispatchResult": "soveren_agent_platform.runtime.planner",
    "PlannerPromptBuilder": "soveren_agent_platform.runtime.planner",
    "PlannerResult": "soveren_agent_platform.runtime.planner",
    "PlannerRunInProgressError": "soveren_agent_platform.runtime.planner",
    "PlannerRunLeaseLostError": "soveren_agent_platform.runtime.planner",
    "PlannerRuntime": "soveren_agent_platform.runtime.planner",
    "PlannerRuntimeConfig": "soveren_agent_platform.runtime.planner",
    "PollingWorkerConfig": "soveren_agent_platform.runtime.worker_loop",
    "run_polling_worker": "soveren_agent_platform.runtime.worker_loop",
}

__all__ = [
    "DecisionParser",
    "ParsedDecision",
    "PlannerDispatchResult",
    "PlannerPromptBuilder",
    "PlannerResult",
    "PlannerRunInProgressError",
    "PlannerRunLeaseLostError",
    "PlannerRuntime",
    "PlannerRuntimeConfig",
    "PollingWorkerConfig",
    "run_polling_worker",
]


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))
