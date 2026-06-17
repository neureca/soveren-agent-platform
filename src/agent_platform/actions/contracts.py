"""Contracts for action execution."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True)
class ActionExecutionResult:
    result: dict[str, Any] = field(default_factory=dict)
    status: str = "executed"


class ActionExecutor(Protocol):
    async def execute(self, conn: sqlite3.Connection, action: sqlite3.Row) -> ActionExecutionResult:
        ...

