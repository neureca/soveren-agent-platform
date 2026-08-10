"""Generic read-only database contracts for model-facing tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from soveren_agent_platform.json_types import JsonValue


@dataclass(frozen=True, slots=True)
class DatabaseQueryResult:
    rows: tuple[dict[str, JsonValue], ...]
    truncated: bool


class DatabaseExecutionError(RuntimeError):
    def __init__(self, *, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ReadOnlyDatabasePort(Protocol):
    async def query(
        self,
        sql: str,
        *,
        max_rows: int | None = None,
    ) -> DatabaseQueryResult: ...

    async def inspect(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
    ) -> dict[str, JsonValue]: ...

    async def close(self) -> None: ...
