from __future__ import annotations

import asyncio
import json

import pytest

from soveren_agent_platform.database import (
    DATABASE_TOOL_NAMESPACE,
    DatabaseExecutionError,
    DatabaseQueryResult,
    ReadOnlyDatabaseQueryError,
    register_database_tools,
    validate_read_query,
)
from soveren_agent_platform.sessions import DynamicToolRegistry


@pytest.mark.parametrize(
    "sql",
    [
        "select * from public.orders",
        "WITH rows AS (SELECT 1) SELECT * FROM rows",
        "select ';' as value",
        "select $$;$$ as value",
    ],
)
def test_validate_read_query_accepts_single_read_statement(sql: str) -> None:
    assert validate_read_query(sql) == sql.strip()


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "update tasks set title = 'x'",
        "select 1; select 2",
    ],
)
def test_validate_read_query_rejects_unsafe_statement(sql: str) -> None:
    with pytest.raises(ReadOnlyDatabaseQueryError):
        validate_read_query(sql)


class RecordingDatabase:
    async def inspect(self, *, schema: str | None = None, table: str | None = None):
        return {"schema": schema, "table": table, "rows": [], "truncated": False}

    async def query(self, sql: str, *, max_rows: int | None = None):
        if sql == "boom":
            raise DatabaseExecutionError(code="query_failed", message="nope")
        return DatabaseQueryResult(rows=({"value": 1},), truncated=False)

    async def close(self) -> None:
        pass


def test_register_database_tools_exposes_bounded_handlers() -> None:
    registry = DynamicToolRegistry()
    register_database_tools(registry, RecordingDatabase())

    assert {
        (spec.namespace, spec.name)
        for spec in registry.specs()
    } == {
        (DATABASE_TOOL_NAMESPACE, "inspect_database"),
        (DATABASE_TOOL_NAMESPACE, "query_database"),
    }

    inspected = asyncio.run(
        registry.call(
            {
                "callId": "call-1",
                "threadId": "thread-1",
                "turnId": "turn-1",
                "namespace": DATABASE_TOOL_NAMESPACE,
                "tool": "inspect_database",
                "arguments": {},
            }
        )
    )
    inspected_payload = json.loads(inspected["contentItems"][0]["text"])

    queried = asyncio.run(
        registry.call(
            {
                "callId": "call-2",
                "threadId": "thread-1",
                "turnId": "turn-1",
                "namespace": DATABASE_TOOL_NAMESPACE,
                "tool": "query_database",
                "arguments": {"sql": "select 1"},
            }
        )
    )
    queried_payload = json.loads(queried["contentItems"][0]["text"])

    assert inspected["success"] is True
    assert inspected_payload == {
        "schema": None,
        "table": None,
        "rows": [],
        "truncated": False,
    }
    assert queried["success"] is True
    assert queried_payload == {
        "rows": [{"value": 1}],
        "row_count": 1,
        "truncated": False,
    }


def test_database_tool_reports_port_errors() -> None:
    registry = DynamicToolRegistry()
    register_database_tools(registry, RecordingDatabase())

    result = asyncio.run(
        registry.call(
            {
                "callId": "call-1",
                "threadId": "thread-1",
                "turnId": "turn-1",
                "namespace": DATABASE_TOOL_NAMESPACE,
                "tool": "query_database",
                "arguments": {"sql": "boom"},
            }
        )
    )
    payload = json.loads(result["contentItems"][0]["text"])

    assert result["success"] is False
    assert payload == {"error": "query_failed", "message": "nope"}
