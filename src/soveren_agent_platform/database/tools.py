"""Dynamic tools for read-only databases."""

from __future__ import annotations

import logging
from typing import Any

from soveren_agent_platform.database.contracts import (
    DatabaseExecutionError,
    ReadOnlyDatabasePort,
)
from soveren_agent_platform.sessions import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)

log = logging.getLogger(__name__)

DATABASE_TOOL_NAMESPACE = "platform.database"


def register_database_tools(
    registry: DynamicToolRegistry,
    database: ReadOnlyDatabasePort,
    *,
    namespace: str = DATABASE_TOOL_NAMESPACE,
) -> None:
    registry.register(
        DynamicToolSpec(
            name="inspect_database",
            namespace=namespace,
            description=(
                "List readable PostgreSQL tables, or describe columns for one "
                "table. Use this before writing a query when the schema is not known."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Optional PostgreSQL schema name.",
                    },
                    "table": {
                        "type": "string",
                        "description": (
                            "Optional table name. When present, returns columns."
                        ),
                    },
                },
                "additionalProperties": False,
            },
        ),
        _inspect_handler(database),
    )
    registry.register(
        DynamicToolSpec(
            name="query_database",
            namespace=namespace,
            description=(
                "Run one read-only SELECT or WITH query against the configured "
                "database. Results are row- and byte-limited. Inspect the database "
                "before querying unknown schema names."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 20_000,
                        "description": "A single PostgreSQL SELECT or WITH query.",
                    },
                    "max_rows": {
                        "type": "integer",
                        "minimum": 1,
                        "description": (
                            "Optional requested row limit; the server cap still applies."
                        ),
                    },
                },
                "required": ["sql"],
                "additionalProperties": False,
            },
        ),
        _query_handler(database),
    )


def _inspect_handler(database: ReadOnlyDatabasePort):
    async def handle(call: DynamicToolCall) -> DynamicToolResult:
        arguments = _arguments(call)
        schema = _optional_string(arguments, "schema")
        table = _optional_string(arguments, "table")
        try:
            result = await database.inspect(schema=schema, table=table)
        except DatabaseExecutionError as exc:
            return DynamicToolResult.json(
                {"error": exc.code, "message": exc.message},
                success=False,
            )
        except ValueError:
            return _failure("invalid_inspection_arguments")
        except Exception:
            log.exception("database inspection tool failed call_id=%s", call.call_id)
            return _failure("database_inspection_failed")
        return DynamicToolResult.json(result)

    return handle


def _query_handler(database: ReadOnlyDatabasePort):
    async def handle(call: DynamicToolCall) -> DynamicToolResult:
        arguments = _arguments(call)
        sql = arguments.get("sql")
        max_rows = arguments.get("max_rows")
        if not isinstance(sql, str) or not sql.strip():
            return _failure("invalid_sql")
        if max_rows is not None and (
            isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0
        ):
            return _failure("invalid_max_rows")
        try:
            result = await database.query(sql, max_rows=max_rows)
        except DatabaseExecutionError as exc:
            return DynamicToolResult.json(
                {"error": exc.code, "message": exc.message},
                success=False,
            )
        except ValueError:
            return _failure("query_rejected")
        except Exception:
            log.exception("database query tool failed call_id=%s", call.call_id)
            return _failure("database_query_failed")
        return DynamicToolResult.json(
            {
                "rows": list(result.rows),
                "row_count": len(result.rows),
                "truncated": result.truncated,
            }
        )

    return handle


def _arguments(call: DynamicToolCall) -> dict[str, Any]:
    if isinstance(call.arguments, dict):
        return call.arguments
    return {}


def _optional_string(arguments: dict[str, Any], name: str) -> str | None:
    value = arguments.get(name)
    return value if isinstance(value, str) and value.strip() else None


def _failure(code: str) -> DynamicToolResult:
    return DynamicToolResult.json({"error": code}, success=False)
