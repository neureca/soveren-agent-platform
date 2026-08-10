"""Asyncpg-backed read-only PostgreSQL adapter for agent tools."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID

from soveren_agent_platform.database.contracts import (
    DatabaseExecutionError,
    DatabaseQueryResult,
)
from soveren_agent_platform.json_types import JsonValue

_READ_QUERY_PREFIX = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class ReadOnlyDatabaseSecurityError(RuntimeError):
    """Raised when the configured database identity is not safe for agent use."""


class ReadOnlyDatabaseQueryError(ValueError):
    """Raised when a model-provided query violates the read-only contract."""


class AsyncpgReadOnlyDatabase:
    def __init__(
        self,
        *,
        dsn: str,
        expected_user: str,
        application_name: str = "soveren-agent-readonly-tools",
        statement_timeout_s: float = 10,
        max_rows: int = 200,
        max_result_bytes: int = 512 * 1024,
        pool_size: int = 2,
    ) -> None:
        if not dsn.strip():
            raise ValueError("database DSN must not be empty")
        if not expected_user.strip():
            raise ValueError("expected database user must not be empty")
        if not application_name.strip():
            raise ValueError("application name must not be empty")
        if statement_timeout_s <= 0 or not math.isfinite(statement_timeout_s):
            raise ValueError("statement timeout must be positive")
        if max_rows <= 0:
            raise ValueError("max rows must be positive")
        if max_result_bytes <= 0:
            raise ValueError("max result bytes must be positive")
        if pool_size <= 0:
            raise ValueError("pool size must be positive")

        self._dsn = dsn
        self._expected_user = expected_user
        self._application_name = application_name
        self._statement_timeout_s = statement_timeout_s
        self._max_rows = max_rows
        self._max_result_bytes = max_result_bytes
        self._pool_size = pool_size
        self._pool: Any | None = None
        self._open_lock = asyncio.Lock()

    async def open(self) -> None:
        await self._get_pool()

    async def close(self) -> None:
        pool = self._pool
        self._pool = None
        if pool is not None:
            await asyncio.wait_for(pool.close(), timeout=10)

    async def query(
        self,
        sql: str,
        *,
        max_rows: int | None = None,
    ) -> DatabaseQueryResult:
        statement = validate_read_query(sql)
        requested_rows = self._max_rows if max_rows is None else max_rows
        if requested_rows <= 0:
            raise ReadOnlyDatabaseQueryError("max_rows must be positive")
        return await self._execute(
            statement,
            (),
            max_rows=min(requested_rows, self._max_rows),
        )

    async def inspect(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
    ) -> dict[str, JsonValue]:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        if table is None:
            result = await self._execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND ($1::text IS NULL OR table_schema = $1)
                  AND has_table_privilege(
                      quote_ident(table_schema) || '.' || quote_ident(table_name),
                      'SELECT'
                  )
                ORDER BY table_schema, table_name
                """,
                (schema,),
                max_rows=self._max_rows,
            )
            return {
                "kind": "tables",
                "schema": schema,
                "rows": list(result.rows),
                "truncated": result.truncated,
            }

        resolved_schema = schema or "public"
        result = await self._execute(
            """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
              AND has_column_privilege(
                  quote_ident(table_schema) || '.' || quote_ident(table_name),
                  column_name,
                  'SELECT'
              )
            ORDER BY ordinal_position
            """,
            (resolved_schema, table),
            max_rows=self._max_rows,
        )
        return {
            "kind": "columns",
            "schema": resolved_schema,
            "table": table,
            "rows": list(result.rows),
            "truncated": result.truncated,
        }

    async def _get_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        async with self._open_lock:
            if self._pool is None:
                asyncpg = _load_asyncpg()
                timeout_ms = max(1, int(self._statement_timeout_s * 1000))
                self._pool = await asyncpg.create_pool(
                    self._dsn,
                    min_size=1,
                    max_size=self._pool_size,
                    command_timeout=self._statement_timeout_s,
                    statement_cache_size=0,
                    server_settings={
                        "application_name": self._application_name,
                        "default_transaction_read_only": "on",
                        "idle_in_transaction_session_timeout": f"{timeout_ms}ms",
                        "lock_timeout": f"{min(timeout_ms, 1000)}ms",
                        "statement_timeout": f"{timeout_ms}ms",
                    },
                    init=self._validate_connection,
                )
        return self._pool

    async def _validate_connection(self, connection: Any) -> None:
        identity = await connection.fetchrow(
            """
            SELECT
                current_user AS username,
                rolcreatedb,
                rolcreaterole,
                rolreplication,
                rolsuper,
                rolbypassrls
            FROM pg_roles
            WHERE rolname = current_user
            """
        )
        if identity is None or identity["username"] != self._expected_user:
            actual = None if identity is None else identity["username"]
            raise ReadOnlyDatabaseSecurityError(
                f"expected database user {self._expected_user!r}, got {actual!r}"
            )
        elevated = (
            "rolcreatedb",
            "rolcreaterole",
            "rolreplication",
            "rolsuper",
            "rolbypassrls",
        )
        if any(identity[attribute] for attribute in elevated):
            raise ReadOnlyDatabaseSecurityError(
                f"database user {self._expected_user!r} has elevated privileges"
            )

    async def _execute(
        self,
        sql: str,
        args: Sequence[Any],
        *,
        max_rows: int,
    ) -> DatabaseQueryResult:
        asyncpg = _load_asyncpg()
        pool = await self._get_pool()
        rows: list[dict[str, JsonValue]] = []
        size_bytes = 2
        truncated = False
        try:
            async with pool.acquire(timeout=self._statement_timeout_s) as connection:
                async with connection.transaction(readonly=True):
                    if await connection.fetchval("SHOW transaction_read_only") != "on":
                        raise ReadOnlyDatabaseSecurityError(
                            "database transaction is not read-only"
                        )
                    cursor = connection.cursor(
                        sql,
                        *args,
                        prefetch=min(max_rows + 1, 50),
                        timeout=self._statement_timeout_s,
                    )
                    async for record in cursor:
                        if len(rows) >= max_rows:
                            truncated = True
                            break
                        normalized = {
                            str(key): _json_value(value)
                            for key, value in dict(record).items()
                        }
                        encoded = json.dumps(
                            normalized,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        separator_size = 1 if rows else 0
                        if (
                            size_bytes + separator_size + len(encoded)
                            > self._max_result_bytes
                        ):
                            truncated = True
                            break
                        rows.append(normalized)
                        size_bytes += separator_size + len(encoded)
        except ReadOnlyDatabaseSecurityError:
            raise
        except asyncpg.QueryCanceledError as exc:
            raise DatabaseExecutionError(
                code="query_timeout",
                message="The database stopped the query after its time limit.",
            ) from exc
        except asyncpg.ReadOnlySQLTransactionError as exc:
            raise DatabaseExecutionError(
                code="query_rejected",
                message="PostgreSQL rejected an operation in the read-only transaction.",
            ) from exc
        except asyncpg.PostgresError as exc:
            message = " ".join(str(getattr(exc, "message", exc)).split())
            raise DatabaseExecutionError(
                code="database_query_failed",
                message=message[:500],
            ) from exc
        return DatabaseQueryResult(rows=tuple(rows), truncated=truncated)


def validate_read_query(sql: str) -> str:
    statement = sql.strip()
    if not statement:
        raise ReadOnlyDatabaseQueryError("sql must not be empty")
    if len(statement) > 20_000:
        raise ReadOnlyDatabaseQueryError("sql is too long")
    if not _READ_QUERY_PREFIX.match(statement):
        raise ReadOnlyDatabaseQueryError("only SELECT or WITH queries are allowed")
    if _contains_statement_separator(statement):
        raise ReadOnlyDatabaseQueryError("only one SQL statement is allowed")
    return statement


def _contains_statement_separator(sql: str) -> bool:
    index = 0
    state = "normal"
    dollar_tag: str | None = None
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if state == "normal":
            if char == "'":
                state = "single"
            elif char == '"':
                state = "double"
            elif char == "-" and following == "-":
                state = "line_comment"
                index += 1
            elif char == "/" and following == "*":
                state = "block_comment"
                index += 1
            elif char == "$":
                match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[index:])
                if match is not None:
                    dollar_tag = match.group(0)
                    state = "dollar"
                    index += len(dollar_tag) - 1
            elif char == ";":
                if sql[index + 1 :].strip():
                    return True
        elif state == "single":
            if char == "'" and following == "'":
                index += 1
            elif char == "'":
                state = "normal"
        elif state == "double":
            if char == '"' and following == '"':
                index += 1
            elif char == '"':
                state = "normal"
        elif state == "line_comment":
            if char in "\r\n":
                state = "normal"
        elif state == "block_comment":
            if char == "*" and following == "/":
                state = "normal"
                index += 1
        elif state == "dollar" and dollar_tag is not None:
            if sql.startswith(dollar_tag, index):
                state = "normal"
                index += len(dollar_tag) - 1
                dollar_tag = None
        index += 1
    return False


def _validate_identifier(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if not _IDENTIFIER.fullmatch(normalized):
        raise ReadOnlyDatabaseQueryError(f"invalid {name} name")
    return normalized


def _json_value(value: Any) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID, IPv4Address, IPv6Address)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_json_value(item) for item in value]
    return str(value)


def _load_asyncpg() -> Any:
    try:
        import asyncpg  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("asyncpg is required to enable read-only database tools") from exc
    return asyncpg
