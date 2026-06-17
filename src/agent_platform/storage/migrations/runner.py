"""Forward-only SQL migrations with platform/app namespaces."""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib.resources import as_file, files
from pathlib import Path
from typing import Iterator, Protocol


class MigrationProvider(Protocol):
    @contextmanager
    def migration_dir(self) -> Iterator[Path]:
        ...


@dataclass(frozen=True, slots=True)
class DirectoryMigrationProvider:
    path: Path

    @contextmanager
    def migration_dir(self) -> Iterator[Path]:
        yield self.path


@dataclass(frozen=True, slots=True)
class PackageMigrationProvider:
    package: str
    resource: str

    @contextmanager
    def migration_dir(self) -> Iterator[Path]:
        migration_root = files(self.package).joinpath(self.resource)
        with as_file(migration_root) as path:
            yield path


PLATFORM_NAMESPACE = "platform"
PLATFORM_MIGRATION_PROVIDER = PackageMigrationProvider(
    "agent_platform.storage.migrations",
    "platform",
)
PLATFORM_TABLE_COLUMNS: dict[str, set[str]] = {
    "event_queue": {
        "id", "tenant_id", "recipient", "message_type", "payload_json",
        "status", "schema_version", "priority", "run_after", "attempts",
        "max_attempts", "lease_owner", "lease_until", "idempotency_key",
        "correlation_id", "causation_id", "last_error", "created_at", "updated_at",
    },
    "agent_runs": {
        "id", "tenant_id", "trigger_event_id", "status", "input_summary",
        "output_json", "model", "prompt_version", "created_at", "updated_at",
    },
    "cron_jobs": {
        "id", "tenant_id", "name", "payload_json", "status", "run_at",
        "rrule", "timezone", "lease_owner", "lease_until", "attempts",
        "max_attempts", "last_error", "created_at", "updated_at",
    },
    "inbound_batches": {
        "id", "tenant_id", "channel", "source_id", "status",
        "first_message_at", "last_message_at", "message_count",
        "decision_json", "created_at", "updated_at",
    },
    "inbound_batch_messages": {
        "id", "batch_id", "tenant_id", "channel", "source_id", "raw_event_id",
        "source_event_id", "payload_json", "message_at", "created_at",
    },
    "runtime_sessions": {
        "id", "tenant_id", "source_id", "owner_id", "kind", "backend",
        "backend_session_id", "title", "cwd", "status",
        "current_action_id", "last_error", "metadata_json", "created_at",
        "updated_at", "last_used_at",
    },
    "actions": {
        "id", "tenant_id", "run_id", "kind", "payload_json", "status",
        "approval_policy", "source_id", "source_event_id", "idempotency_key",
        "approved_by", "approved_at", "executed_at", "result_json",
        "last_error", "created_at", "updated_at",
    },
    "outbound_messages": {
        "id", "tenant_id", "channel", "destination_id", "text", "payload_json",
        "status", "priority", "run_after", "lease_owner", "lease_until",
        "attempts", "max_attempts", "idempotency_key", "correlation_id",
        "last_error", "sent_at", "created_at", "updated_at",
    },
    "session_mailbox": {
        "id", "session_id", "tenant_id", "source_id", "source_event_id",
        "action_id", "prompt", "status", "last_error", "result_json",
        "sent_at", "created_at", "updated_at",
    },
    "runtime_session_events": {
        "id", "session_id", "action_id", "direction", "payload_text",
        "marker", "created_at",
    },
    "runtime_session_context_snapshots": {
        "id", "session_id", "version", "source_event_id",
        "source_range_json", "summary", "keywords_json", "entities_json",
        "files_json", "cwd", "branch", "topic_key", "open_questions_json",
        "last_user_intent", "last_agent_state", "confidence", "created_at",
    },
    "runtime_session_route_decisions": {
        "id", "tenant_id", "source_id", "user_id", "preferred_kind",
        "fragment_text", "selected_session_id", "action", "confidence",
        "candidates_json", "reasons_json", "created_at",
    },
}


@dataclass(frozen=True, slots=True)
class SchemaIssue:
    object_name: str
    message: str


@dataclass(frozen=True, slots=True)
class PlatformSchemaReport:
    expected_migrations: list[str]
    applied_migrations: list[str]
    missing_migrations: list[str] = field(default_factory=list)
    issues: list[SchemaIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing_migrations and not self.issues


class PlatformSchemaValidationError(RuntimeError):
    def __init__(self, report: PlatformSchemaReport) -> None:
        self.report = report
        details = [
            *(f"missing migration: {version}" for version in report.missing_migrations),
            *(f"{issue.object_name}: {issue.message}" for issue in report.issues),
        ]
        super().__init__("platform schema is not compatible: " + "; ".join(details))


def _ensure_meta(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "  namespace TEXT NOT NULL,"
        "  version TEXT NOT NULL,"
        "  applied_at INTEGER NOT NULL,"
        "  PRIMARY KEY(namespace, version)"
        ")"
    )


def _applied(conn: sqlite3.Connection, namespace: str) -> set[str]:
    rows = conn.execute(
        "SELECT version FROM schema_migrations WHERE namespace = ?",
        (namespace,),
    ).fetchall()
    return {r["version"] for r in rows}


def apply_migrations_from_dir(
    conn: sqlite3.Connection,
    migration_dir: Path,
    *,
    namespace: str,
) -> list[str]:
    """Apply pending `*.sql` files in lexicographic order for one namespace."""
    _ensure_meta(conn)
    applied = _applied(conn, namespace)
    fresh: list[str] = []
    for path in sorted(migration_dir.glob("*.sql")):
        version = path.stem
        if version in applied:
            continue
        body = path.read_text()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executescript(body)
            conn.execute(
                "INSERT INTO schema_migrations(namespace, version, applied_at)"
                " VALUES (?, ?, ?)",
                (namespace, version, int(time.time())),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        fresh.append(version)
    return fresh


def apply_migrations(
    conn: sqlite3.Connection,
    provider: MigrationProvider,
    *,
    namespace: str,
) -> list[str]:
    """Apply pending migrations from a provider under a namespace."""
    with provider.migration_dir() as path:
        return apply_migrations_from_dir(conn, path, namespace=namespace)


def apply_app_migrations(
    conn: sqlite3.Connection,
    provider: MigrationProvider,
    *,
    namespace: str,
) -> list[str]:
    """Apply app-owned migrations; namespace must not collide with platform."""
    if namespace == "platform":
        raise ValueError("app migrations must not use the reserved 'platform' namespace")
    return apply_migrations(conn, provider, namespace=namespace)


def apply_platform_migrations(conn: sqlite3.Connection) -> list[str]:
    """Apply bundled platform migrations."""
    return apply_migrations(conn, PLATFORM_MIGRATION_PROVIDER, namespace=PLATFORM_NAMESPACE)


def expected_platform_migrations(
    provider: MigrationProvider = PLATFORM_MIGRATION_PROVIDER,
) -> list[str]:
    with provider.migration_dir() as path:
        return [item.stem for item in sorted(path.glob("*.sql"))]


def inspect_platform_schema(conn: sqlite3.Connection) -> PlatformSchemaReport:
    """Inspect whether the current SQLite schema can run platform modules."""
    expected = expected_platform_migrations()
    applied = _applied_if_meta_exists(conn, PLATFORM_NAMESPACE)
    missing = [version for version in expected if version not in applied]
    issues: list[SchemaIssue] = []
    for table, expected_columns in PLATFORM_TABLE_COLUMNS.items():
        existing = _table_columns(conn, table)
        if existing is None:
            issues.append(SchemaIssue(table, "missing table"))
            continue
        missing_columns = sorted(expected_columns - existing)
        if missing_columns:
            issues.append(SchemaIssue(
                table,
                "missing columns: " + ", ".join(missing_columns),
            ))
    return PlatformSchemaReport(
        expected_migrations=expected,
        applied_migrations=sorted(applied),
        missing_migrations=missing,
        issues=issues,
    )


def assert_platform_schema(conn: sqlite3.Connection) -> None:
    """Raise when the current SQLite schema is not compatible with the platform."""
    report = inspect_platform_schema(conn)
    if not report.ok:
        raise PlatformSchemaValidationError(report)


def _applied_if_meta_exists(conn: sqlite3.Connection, namespace: str) -> set[str]:
    if not _table_exists(conn, "schema_migrations"):
        return set()
    return _applied(conn, namespace)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str] | None:
    if not _table_exists(conn, table):
        return None
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row["name"] for row in rows}
