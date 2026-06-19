import pytest

from agent_platform.queue.durable import claim_due, enqueue, mark_done, mark_retry
from agent_platform.storage import bootstrap_platform_storage
from agent_platform.storage.migrations import (
    DirectoryMigrationProvider,
    PlatformSchemaValidationError,
    apply_app_migrations,
    apply_platform_migrations,
    assert_platform_schema,
    inspect_platform_schema,
)
from agent_platform.storage.sqlite import open_sqlite


def test_platform_migrations_are_namespaced_and_idempotent(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")

    first = apply_platform_migrations(conn)
    second = apply_platform_migrations(conn)

    assert first == [
        "001_event_queue",
        "002_agent_runs",
        "003_cron_jobs",
        "004_inbound_batches",
        "005_runtime_sessions",
        "006_actions_and_outbound",
        "007_session_routing",
    ]
    assert second == []
    rows = conn.execute(
        "SELECT namespace, version FROM schema_migrations ORDER BY version"
    ).fetchall()
    assert [(r["namespace"], r["version"]) for r in rows] == [
        ("platform", "001_event_queue"),
        ("platform", "002_agent_runs"),
        ("platform", "003_cron_jobs"),
        ("platform", "004_inbound_batches"),
        ("platform", "005_runtime_sessions"),
        ("platform", "006_actions_and_outbound"),
        ("platform", "007_session_routing"),
    ]


def test_app_migrations_use_separate_namespace(tmp_path):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "001_app_table.sql").write_text(
        "CREATE TABLE app_notes (id TEXT PRIMARY KEY);"
    )
    conn = open_sqlite(tmp_path / "app.db")

    applied = apply_app_migrations(
        conn,
        DirectoryMigrationProvider(migration_dir),
        namespace="poruchen",
    )
    second = apply_app_migrations(
        conn,
        DirectoryMigrationProvider(migration_dir),
        namespace="poruchen",
    )

    assert applied == ["001_app_table"]
    assert second == []
    row = conn.execute(
        "SELECT namespace, version FROM schema_migrations WHERE namespace = 'poruchen'"
    ).fetchone()
    assert (row["namespace"], row["version"]) == ("poruchen", "001_app_table")


def test_app_migrations_cannot_use_platform_namespace(tmp_path):
    with pytest.raises(ValueError, match="reserved"):
        apply_app_migrations(
            open_sqlite(tmp_path / "app.db"),
            DirectoryMigrationProvider(tmp_path),
            namespace="platform",
        )


def test_platform_schema_check_passes_after_platform_migrations(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    report = inspect_platform_schema(conn)

    assert report.ok
    assert report.missing_migrations == []
    assert report.issues == []
    assert_platform_schema(conn)


def test_bootstrap_platform_storage_applies_and_validates_schema(tmp_path):
    db_path = tmp_path / "app.db"

    applied = bootstrap_platform_storage(db_path)

    assert "001_event_queue" in applied
    conn = open_sqlite(db_path)
    try:
        assert_platform_schema(conn)
    finally:
        conn.close()


def test_platform_schema_check_reports_empty_database(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")

    report = inspect_platform_schema(conn)

    assert not report.ok
    assert "001_event_queue" in report.missing_migrations
    assert any(issue.object_name == "event_queue" for issue in report.issues)
    with pytest.raises(PlatformSchemaValidationError, match="missing migration"):
        assert_platform_schema(conn)


def test_platform_schema_check_reports_incompatible_existing_table(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    conn.execute("CREATE TABLE event_queue (id TEXT PRIMARY KEY)")

    report = inspect_platform_schema(conn)

    event_queue_issue = next(issue for issue in report.issues if issue.object_name == "event_queue")
    assert "tenant_id" in event_queue_issue.message
    assert "payload_json" in event_queue_issue.message


def test_durable_queue_lifecycle(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"batch_id": "b1"},
        idempotency_key="batch:b1",
        now=100,
    )
    duplicate_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"batch_id": "b1"},
        idempotency_key="batch:b1",
        now=100,
    )

    assert event_id is not None
    assert duplicate_id is None

    claimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    assert [row["id"] for row in claimed] == [event_id]
    assert claimed[0]["attempts"] == 1

    mark_retry(conn, event_id, run_after=150, last_error="boom", now=101)
    row = conn.execute("SELECT status, last_error FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "retrying"
    assert row["last_error"] == "boom"

    reclaimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=30,
        now=150,
    )
    assert [row["id"] for row in reclaimed] == [event_id]

    mark_done(conn, event_id, now=151)
    row = conn.execute("SELECT status, lease_owner, lease_until FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "done"
    assert row["lease_owner"] is None
    assert row["lease_until"] is None


def test_expired_lease_is_reclaimed(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="x",
        payload={},
        idempotency_key="x",
        now=100,
    )
    assert event_id is not None

    assert claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    claimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    )

    assert [row["id"] for row in claimed] == [event_id]
    assert claimed[0]["lease_owner"] == "worker-2"
    assert claimed[0]["attempts"] == 2


def test_retry_moves_to_dead_letter_after_max_attempts(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="x",
        payload={},
        idempotency_key="x",
        max_attempts=1,
        now=100,
    )
    assert event_id is not None

    claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    mark_retry(conn, event_id, run_after=110, last_error="nope", now=101)

    row = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "dead_letter"
