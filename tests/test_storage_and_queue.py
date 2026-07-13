import asyncio
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import soveren_agent_platform.storage.migrations.runner as migration_runner
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.queue.durable import claim_due, enqueue, mark_done, mark_retry, renew_lease
from soveren_agent_platform.storage import bootstrap_platform_storage
from soveren_agent_platform.storage.migrations import (
    DirectoryMigrationProvider,
    PlatformSchemaValidationError,
    apply_app_migrations,
    apply_migrations_from_dir,
    apply_platform_migrations,
    assert_platform_schema,
    inspect_platform_schema,
)
from soveren_agent_platform.storage.sqlite import open_sqlite


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
        "008_telegram_chat_registrations",
        "009_memory_records",
        "010_mailbox_delivery_state",
        "011_tenant_idempotency_and_lease_fencing",
        "012_effect_execution_fencing",
        "013_planner_run_fencing",
        "014_effect_reconciliation",
        "015_conversation_privacy",
        "016_cron_retry_schedule",
        "017_idempotency_fingerprints",
    ]
    assert second == []
    for table in ("actions", "outbound_messages", "cron_jobs", "memory_records", "effect_reconciliations"):
        columns = {row["name"]: row for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        assert columns["source_id"]["notnull"] == 1
    rows = conn.execute("SELECT namespace, version FROM schema_migrations ORDER BY version").fetchall()
    assert [(r["namespace"], r["version"]) for r in rows] == [
        ("platform", "001_event_queue"),
        ("platform", "002_agent_runs"),
        ("platform", "003_cron_jobs"),
        ("platform", "004_inbound_batches"),
        ("platform", "005_runtime_sessions"),
        ("platform", "006_actions_and_outbound"),
        ("platform", "007_session_routing"),
        ("platform", "008_telegram_chat_registrations"),
        ("platform", "009_memory_records"),
        ("platform", "010_mailbox_delivery_state"),
        ("platform", "011_tenant_idempotency_and_lease_fencing"),
        ("platform", "012_effect_execution_fencing"),
        ("platform", "013_planner_run_fencing"),
        ("platform", "014_effect_reconciliation"),
        ("platform", "015_conversation_privacy"),
        ("platform", "016_cron_retry_schedule"),
        ("platform", "017_idempotency_fingerprints"),
    ]


def test_mailbox_delivery_migration_upgrades_existing_database_without_losing_rows(tmp_path):
    old_migrations = tmp_path / "old-migrations"
    old_migrations.mkdir()
    migration_source = (
        Path(__file__).parents[1] / "src" / "soveren_agent_platform" / "storage" / "migrations" / "platform"
    )
    for migration in sorted(migration_source.glob("00[1-9]_*.sql")):
        shutil.copy(migration, old_migrations / migration.name)
    conn = open_sqlite(tmp_path / "app.db")
    apply_migrations_from_dir(conn, old_migrations, namespace="platform")
    conn.execute(
        "INSERT INTO runtime_sessions"
        " (id, tenant_id, source_id, kind, backend, backend_session_id, title, cwd, status,"
        "  metadata_json, created_at, updated_at)"
        " VALUES ('rs_1', 'tenant-a', 'chat-1', 'codex_cli', 'codex', 'thread-1', '', '',"
        " 'idle', '{}', 1, 1)"
    )
    conn.execute(
        "INSERT INTO session_mailbox"
        " (id, session_id, tenant_id, source_id, prompt, status, created_at, updated_at)"
        " VALUES ('sm_1', 'rs_1', 'tenant-a', 'chat-1', 'existing', 'queued', 1, 1)"
    )

    applied = apply_platform_migrations(conn)
    row = conn.execute("SELECT * FROM session_mailbox WHERE id = 'sm_1'").fetchone()

    assert applied == [
        "010_mailbox_delivery_state",
        "011_tenant_idempotency_and_lease_fencing",
        "012_effect_execution_fencing",
        "013_planner_run_fencing",
        "014_effect_reconciliation",
        "015_conversation_privacy",
        "016_cron_retry_schedule",
        "017_idempotency_fingerprints",
    ]
    assert row["prompt"] == "existing"
    assert row["accepted_at"] is None
    assert row["attempts"] == 0
    assert row["max_attempts"] == 3
    assert row["idempotency_key"] is None


def test_tenant_fencing_migration_preserves_existing_runtime_rows(tmp_path):
    old_migrations = tmp_path / "old-migrations"
    old_migrations.mkdir()
    migration_source = (
        Path(__file__).parents[1] / "src" / "soveren_agent_platform" / "storage" / "migrations" / "platform"
    )
    for migration in sorted(migration_source.glob("*.sql")):
        if not migration.name.startswith(("011_", "012_", "013_", "014_", "015_", "016_", "017_")):
            shutil.copy(migration, old_migrations / migration.name)
    conn = open_sqlite(tmp_path / "app.db")
    apply_migrations_from_dir(conn, old_migrations, namespace="platform")
    conn.execute(
        "INSERT INTO event_queue"
        " (id, tenant_id, recipient, message_type, payload_json, status, run_after,"
        " idempotency_key, created_at, updated_at)"
        " VALUES ('evt_old', 'tenant-a', 'agent', 'x', '{}', 'queued', 1, 'event-key', 1, 1)"
    )
    conn.execute(
        "INSERT INTO event_queue"
        " (id, tenant_id, recipient, message_type, payload_json, status, run_after,"
        " idempotency_key, correlation_id, created_at, updated_at)"
        " VALUES ('evt_handoff', 'tenant-a', 'actions', 'ExecuteAction', '{}', 'done', 1,"
        " 'handoff-event-key', 'act_handoff', 1, 1)"
    )
    conn.execute(
        "INSERT INTO actions"
        " (id, tenant_id, kind, payload_json, status, approval_policy, idempotency_key,"
        " created_at, updated_at)"
        " VALUES ('act_old', 'tenant-a', 'x', '{}', 'approved', 'auto', 'action-key', 1, 1)"
    )
    conn.execute(
        "INSERT INTO actions"
        " (id, tenant_id, kind, payload_json, status, approval_policy, idempotency_key,"
        " last_error, created_at, updated_at)"
        " VALUES ('act_handoff', 'tenant-a', 'x', '{}', 'queued', 'auto',"
        " 'handoff-action-key', 'old retry', 1, 1)"
    )
    conn.execute(
        "INSERT INTO outbound_messages"
        " (id, tenant_id, channel, destination_id, text, payload_json, status, run_after,"
        " idempotency_key, created_at, updated_at)"
        " VALUES ('out_old', 'tenant-a', 'telegram', '1', 'hello', '{}', 'queued', 1,"
        " 'outbound-key', 1, 1)"
    )
    conn.execute(
        "INSERT INTO inbound_batches"
        " (id, tenant_id, channel, source_id, status, first_message_at, last_message_at,"
        " message_count, created_at, updated_at)"
        " VALUES ('batch_old', 'tenant-a', 'telegram', '1', 'collecting', 1, 1, 1, 1, 1)"
    )
    conn.execute(
        "INSERT INTO inbound_batch_messages"
        " (id, batch_id, tenant_id, channel, source_id, raw_event_id, payload_json,"
        " message_at, created_at)"
        " VALUES ('message_old', 'batch_old', 'tenant-a', 'telegram', '1', 'raw-key', '{}', 1, 1)"
    )
    conn.execute(
        "INSERT INTO cron_jobs"
        " (id, tenant_id, name, payload_json, status, run_at, created_at, updated_at)"
        " VALUES ('cron_old', 'tenant-a', 'job', '{}', 'pending', 1, 1, 1)"
    )

    assert apply_platform_migrations(conn) == [
        "011_tenant_idempotency_and_lease_fencing",
        "012_effect_execution_fencing",
        "013_planner_run_fencing",
        "014_effect_reconciliation",
        "015_conversation_privacy",
        "016_cron_retry_schedule",
        "017_idempotency_fingerprints",
    ]

    assert conn.execute("SELECT payload_json FROM event_queue WHERE id = 'evt_old'").fetchone()[0] == "{}"
    assert conn.execute("SELECT status FROM actions WHERE id = 'act_old'").fetchone()[0] == "approved"
    assert (
        conn.execute("SELECT source_id FROM actions WHERE id = 'act_old'")
        .fetchone()[0]
        .startswith("__legacy_unscoped__:")
    )
    assert conn.execute("SELECT last_error FROM actions WHERE id = 'act_handoff'").fetchone()[0] is None
    assert conn.execute("SELECT text FROM outbound_messages WHERE id = 'out_old'").fetchone()[0] == "hello"
    assert (
        conn.execute("SELECT raw_event_id FROM inbound_batch_messages WHERE id = 'message_old'").fetchone()[0]
        == "raw-key"
    )
    assert conn.execute("SELECT lease_token FROM cron_jobs WHERE id = 'cron_old'").fetchone()[0] is None
    assert conn.execute("SELECT retry_at FROM cron_jobs WHERE id = 'cron_old'").fetchone()[0] is None
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_app_migrations_use_separate_namespace(tmp_path):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "001_app_table.sql").write_text("CREATE TABLE app_notes (id TEXT PRIMARY KEY);")
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
    row = conn.execute("SELECT namespace, version FROM schema_migrations WHERE namespace = 'poruchen'").fetchone()
    assert (row["namespace"], row["version"]) == ("poruchen", "001_app_table")


def test_concurrent_migrators_recheck_version_after_acquiring_write_lock(tmp_path, monkeypatch):
    migration_dir = tmp_path / "race-migrations"
    migration_dir.mkdir()
    (migration_dir / "001_shared.sql").write_text("CREATE TABLE shared_table (id TEXT PRIMARY KEY);")
    db_path = tmp_path / "app.db"
    barrier = threading.Barrier(2)
    original_applied = migration_runner._applied

    def synchronized_applied(conn, namespace):
        result = original_applied(conn, namespace)
        if namespace == "race":
            barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(migration_runner, "_applied", synchronized_applied)

    def migrate() -> list[str]:
        conn = open_sqlite(db_path)
        try:
            return apply_migrations_from_dir(conn, migration_dir, namespace="race")
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result(timeout=5) for future in [executor.submit(migrate), executor.submit(migrate)]]

    assert sorted(results, key=len) == [[], ["001_shared"]]
    conn = open_sqlite(db_path)
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE namespace = 'race' AND version = '001_shared'"
        ).fetchone()[0]
        == 1
    )


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

    applied = asyncio.run(bootstrap_platform_storage(db_path))

    assert "001_event_queue" in applied
    conn = open_sqlite(db_path)
    try:
        assert_platform_schema(conn)
    finally:
        conn.close()


def test_bootstrap_baselines_compatible_platform_schema_without_history(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    expected = apply_platform_migrations(conn)
    conn.execute("DELETE FROM schema_migrations WHERE namespace = 'platform'")
    conn.close()

    baselined = asyncio.run(bootstrap_platform_storage(db_path))

    assert baselined == expected
    conn = open_sqlite(db_path)
    try:
        assert_platform_schema(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE namespace = 'platform'"
        ).fetchone()[0] == len(expected)
    finally:
        conn.close()


def test_bootstrap_rejects_partial_platform_schema_before_running_migrations(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    conn.execute("CREATE TABLE event_queue (id TEXT PRIMARY KEY)")
    conn.close()

    with pytest.raises(PlatformSchemaValidationError, match="platform schema is not compatible"):
        asyncio.run(bootstrap_platform_storage(db_path))

    conn = open_sqlite(db_path)
    try:
        assert {row["name"] for row in conn.execute("PRAGMA table_info(event_queue)")} == {"id"}
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE namespace = 'platform'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_platform_schema_check_reports_missing_runtime_index(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    conn.execute("DROP INDEX idx_agent_runs_tenant_operation")

    report = inspect_platform_schema(conn)

    assert not report.ok
    assert any(
        issue.object_name == "idx_agent_runs_tenant_operation" and issue.message == "missing index"
        for issue in report.issues
    )


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
        run_after=100,
        now=100,
    )
    duplicate_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"batch_id": "b1"},
        idempotency_key="batch:b1",
        run_after=100,
        now=100,
    )

    assert event_id is not None
    assert duplicate_id is None
    with pytest.raises(IdempotencyConflictError):
        enqueue(
            conn,
            tenant_id="tenant-a",
            recipient="agent_core",
            message_type="ChatBatchReady",
            payload={"batch_id": "different"},
            idempotency_key="batch:b1",
            run_after=100,
            now=100,
        )

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

    mark_retry(
        conn,
        event_id,
        lease_token=claimed[0]["lease_token"],
        run_after=150,
        last_error="boom",
        now=101,
    )
    row = conn.execute("SELECT status, last_error FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "retrying"
    assert row["last_error"] == "boom"
    assert (
        enqueue(
            conn,
            tenant_id="tenant-a",
            recipient="agent_core",
            message_type="ChatBatchReady",
            payload={"batch_id": "b1"},
            idempotency_key="batch:b1",
            run_after=100,
            now=102,
        )
        is None
    )
    with pytest.raises(IdempotencyConflictError):
        enqueue(
            conn,
            tenant_id="tenant-a",
            recipient="agent_core",
            message_type="ChatBatchReady",
            payload={"batch_id": "b1"},
            idempotency_key="batch:b1",
            run_after=101,
            now=102,
        )

    reclaimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=30,
        now=150,
    )
    assert [row["id"] for row in reclaimed] == [event_id]

    mark_done(
        conn,
        event_id,
        lease_token=reclaimed[0]["lease_token"],
        now=151,
    )
    row = conn.execute("SELECT status, lease_owner, lease_until FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "done"
    assert row["lease_owner"] is None
    assert row["lease_until"] is None


def test_legacy_queue_replay_survives_retry_schedule_change(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"batch_id": "b1"},
        idempotency_key="legacy:b1",
        run_after=100,
        now=90,
    )
    assert event_id is not None
    conn.execute(
        "UPDATE event_queue SET idempotency_fingerprint = NULL WHERE id = ?",
        (event_id,),
    )
    claimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    assert mark_retry(
        conn,
        event_id,
        lease_token=claimed[0]["lease_token"],
        run_after=150,
        last_error="retry",
        now=101,
    ) == "retrying"

    assert enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        payload={"batch_id": "b1"},
        idempotency_key="legacy:b1",
        run_after=100,
        now=102,
    ) is None
    with pytest.raises(IdempotencyConflictError):
        enqueue(
            conn,
            tenant_id="tenant-a",
            recipient="agent_core",
            message_type="ChatBatchReady",
            payload={"batch_id": "different"},
            idempotency_key="legacy:b1",
            run_after=100,
            now=102,
        )


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


def test_active_lease_can_be_renewed_but_expired_or_stale_token_cannot(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="x",
        payload={},
        idempotency_key="renew-x",
        now=100,
    )
    assert event_id is not None
    claimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    token = claimed[0]["lease_token"]

    assert renew_lease(conn, event_id, lease_token=token, lease_seconds=10, now=105)
    assert not renew_lease(conn, event_id, lease_token="stale", lease_seconds=10, now=106)
    assert not renew_lease(conn, event_id, lease_token=token, lease_seconds=10, now=116)


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

    claimed = claim_due(
        conn,
        recipient="agent_core",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    mark_retry(
        conn,
        event_id,
        lease_token=claimed[0]["lease_token"],
        run_after=110,
        last_error="nope",
        now=101,
    )

    row = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "dead_letter"
