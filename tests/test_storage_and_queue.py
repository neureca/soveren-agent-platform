from agent_platform.queue.durable import claim_due, enqueue, mark_done, mark_retry
from agent_platform.storage.migrations import apply_platform_migrations
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
