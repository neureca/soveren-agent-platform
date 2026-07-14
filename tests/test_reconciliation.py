import asyncio
import json

import pytest

from soveren_agent_platform.actions.store import (
    insert_action,
    mark_executing,
)
from soveren_agent_platform.actions.store import (
    mark_uncertain as mark_action_uncertain,
)
from soveren_agent_platform.cron.store import (
    claim_due_jobs,
    insert_job,
    start_execution,
)
from soveren_agent_platform.cron.store import (
    mark_uncertain as mark_cron_uncertain,
)
from soveren_agent_platform.outbound.store import (
    claim_due as claim_outbound,
)
from soveren_agent_platform.outbound.store import (
    enqueue_outbound,
    mark_sending,
)
from soveren_agent_platform.outbound.store import (
    mark_uncertain as mark_outbound_uncertain,
)
from soveren_agent_platform.reconciliation import SQLiteEffectReconciler
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_action_reconciliation_retry_is_atomic_idempotent_and_tenant_fenced(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="external",
        payload={},
        approval_policy="auto",
        now=100,
    )
    assert mark_executing(
        conn,
        action_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=101,
    )
    assert mark_action_uncertain(
        conn,
        action_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        error="outcome unknown",
        now=102,
    )
    reconciler = SQLiteEffectReconciler._from_connection(conn)

    async def resolve():
        first = await reconciler.resolve_action(
            action_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            resolution="not_executed",
            request_key="operator-check-1",
            actor_id="operator-1",
            evidence={"provider_lookup": "not_found"},
        )
        duplicate = await reconciler.resolve_action(
            action_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            resolution="not_executed",
            request_key="operator-check-1",
            actor_id="operator-1",
            evidence={"provider_lookup": "not_found"},
        )
        return first, duplicate

    first, duplicate = asyncio.run(resolve())

    assert first.status == "approved"
    assert first.applied
    assert not duplicate.applied
    assert conn.execute("SELECT status FROM actions WHERE id = ?", (action_id,)).fetchone()[0] == "approved"
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM event_queue WHERE correlation_id = ? AND message_type = 'ExecuteAction'",
            (action_id,),
        ).fetchone()[0]
        == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM effect_reconciliations WHERE effect_id = ?",
            (action_id,),
        ).fetchone()[0]
        == 1
    )

    with pytest.raises(ValueError, match="different input"):
        asyncio.run(
            reconciler.resolve_action(
                action_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                resolution="executed",
                request_key="operator-check-1",
                actor_id="operator-1",
                evidence={"provider_lookup": "found"},
            )
        )
    with pytest.raises(KeyError, match="not found"):
        asyncio.run(
            reconciler.resolve_action(
                action_id,
                tenant_id="tenant-b",
                source_id="chat-1",
                resolution="executed",
                request_key="other-tenant-check",
                actor_id="operator-1",
                evidence={"provider_lookup": "found"},
            )
        )
    with pytest.raises(KeyError, match="not found"):
        asyncio.run(
            reconciler.resolve_action(
                action_id,
                tenant_id="tenant-a",
                source_id="chat-2",
                resolution="executed",
                request_key="other-conversation-check",
                actor_id="operator-1",
                evidence={"provider_lookup": "found"},
            )
        )


def test_outbound_reconciliation_preserves_payload_and_records_provider_evidence(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        payload={"parse_mode": "HTML"},
        idempotency_key="message-1",
        now=100,
    )
    assert message_id is not None
    claimed = claim_outbound(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    token = claimed[0]["lease_token"]
    assert mark_sending(conn, message_id, lease_token=token, now=101)
    assert mark_outbound_uncertain(
        conn,
        message_id,
        lease_token=token,
        last_error="timeout",
        now=102,
    )

    result = asyncio.run(
        SQLiteEffectReconciler._from_connection(conn).resolve_outbound(
            message_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            resolution="sent",
            request_key="provider-message-1",
            actor_id="operator-1",
            evidence={"external_id": "tg-42"},
            effect_at=103,
        )
    )
    row = conn.execute("SELECT * FROM outbound_messages WHERE id = ?", (message_id,)).fetchone()

    assert result.status == "sent"
    assert json.loads(row["payload_json"]) == {"parse_mode": "HTML"}
    assert json.loads(row["result_json"]) == {"external_id": "tg-42"}
    assert row["sent_at"] == 103


def test_cron_reconciliation_not_fired_requeues_explicitly(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="sync",
        payload={},
        run_at=100,
        now=90,
    )
    claimed = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    assert start_execution(conn, job_id, lease_token=claimed[0].lease_token, now=101)
    assert mark_cron_uncertain(
        conn,
        job_id,
        lease_token=claimed[0].lease_token,
        last_error="worker lost",
        now=102,
    )

    result = asyncio.run(
        SQLiteEffectReconciler._from_connection(conn).resolve_cron(
            job_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            resolution="not_fired",
            request_key="provider-job-1",
            actor_id="operator-1",
            evidence={"provider_lookup": "not_found"},
            retry_at=200,
        )
    )
    row = conn.execute("SELECT status, run_at, retry_at FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()

    assert result.status == "pending"
    assert row["status"] == "pending"
    assert row["run_at"] == 100
    assert row["retry_at"] == 200
    assert [
        job.id
        for job in claim_due_jobs(
            conn,
            limit=1,
            lease_owner="worker-2",
            lease_seconds=30,
            now=200,
        )
    ] == [job_id]


def test_cron_reconciliation_fired_preserves_finite_schedule_anchor(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    first_run = 100
    second_run = first_run + 24 * 60 * 60
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="twice",
        payload={},
        run_at=first_run,
        rrule="FREQ=DAILY;COUNT=2",
        now=90,
    )

    for index, scheduled_at in enumerate((first_run, second_run), start=1):
        claimed = claim_due_jobs(
            conn,
            limit=1,
            lease_owner="worker-1",
            lease_seconds=30,
            now=scheduled_at,
        )[0]
        assert start_execution(conn, job_id, lease_token=claimed.lease_token, now=scheduled_at)
        assert mark_cron_uncertain(
            conn,
            job_id,
            lease_token=claimed.lease_token,
            last_error="worker lost after dispatch",
            now=scheduled_at,
        )
        result = asyncio.run(
            SQLiteEffectReconciler._from_connection(conn).resolve_cron(
                job_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                resolution="fired",
                request_key=f"provider-job-{index}",
                actor_id="operator-1",
                evidence={"provider_lookup": "found"},
                effect_at=scheduled_at,
            )
        )

    row = conn.execute(
        "SELECT status, schedule_anchor_at, run_at FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert result.status == "fired"
    assert row["status"] == "fired"
    assert row["schedule_anchor_at"] == first_run
    assert row["run_at"] == second_run
