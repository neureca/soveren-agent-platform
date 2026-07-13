import asyncio

import pytest

from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.store import (
    approve_action,
    get_action,
    insert_action,
    mark_executed,
    mark_executing,
    mark_queued,
)
from soveren_agent_platform.actions.worker import process_action_event
from soveren_agent_platform.approvals.runtime import approve_action_and_enqueue
from soveren_agent_platform.batching import InboundMessage
from soveren_agent_platform.batching.store import append_inbound_message
from soveren_agent_platform.cron import store as cron_store
from soveren_agent_platform.outbound import store as outbound_store
from soveren_agent_platform.queue import durable
from soveren_agent_platform.sessions.mailbox import claim_next, enqueue_prompt, mark_accepted
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class MustNotExecute:
    async def execute(self, action):
        raise AssertionError("uncertain action must not be executed again")


def test_idempotency_keys_are_tenant_scoped_across_runtime_tables(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    event_a = durable.enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent",
        message_type="x",
        payload={},
        idempotency_key="same",
    )
    event_b = durable.enqueue(
        conn,
        tenant_id="tenant-b",
        recipient="agent",
        message_type="x",
        payload={},
        idempotency_key="same",
    )
    outbound_a = outbound_store.enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        channel="telegram",
        destination_id="1",
        text="a",
        idempotency_key="same",
    )
    outbound_b = outbound_store.enqueue_outbound(
        conn,
        tenant_id="tenant-b",
        source_id="chat-b",
        channel="telegram",
        destination_id="2",
        text="b",
        idempotency_key="same",
    )
    action_a, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        kind="x",
        payload={},
        idempotency_key="same",
    )
    action_b, _ = insert_action(
        conn,
        tenant_id="tenant-b",
        source_id="chat-b",
        kind="x",
        payload={},
        idempotency_key="same",
    )
    batch_a = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-a",
            raw_event_id="same",
            text="a",
            payload={},
            message_at=1,
        ),
    )
    batch_b = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-b",
            channel="telegram",
            source_id="chat-b",
            raw_event_id="same",
            text="b",
            payload={},
            message_at=1,
        ),
    )
    cron_a, _ = cron_store.insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        name="x",
        payload={},
        run_at=1,
        idempotency_key="same",
    )
    cron_b, _ = cron_store.insert_job(
        conn,
        tenant_id="tenant-b",
        source_id="chat-b",
        name="x",
        payload={},
        run_at=1,
        idempotency_key="same",
    )

    assert event_a is not None and event_b is not None and event_a != event_b
    assert outbound_a is not None and outbound_b is not None and outbound_a != outbound_b
    assert action_a != action_b
    assert batch_a is not None and batch_b is not None and batch_a != batch_b
    assert cron_a != cron_b


def test_action_access_and_approval_are_conversation_scoped(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        kind="x",
        payload={},
    )

    assert get_action(conn, action_id, tenant_id="tenant-b", source_id="chat-a") is None
    assert get_action(conn, action_id, tenant_id="tenant-a", source_id="chat-b") is None
    assert (
        approve_action(
            conn,
            action_id,
            tenant_id="tenant-a",
            source_id="chat-b",
            approver_id="attacker",
        )
        is False
    )
    with pytest.raises(KeyError, match="action not found"):
        approve_action_and_enqueue(
            conn,
            tenant_id="tenant-a",
            source_id="chat-b",
            action_id=action_id,
            approver_id="attacker",
        )
    assert (
        approve_action(
            conn,
            action_id,
            tenant_id="tenant-b",
            source_id="chat-a",
            approver_id="attacker",
        )
        is False
    )
    with pytest.raises(KeyError, match="action not found"):
        approve_action_and_enqueue(
            conn,
            tenant_id="tenant-b",
            source_id="chat-a",
            action_id=action_id,
            approver_id="attacker",
        )

    first = approve_action_and_enqueue(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        action_id=action_id,
        approver_id="owner",
    )
    second = approve_action_and_enqueue(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        action_id=action_id,
        approver_id="owner",
    )

    assert first.transitioned is True
    assert first.execution_event_created is True
    assert second.transitioned is False
    assert second.execution_event_created is False
    assert second.execution_event_id == first.execution_event_id
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM event_queue WHERE tenant_id = 'tenant-a' AND idempotency_key = ?",
            (f"execute-action:{action_id}",),
        ).fetchone()[0]
        == 1
    )


def test_effect_idempotency_keys_are_conversation_scoped_within_one_tenant(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    outbound_a = outbound_store.enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        channel="telegram",
        destination_id="chat-a",
        text="a",
        idempotency_key="same",
    )
    outbound_b = outbound_store.enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        channel="telegram",
        destination_id="chat-b",
        text="b",
        idempotency_key="same",
    )
    action_a, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        kind="x",
        payload={},
        idempotency_key="same",
    )
    action_b, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        kind="x",
        payload={},
        idempotency_key="same",
    )
    cron_a, _ = cron_store.insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        name="x",
        payload={},
        run_at=1,
        idempotency_key="same",
    )
    cron_b, _ = cron_store.insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        name="x",
        payload={},
        run_at=1,
        idempotency_key="same",
    )

    assert outbound_a is not None and outbound_b is not None and outbound_a != outbound_b
    assert action_a != action_b
    assert cron_a != cron_b


def test_mailbox_action_and_idempotency_keys_are_conversation_scoped(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_a = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        kind="codex_cli",
        backend="fake",
        backend_session_id="a",
    )
    session_b = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        kind="codex_cli",
        backend="fake",
        backend_session_id="b",
    )

    mailbox_a, created_a = enqueue_prompt(
        conn,
        session_id=session_a,
        tenant_id="tenant-a",
        source_id="chat-a",
        prompt="a",
        action_id="same-action",
        idempotency_key="same-key",
    )
    mailbox_b, created_b = enqueue_prompt(
        conn,
        session_id=session_b,
        tenant_id="tenant-a",
        source_id="chat-b",
        prompt="b",
        action_id="same-action",
        idempotency_key="same-key",
    )

    assert created_a and created_b
    assert mailbox_a != mailbox_b


def test_mailbox_claim_and_transition_require_conversation_scope(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        kind="codex_cli",
        backend="fake",
        backend_session_id="b",
    )
    mailbox_id, _ = enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-b",
        prompt="private-b",
    )

    assert claim_next(conn, session_id, tenant_id="tenant-a", source_id="chat-a") is None
    assert conn.execute("SELECT status FROM session_mailbox WHERE id = ?", (mailbox_id,)).fetchone()[0] == "queued"

    claimed = claim_next(conn, session_id, tenant_id="tenant-a", source_id="chat-b")
    assert claimed is not None
    mark_accepted(
        conn,
        mailbox_id,
        tenant_id="tenant-a",
        source_id="chat-a",
    )
    row = conn.execute(
        "SELECT status, accepted_at FROM session_mailbox WHERE id = ?",
        (mailbox_id,),
    ).fetchone()
    assert row["status"] == "sending"
    assert row["accepted_at"] is None


def test_event_queue_stale_lease_cannot_overwrite_new_owner(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    event_id = durable.enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent",
        message_type="x",
        payload={},
        idempotency_key="x",
        now=100,
    )
    assert event_id is not None
    first = durable.claim_due(
        conn,
        recipient="agent",
        limit=1,
        lease_owner="worker-a",
        lease_seconds=10,
        now=100,
    )[0]
    second = durable.claim_due(
        conn,
        recipient="agent",
        limit=1,
        lease_owner="worker-b",
        lease_seconds=10,
        now=111,
    )[0]

    assert (
        durable.mark_done(
            conn,
            event_id,
            lease_token=first["lease_token"],
            now=112,
        )
        is False
    )
    assert (
        durable.mark_retry(
            conn,
            event_id,
            lease_token=first["lease_token"],
            run_after=200,
            last_error="stale",
            now=112,
        )
        is None
    )
    row = conn.execute("SELECT status, lease_token FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert row["status"] == "leased"
    assert row["lease_token"] == second["lease_token"]
    assert (
        durable.mark_done(
            conn,
            event_id,
            lease_token=second["lease_token"],
            now=113,
        )
        is True
    )


def test_outbound_and_cron_stale_leases_are_fenced(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = outbound_store.enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="1",
        text="hello",
        idempotency_key="message",
        now=100,
    )
    assert message_id is not None
    outbound_first = outbound_store.claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-a",
        lease_seconds=10,
        now=100,
    )[0]
    outbound_second = outbound_store.claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-b",
        lease_seconds=10,
        now=111,
    )[0]
    assert (
        outbound_store.mark_sent(
            conn,
            message_id,
            lease_token=outbound_first["lease_token"],
            now=112,
        )
        is False
    )
    assert (
        outbound_store.mark_retry(
            conn,
            message_id,
            lease_token=outbound_first["lease_token"],
            run_after=200,
            last_error="stale",
            now=112,
        )
        is None
    )
    assert outbound_store.mark_sending(
        conn,
        message_id,
        lease_token=outbound_second["lease_token"],
        now=112,
    )
    assert (
        outbound_store.mark_sent(
            conn,
            message_id,
            lease_token=outbound_second["lease_token"],
            now=112,
        )
        is True
    )

    job_id, _ = cron_store.insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="job",
        payload={},
        run_at=100,
        now=90,
    )
    cron_first = cron_store.claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-a",
        lease_seconds=10,
        now=100,
    )[0]
    cron_second = cron_store.claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-b",
        lease_seconds=10,
        now=111,
    )[0]
    assert (
        cron_store.complete_job(
            conn,
            job_id,
            lease_token=cron_first.lease_token,
            fired_at=112,
        )
        is False
    )
    assert (
        cron_store.fail_job(
            conn,
            job_id,
            lease_token=cron_first.lease_token,
            retry_at=200,
            last_error="stale",
            now=112,
        )
        is False
    )
    assert cron_store.start_execution(
        conn,
        job_id,
        lease_token=cron_second.lease_token,
        now=112,
    )
    assert (
        cron_store.complete_job(
            conn,
            job_id,
            lease_token=cron_second.lease_token,
            fired_at=112,
        )
        is True
    )


def test_reclaimed_executing_action_becomes_uncertain_without_reexecution(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="external-effect",
        payload={},
        approval_policy="auto",
    )
    event_id = durable.enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id, "source_id": "chat-1"},
        idempotency_key="execute",
        now=100,
    )
    assert event_id is not None
    durable.claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="worker-a",
        lease_seconds=10,
        now=100,
    )
    assert mark_executing(
        conn,
        action_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=100,
    )
    reclaimed = durable.claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="worker-b",
        lease_seconds=10,
        now=111,
    )[0]

    asyncio.run(
        process_action_event(
            conn,
            reclaimed,
            registry=ActionRegistry({"external-effect": MustNotExecute()}),
        )
    )

    action = get_action(conn, action_id, tenant_id="tenant-a", source_id="chat-1")
    event = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert action is not None and action["status"] == "uncertain"
    assert event["status"] == "done"
    assert (
        mark_executed(
            conn,
            action_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            result={"late": True},
        )
        is False
    )


def test_reclaimed_successfully_queued_action_is_not_dispatched_twice(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="external-effect",
        payload={},
        approval_policy="auto",
    )
    event_id = durable.enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id, "source_id": "chat-1"},
        idempotency_key="execute-queued",
        now=100,
    )
    assert event_id is not None
    durable.claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="worker-a",
        lease_seconds=10,
        now=100,
    )
    assert mark_executing(
        conn,
        action_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=100,
    )
    assert mark_queued(
        conn,
        action_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        result={"downstream_id": "job-1"},
        now=101,
    )
    reclaimed = durable.claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="worker-b",
        lease_seconds=10,
        now=111,
    )[0]

    asyncio.run(
        process_action_event(
            conn,
            reclaimed,
            registry=ActionRegistry({"external-effect": MustNotExecute()}),
        )
    )

    action = get_action(conn, action_id, tenant_id="tenant-a", source_id="chat-1")
    event = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert action is not None and action["status"] == "queued"
    assert event["status"] == "done"
    assert (
        mark_executed(
            conn,
            action_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            result={"completed": True},
        )
        is True
    )
