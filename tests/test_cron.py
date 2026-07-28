import asyncio
import sqlite3
from datetime import datetime, timezone

import pytest

from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.cron.store import (
    cancel_scheduled_job,
    claim_due_jobs,
    dispatch_due_event,
    insert_job,
    list_scheduled_jobs,
    renew_lease,
)
from soveren_agent_platform.cron.worker import run_cron_store_worker, run_cron_worker
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_cron_event_dispatch_atomically_enqueues_and_advances_recurring_job(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, created = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily-reminder",
        payload={"text": "Stand up"},
        run_at=100,
        rrule="FREQ=DAILY",
        now=90,
    )
    assert created is True
    claimed = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )[0]

    assert dispatch_due_event(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        recipient="agent",
        fired_at=101,
    )

    job = conn.execute(
        "SELECT status, run_at, attempts, lease_token FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    event = conn.execute(
        "SELECT tenant_id, recipient, message_type, status, payload_json"
        " FROM event_queue WHERE correlation_id = ?",
        (job_id,),
    ).fetchone()
    assert tuple(job) == ("pending", 86500, 0, None)
    assert tuple(event)[:4] == ("tenant-a", "agent", "CronJobDue", "queued")
    assert '"source_id": "chat-1"' in event["payload_json"]


def test_one_shot_dispatch_marks_job_fired_and_stale_token_cannot_repeat_it(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="reminder",
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
    )[0]

    assert dispatch_due_event(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        recipient="agent",
        fired_at=101,
    )
    assert not dispatch_due_event(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        recipient="agent",
        fired_at=102,
    )

    assert conn.execute(
        "SELECT status FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()["status"] == "fired"
    assert conn.execute(
        "SELECT COUNT(*) FROM event_queue WHERE correlation_id = ?",
        (job_id,),
    ).fetchone()[0] == 1


def test_cron_event_dispatch_rolls_back_event_and_schedule_together(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="reminder",
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
    )[0]
    conn.execute(
        "CREATE TRIGGER reject_cron_event BEFORE INSERT ON event_queue"
        " WHEN NEW.message_type = 'CronJobDue'"
        " BEGIN SELECT RAISE(ABORT, 'injected dispatch failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="injected dispatch failure"):
        dispatch_due_event(
            conn,
            job_id,
            lease_token=claimed.lease_token,
            recipient="agent",
            fired_at=101,
        )

    job = conn.execute(
        "SELECT status, run_at, lease_token FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert tuple(job) == ("leased", 100, claimed.lease_token)
    assert conn.execute(
        "SELECT COUNT(*) FROM event_queue WHERE correlation_id = ?",
        (job_id,),
    ).fetchone()[0] == 0


def test_cron_job_is_reclaimed_when_worker_stops_before_atomic_dispatch(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="reminder",
        payload={},
        run_at=100,
        now=90,
    )
    first = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )[0]
    second = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    )[0]

    assert second.id == job_id
    assert second.lease_token != first.lease_token
    assert not dispatch_due_event(
        conn,
        job_id,
        lease_token=first.lease_token,
        recipient="agent",
        fired_at=112,
    )


def test_expired_cron_lease_is_dead_lettered_after_max_attempts(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="one-shot",
        payload={},
        run_at=100,
        max_attempts=1,
        now=90,
    )
    assert claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )

    assert claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    ) == []
    row = conn.execute(
        "SELECT status, attempts, lease_token, last_error FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert row["status"] == "dead_letter"
    assert row["attempts"] == 1
    assert row["lease_token"] is None
    assert row["last_error"] == "cron lease expired after the maximum number of attempts"


def test_tenant_scoped_claim_fences_selection_and_expired_cleanup(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    jobs: dict[str, str] = {}
    for tenant_id in ("tenant-a", "tenant-b"):
        for kind in ("due", "exhausted"):
            jobs[f"{tenant_id}:{kind}"] = insert_job(
                conn,
                tenant_id=tenant_id,
                source_id=f"chat-{tenant_id[-1]}",
                name=f"{kind}-{tenant_id}",
                payload={},
                run_at=100,
                max_attempts=1,
                now=90,
            )[0]
        conn.execute(
            "UPDATE cron_jobs SET status = 'leased', attempts = 1,"
            " lease_owner = 'old', lease_until = 99, lease_token = 'old-token'"
            " WHERE id = ?",
            (jobs[f"{tenant_id}:exhausted"],),
        )

    claimed = claim_due_jobs(
        conn,
        tenant_id="tenant-a",
        limit=10,
        lease_owner="tenant-a-worker",
        lease_seconds=30,
        now=100,
    )

    assert [(job.tenant_id, job.name) for job in claimed] == [("tenant-a", "due-tenant-a")]
    statuses = {
        row["name"]: row["status"]
        for row in conn.execute("SELECT name, status FROM cron_jobs")
    }
    assert statuses == {
        "due-tenant-a": "leased",
        "exhausted-tenant-a": "dead_letter",
        "due-tenant-b": "pending",
        "exhausted-tenant-b": "leased",
    }


def test_scheduled_job_listing_is_conversation_scoped_and_active_only(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    visible_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="visible",
        payload={},
        run_at=200,
        now=90,
    )
    hidden_ids = [
        insert_job(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            name=name,
            payload={},
            run_at=run_at,
            now=90,
        )[0]
        for tenant_id, source_id, name, run_at in (
            ("tenant-a", "chat-2", "other-chat", 100),
            ("tenant-b", "chat-1", "other-tenant", 50),
            ("tenant-a", "chat-1", "finished", 75),
        )
    ]
    conn.execute("UPDATE cron_jobs SET status = 'fired' WHERE id = ?", (hidden_ids[-1],))

    jobs = list_scheduled_jobs(conn, tenant_id="tenant-a", source_id="chat-1")

    assert [(job.id, job.name, job.status) for job in jobs] == [
        (visible_id, "visible", "pending"),
    ]


def test_cancel_scheduled_job_fences_conversation_and_stops_unstarted_work(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="reminder",
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
    )[0]

    assert cancel_scheduled_job(
        conn,
        job_id,
        tenant_id="tenant-a",
        source_id="chat-2",
        now=101,
    ).outcome == "not_found"
    assert cancel_scheduled_job(
        conn,
        job_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=102,
    ).outcome == "cancelled"
    assert not renew_lease(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        lease_seconds=30,
        now=103,
    )
    row = conn.execute(
        "SELECT status, lease_owner, lease_until, lease_token FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert tuple(row) == ("cancelled", None, None, None)
    assert cancel_scheduled_job(
        conn,
        job_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=104,
    ).outcome == "already_cancelled"


def test_cancel_after_recurring_dispatch_stops_future_run_and_reports_current_event(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily-reminder",
        payload={},
        run_at=100,
        rrule="FREQ=DAILY",
        now=90,
    )
    claimed = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )[0]
    assert dispatch_due_event(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        recipient="agent",
        fired_at=101,
    )

    cancellation = cancel_scheduled_job(
        conn,
        job_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        now=102,
    )

    assert cancellation.outcome == "current_run_may_complete"
    assert conn.execute(
        "SELECT status FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()["status"] == "cancelled"
    assert conn.execute(
        "SELECT status FROM event_queue WHERE correlation_id = ?",
        (job_id,),
    ).fetchone()["status"] == "queued"


def test_finite_recurring_cron_uses_immutable_schedule_anchor(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    scheduled_runs = [
        int(datetime(2026, 1, day, 9, tzinfo=timezone.utc).timestamp())
        for day in (1, 2, 3)
    ]
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="three-digests",
        payload={},
        run_at=scheduled_runs[0],
        rrule="FREQ=DAILY;COUNT=3",
        now=scheduled_runs[0] - 60,
    )

    for scheduled_at in scheduled_runs:
        claimed = claim_due_jobs(
            conn,
            limit=1,
            lease_owner="worker-1",
            lease_seconds=60,
            now=scheduled_at,
        )
        assert [job.run_at for job in claimed] == [scheduled_at]
        assert dispatch_due_event(
            conn,
            job_id,
            lease_token=claimed[0].lease_token,
            recipient="agent",
            fired_at=scheduled_at,
        )

    row = conn.execute(
        "SELECT status, schedule_anchor_at, run_at FROM cron_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert tuple(row) == ("fired", scheduled_runs[0], scheduled_runs[-1])


def test_legacy_cron_replay_survives_recurring_schedule_advance(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    scheduled_at = int(datetime(2026, 1, 1, 9, tzinfo=timezone.utc).timestamp())
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"kind": "digest"},
        run_at=scheduled_at,
        rrule="FREQ=DAILY",
        idempotency_key="legacy-daily",
        now=scheduled_at - 60,
    )
    conn.execute(
        "UPDATE cron_jobs SET idempotency_fingerprint = NULL WHERE id = ?",
        (job_id,),
    )
    claimed = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=60,
        now=scheduled_at,
    )[0]
    assert dispatch_due_event(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        recipient="agent",
        fired_at=scheduled_at,
    )

    assert insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"kind": "digest"},
        run_at=scheduled_at,
        rrule="FREQ=DAILY",
        idempotency_key="legacy-daily",
    ) == (job_id, False)
    with pytest.raises(IdempotencyConflictError):
        insert_job(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            name="daily_digest",
            payload={"kind": "different"},
            run_at=scheduled_at,
            rrule="FREQ=DAILY",
            idempotency_key="legacy-daily",
        )


def test_cron_worker_publishes_due_event(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"chat_id": 1},
        run_at=100,
        now=90,
    )
    conn.close()

    async def run() -> None:
        stop_event = asyncio.Event()
        worker = asyncio.create_task(
            run_cron_worker(
                db_path,
                stop_event,
                tenant_id="tenant-a",
                poll_interval_s=0.01,
            )
        )
        for _ in range(100):
            check = open_sqlite(db_path)
            published = check.execute(
                "SELECT COUNT(*) FROM event_queue WHERE correlation_id = ?",
                (job_id,),
            ).fetchone()[0]
            check.close()
            if published:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("cron worker did not publish the due event")
        stop_event.set()
        await worker

    asyncio.run(run())


class FakeCronStore:
    def __init__(self) -> None:
        self.jobs = [
            CronJob(
                id="cron_1",
                tenant_id="tenant-a",
                source_id="chat-1",
                name="daily_digest",
                payload={"chat_id": 1},
                run_at=100,
                rrule=None,
                timezone="UTC",
                attempts=1,
                lease_token="lease-1",
            )
        ]
        self.dispatched: list[tuple[str, str]] = []
        self.claim_tenant_ids: list[str | None] = []
        self.stop_event: asyncio.Event | None = None

    async def claim_due(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        tenant_id: str | None = None,
    ) -> list[CronJob]:
        self.claim_tenant_ids.append(tenant_id)
        claimed, self.jobs = self.jobs[:limit], self.jobs[limit:]
        return claimed

    async def renew_lease(
        self,
        job_id: str,
        *,
        lease_token: str,
        lease_seconds: int,
    ) -> bool:
        return True

    async def dispatch_due_event(
        self,
        job_id: str,
        *,
        lease_token: str,
        recipient: str,
    ) -> bool:
        self.dispatched.append((job_id, recipient))
        assert self.stop_event is not None
        self.stop_event.set()
        return True


def test_cron_store_worker_uses_atomic_dispatch_port():
    async def run() -> FakeCronStore:
        stop_event = asyncio.Event()
        store = FakeCronStore()
        store.stop_event = stop_event
        await asyncio.wait_for(
            run_cron_store_worker(
                store,
                stop_event,
                tenant_id="tenant-a",
                recipient="custom-agent",
                poll_interval_s=0.01,
            ),
            timeout=1,
        )
        return store

    store = asyncio.run(run())

    assert store.dispatched == [("cron_1", "custom-agent")]
    assert store.claim_tenant_ids == ["tenant-a"]


@pytest.mark.parametrize(
    ("batch_size", "lease_seconds", "tenant_id", "recipient", "message"),
    [
        (0, 60, None, "agent", "batch_size must be positive"),
        (1, 0, None, "agent", "lease_seconds must be positive"),
        (1, 60, " ", "agent", "tenant_id must be non-empty when provided"),
        (1, 60, None, " ", "recipient must be a non-empty string"),
    ],
)
def test_cron_store_worker_rejects_invalid_settings(
    batch_size,
    lease_seconds,
    tenant_id,
    recipient,
    message,
):
    async def run() -> None:
        stop_event = asyncio.Event()
        with pytest.raises(ValueError, match=message):
            await run_cron_store_worker(
                FakeCronStore(),
                stop_event,
                batch_size=batch_size,
                lease_seconds=lease_seconds,
                tenant_id=tenant_id,
                recipient=recipient,
            )

    asyncio.run(run())


def test_cron_rejects_invalid_schedule_before_insert(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    with pytest.raises(ValueError, match="rrule"):
        insert_job(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            name="broken",
            payload={},
            run_at=100,
            rrule="not an rrule",
        )

    assert conn.execute("SELECT COUNT(*) FROM cron_jobs").fetchone()[0] == 0


def test_cron_idempotency_replay_rejects_different_schedule(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    first = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily",
        payload={"kind": "digest"},
        run_at=100,
        idempotency_key="daily-1",
    )

    assert insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily",
        payload={"kind": "digest"},
        run_at=100,
        idempotency_key="daily-1",
    ) == (first[0], False)
    with pytest.raises(IdempotencyConflictError):
        insert_job(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            name="daily",
            payload={"kind": "digest"},
            run_at=200,
            idempotency_key="daily-1",
        )
