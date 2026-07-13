import asyncio
from datetime import datetime, timezone

import pytest

from soveren_agent_platform.cron.contracts import CronJob
from soveren_agent_platform.cron.queue_handler import QueueCronHandler
from soveren_agent_platform.cron.store import (
    claim_due_jobs,
    complete_job,
    fail_job,
    insert_job,
    start_execution,
)
from soveren_agent_platform.cron.worker import run_cron_store_worker, run_cron_worker
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class RecordingCronHandler:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.jobs: list[CronJob] = []

    async def handle(self, job: CronJob) -> None:
        self.jobs.append(job)
        self.stop_event.set()


def test_queue_cron_handler_uses_async_queue_port() -> None:
    class RecordingQueue:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def enqueue(self, **kwargs):
            self.calls.append(kwargs)
            return "event-1"

    queue = RecordingQueue()
    job = CronJob(
        id="cron-1",
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"kind": "digest"},
        run_at=100,
        rrule=None,
        timezone="UTC",
        attempts=1,
        lease_token="lease-1",
    )

    asyncio.run(QueueCronHandler(queue).handle(job))

    assert queue.calls == [
        {
            "tenant_id": "tenant-a",
            "recipient": "agent",
            "message_type": "CronJobDue",
            "payload": {
                "cron_job_id": "cron-1",
                "source_id": "chat-1",
                "name": "daily_digest",
                "payload": {"kind": "digest"},
                "run_at": 100,
            },
            "idempotency_key": "cron:cron-1:100",
            "correlation_id": "cron-1",
        }
    ]


def test_cron_store_claims_and_completes_one_shot_job(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, created = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"chat_id": 1},
        run_at=100,
        now=90,
    )
    assert created is True

    jobs = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=100,
    )
    assert [job.id for job in jobs] == [job_id]

    assert start_execution(conn, job_id, lease_token=jobs[0].lease_token, now=100)
    complete_job(conn, job_id, lease_token=jobs[0].lease_token, fired_at=101)
    row = conn.execute("SELECT status FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "fired"


def test_expired_running_cron_job_becomes_uncertain_instead_of_being_replayed(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="external_effect",
        payload={},
        run_at=100,
        now=90,
    )
    claimed = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    assert start_execution(conn, job_id, lease_token=claimed[0].lease_token, now=100)

    assert claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    ) == []
    row = conn.execute("SELECT status FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "uncertain"


def test_recurring_cron_retry_does_not_shift_schedule_anchor(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    scheduled_at = int(datetime(2026, 1, 1, 9, tzinfo=timezone.utc).timestamp())
    retry_at = scheduled_at + 30 * 60
    next_scheduled_at = int(datetime(2026, 1, 2, 9, tzinfo=timezone.utc).timestamp())
    job_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={},
        run_at=scheduled_at,
        rrule="FREQ=DAILY",
        idempotency_key="daily-digest",
        now=scheduled_at - 60,
    )
    first = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-1",
        lease_seconds=60,
        now=scheduled_at,
    )[0]
    assert start_execution(conn, job_id, lease_token=first.lease_token, now=scheduled_at)
    assert fail_job(
        conn,
        job_id,
        lease_token=first.lease_token,
        retry_at=retry_at,
        last_error="not started",
        now=scheduled_at + 1,
    )

    row = conn.execute("SELECT run_at, retry_at FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()
    assert (row["run_at"], row["retry_at"]) == (scheduled_at, retry_at)
    assert claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-2",
        lease_seconds=60,
        now=retry_at - 1,
    ) == []

    retried = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="worker-2",
        lease_seconds=60,
        now=retry_at,
    )[0]
    assert start_execution(conn, job_id, lease_token=retried.lease_token, now=retry_at)
    assert complete_job(conn, job_id, lease_token=retried.lease_token, fired_at=retry_at + 60)

    row = conn.execute("SELECT status, run_at, retry_at FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "pending"
    assert row["run_at"] == next_scheduled_at
    assert row["retry_at"] is None
    assert insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={},
        run_at=scheduled_at,
        rrule="FREQ=DAILY",
        idempotency_key="daily-digest",
    ) == (job_id, False)
    with pytest.raises(IdempotencyConflictError):
        insert_job(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            name="daily_digest",
            payload={},
            run_at=scheduled_at + 60,
            rrule="FREQ=DAILY",
            idempotency_key="daily-digest",
        )


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
    assert start_execution(conn, job_id, lease_token=claimed.lease_token, now=scheduled_at)
    assert complete_job(
        conn,
        job_id,
        lease_token=claimed.lease_token,
        fired_at=scheduled_at + 60,
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


def test_cron_worker_calls_handler(tmp_path):
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

    async def run() -> RecordingCronHandler:
        stop_event = asyncio.Event()
        handler = RecordingCronHandler(stop_event)
        await asyncio.wait_for(
            run_cron_worker(
                db_path,
                stop_event,
                handler=handler,
                poll_interval_s=0.01,
            ),
            timeout=1,
        )
        return handler

    handler = asyncio.run(run())
    conn = open_sqlite(db_path)
    row = conn.execute("SELECT status FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()

    assert [job.name for job in handler.jobs] == ["daily_digest"]
    assert row["status"] == "fired"


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
        self.completed: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self.uncertain: list[tuple[str, str]] = []

    async def insert(self, **kwargs):
        return "cron_fake", True

    async def claim_due(self, *, limit: int, lease_owner: str, lease_seconds: int):
        claimed, self.jobs = self.jobs[:limit], self.jobs[limit:]
        return claimed

    async def renew_lease(self, job_id: str, *, lease_token: str, lease_seconds: int) -> bool:
        return True

    async def start_execution(self, job_id: str, *, lease_token: str) -> bool:
        return True

    async def complete(self, job_id: str, *, lease_token: str) -> bool:
        self.completed.append(job_id)
        return True

    async def mark_uncertain(
        self,
        job_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        self.uncertain.append((job_id, last_error))
        return True

    async def fail(
        self,
        job_id: str,
        *,
        lease_token: str,
        retry_at: int,
        last_error: str,
    ) -> bool:
        self.failed.append((job_id, last_error))
        return True


def test_cron_store_worker_uses_cron_store_port():
    async def run() -> tuple[RecordingCronHandler, FakeCronStore]:
        stop_event = asyncio.Event()
        handler = RecordingCronHandler(stop_event)
        store = FakeCronStore()
        await asyncio.wait_for(
            run_cron_store_worker(
                store,
                stop_event,
                handler=handler,
                poll_interval_s=0.01,
            ),
            timeout=1,
        )
        return handler, store

    handler, store = asyncio.run(run())

    assert [job.name for job in handler.jobs] == ["daily_digest"]
    assert store.completed == ["cron_1"]
    assert store.failed == []


def test_cron_rejects_invalid_schedule_before_insert(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    try:
        insert_job(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            name="broken",
            payload={},
            run_at=100,
            rrule="not an rrule",
        )
    except ValueError as exc:
        assert "rrule" in str(exc)
    else:
        raise AssertionError("invalid rrule was accepted")

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
    replay = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily",
        payload={"kind": "digest"},
        run_at=100,
        idempotency_key="daily-1",
    )

    assert replay == (first[0], False)
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


def test_cron_handler_failure_after_start_is_uncertain():
    class FailingHandler:
        async def handle(self, job: CronJob) -> None:
            raise TimeoutError("outcome unknown")

    async def run() -> FakeCronStore:
        stop_event = asyncio.Event()
        store = FakeCronStore()

        async def stop_when_uncertain() -> None:
            while not store.uncertain:
                await asyncio.sleep(0.01)
            stop_event.set()

        stopper = asyncio.create_task(stop_when_uncertain())
        await asyncio.wait_for(
            run_cron_store_worker(
                store,
                stop_event,
                handler=FailingHandler(),
                poll_interval_s=0.01,
            ),
            timeout=1,
        )
        await stopper
        return store

    store = asyncio.run(run())

    assert store.failed == []
    assert store.uncertain[0][0] == "cron_1"
