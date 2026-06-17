import asyncio

from agent_platform.cron.contracts import CronJob
from agent_platform.cron.store import claim_due_jobs, complete_job, insert_job
from agent_platform.cron.worker import run_cron_store_worker, run_cron_worker
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


class RecordingCronHandler:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.jobs: list[CronJob] = []

    async def handle(self, job: CronJob) -> None:
        self.jobs.append(job)
        self.stop_event.set()


def test_cron_store_claims_and_completes_one_shot_job(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    job_id = insert_job(
        conn,
        tenant_id="tenant-a",
        name="daily_digest",
        payload={"chat_id": 1},
        run_at=100,
        now=90,
    )

    jobs = claim_due_jobs(
        conn,
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=100,
    )
    assert [job.id for job in jobs] == [job_id]

    complete_job(conn, job_id, fired_at=101)
    row = conn.execute("SELECT status FROM cron_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "fired"


def test_cron_worker_calls_handler(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    job_id = insert_job(
        conn,
        tenant_id="tenant-a",
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
                name="daily_digest",
                payload={"chat_id": 1},
                run_at=100,
                rrule=None,
                timezone="UTC",
                attempts=1,
            )
        ]
        self.completed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    async def insert(self, **kwargs):
        return "cron_fake"

    async def claim_due(self, *, limit: int, lease_owner: str, lease_seconds: int):
        claimed, self.jobs = self.jobs[:limit], self.jobs[limit:]
        return claimed

    async def complete(self, job_id: str) -> None:
        self.completed.append(job_id)

    async def fail(self, job_id: str, *, retry_at: int, last_error: str) -> None:
        self.failed.append((job_id, last_error))


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
