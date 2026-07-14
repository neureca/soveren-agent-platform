import asyncio

from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.agent.worker import run_agent_queue_worker, run_agent_worker
from soveren_agent_platform.queue.contracts import QueueEvent
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class RecordingAgentHandler:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.events: list[AgentEvent] = []

    async def handle(self, event: AgentEvent) -> None:
        self.events.append(event)
        self.stop_event.set()


def test_agent_worker_claims_queue_event_and_calls_handler(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="agent",
        message_type="TelegramMessageReceived",
        payload={"text": "hello"},
        idempotency_key="msg:1",
        now=100,
    )
    conn.close()
    assert event_id is not None

    async def run() -> RecordingAgentHandler:
        stop_event = asyncio.Event()
        handler = RecordingAgentHandler(stop_event)
        await asyncio.wait_for(
            run_agent_worker(
                db_path,
                stop_event,
                handler=handler,
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        return handler

    handler = asyncio.run(run())
    conn = open_sqlite(db_path)
    row = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()

    assert [event.payload for event in handler.events] == [{"text": "hello"}]
    assert row["status"] == "done"


class FakeQueue:
    def __init__(self) -> None:
        self.events = [
            QueueEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent",
                message_type="TestEvent",
                payload={"text": "from fake broker"},
                lease_token="lease-1",
                attempts=1,
                max_attempts=5,
            )
        ]
        self.done: list[str] = []
        self.retries: list[tuple[str, str]] = []

    async def enqueue(self, **kwargs):
        return "evt_fake"

    async def claim_due(
        self,
        *,
        recipient: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        recover_exhausted: bool = False,
    ):
        claimed, self.events = self.events[:limit], self.events[limit:]
        return claimed

    async def renew_lease(self, event_id: str, *, lease_token: str, lease_seconds: int) -> bool:
        return True

    async def mark_done(self, event_id: str, *, lease_token: str) -> bool:
        self.done.append(event_id)
        return True

    async def mark_retry(
        self,
        event_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str:
        self.retries.append((event_id, last_error))
        return "retrying"


def test_agent_queue_worker_uses_durable_queue_port():
    async def run() -> tuple[RecordingAgentHandler, FakeQueue]:
        stop_event = asyncio.Event()
        handler = RecordingAgentHandler(stop_event)
        queue = FakeQueue()
        await asyncio.wait_for(
            run_agent_queue_worker(
                queue,
                stop_event,
                handler=handler,
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        return handler, queue

    handler, queue = asyncio.run(run())

    assert [event.payload for event in handler.events] == [{"text": "from fake broker"}]
    assert queue.done == ["evt_1"]
    assert queue.retries == []
