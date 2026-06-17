import asyncio

from agent_platform.agent.contracts import AgentEvent
from agent_platform.agent.worker import run_agent_worker
from agent_platform.queue.durable import enqueue
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


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

