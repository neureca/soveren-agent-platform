import asyncio

from agent_platform.outbound.contracts import OutboundMessage, SendResult
from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.store import claim_due, enqueue_outbound
from agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


class RecordingSender:
    def __init__(self) -> None:
        self.messages: list[OutboundMessage] = []

    async def send(self, message: OutboundMessage) -> SendResult:
        self.messages.append(message)
        return SendResult(metadata={"external_id": "msg-1"})


def test_outbound_enqueue_is_idempotent(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    first = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="hello:1",
        now=100,
    )
    second = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        channel="telegram",
        destination_id="chat-1",
        text="hello again",
        idempotency_key="hello:1",
        now=101,
    )

    assert first is not None
    assert second is None


def test_outbound_worker_sends_via_registered_channel(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="hello:1",
        payload={"parse_mode": "HTML"},
        now=100,
    )
    conn.close()
    assert message_id is not None

    async def run() -> RecordingSender:
        stop_event = asyncio.Event()
        sender = RecordingSender()
        task = asyncio.create_task(
            run_outbound_worker(
                db_path,
                stop_event,
                registry=OutboundRegistry({"telegram": sender}),
                channel="telegram",
            )
        )
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)
        return sender

    sender = asyncio.run(run())
    conn = open_sqlite(db_path)
    row = conn.execute("SELECT status, payload_json FROM outbound_messages WHERE id = ?", (message_id,)).fetchone()

    assert [message.text for message in sender.messages] == ["hello"]
    assert row["status"] == "sent"


class FakeOutboundQueue:
    def __init__(self) -> None:
        self.messages = [
            OutboundMessage(
                id="out_1",
                tenant_id="tenant-a",
                channel="telegram",
                destination_id="chat-1",
                text="from fake queue",
            )
        ]
        self.sent: list[tuple[str, dict]] = []
        self.retries: list[tuple[str, str]] = []

    async def enqueue(self, **kwargs):
        return "out_fake"

    async def claim_due(self, *, channel: str, limit: int, lease_owner: str, lease_seconds: int):
        claimed, self.messages = self.messages[:limit], self.messages[limit:]
        return claimed

    async def mark_sent(self, message_id: str, *, result: dict | None = None) -> None:
        self.sent.append((message_id, result or {}))

    async def mark_retry(self, message_id: str, *, run_after: int, last_error: str) -> None:
        self.retries.append((message_id, last_error))


def test_outbound_queue_worker_uses_outbound_queue_port():
    async def run() -> tuple[RecordingSender, FakeOutboundQueue]:
        stop_event = asyncio.Event()
        sender = RecordingSender()
        queue = FakeOutboundQueue()

        async def stopper():
            while not queue.sent:
                await asyncio.sleep(0.01)
            stop_event.set()

        stop_task = asyncio.create_task(stopper())
        await asyncio.wait_for(
            run_outbound_queue_worker(
                queue,
                stop_event,
                registry=OutboundRegistry({"telegram": sender}),
                channel="telegram",
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stop_task
        return sender, queue

    sender, queue = asyncio.run(run())

    assert [message.text for message in sender.messages] == ["from fake queue"]
    assert queue.sent == [("out_1", {"external_id": "msg-1"})]
    assert queue.retries == []
