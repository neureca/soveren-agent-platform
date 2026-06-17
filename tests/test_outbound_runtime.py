import asyncio

from agent_platform.outbound.contracts import OutboundMessage, SendResult
from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.store import claim_due, enqueue_outbound
from agent_platform.outbound.worker import run_outbound_worker
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

