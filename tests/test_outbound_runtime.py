import asyncio

import pytest

from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.outbound.contracts import OutboundMessage, SendResult
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.store import claim_due, enqueue_outbound, mark_retry, mark_sending
from soveren_agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


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
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="hello:1",
        run_after=100,
        now=100,
    )
    second = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="hello:1",
        run_after=100,
        now=101,
    )

    assert first is not None
    assert second is None
    with pytest.raises(IdempotencyConflictError):
        enqueue_outbound(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            destination_id="chat-1",
            text="hello again",
            idempotency_key="hello:1",
            run_after=100,
            now=102,
        )

    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    assert mark_retry(
        conn,
        first,
        lease_token=claimed[0]["lease_token"],
        run_after=150,
        last_error="not started",
        now=101,
    ) == "retrying"
    assert (
        enqueue_outbound(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            destination_id="chat-1",
            text="hello",
            idempotency_key="hello:1",
            run_after=100,
            now=102,
        )
        is None
    )
    with pytest.raises(IdempotencyConflictError):
        enqueue_outbound(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            destination_id="chat-1",
            text="hello",
            idempotency_key="hello:1",
            run_after=101,
            now=102,
        )


def test_legacy_outbound_replay_survives_retry_schedule_change(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="legacy:hello",
        run_after=100,
        now=90,
    )
    assert message_id is not None
    conn.execute(
        "UPDATE outbound_messages SET idempotency_fingerprint = NULL WHERE id = ?",
        (message_id,),
    )
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
        now=100,
    )
    assert mark_retry(
        conn,
        message_id,
        lease_token=claimed[0]["lease_token"],
        run_after=150,
        last_error="retry",
        now=101,
    ) == "retrying"

    assert enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="legacy:hello",
        run_after=100,
        now=102,
    ) is None
    with pytest.raises(IdempotencyConflictError):
        enqueue_outbound(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            destination_id="chat-1",
            text="different",
            idempotency_key="legacy:hello",
            run_after=100,
            now=102,
        )


def test_expired_sending_message_becomes_uncertain_instead_of_being_replayed(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="uncertain:1",
        now=100,
    )
    assert message_id is not None
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )
    assert mark_sending(conn, message_id, lease_token=claimed[0]["lease_token"], now=100)

    assert claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    ) == []
    row = conn.execute("SELECT status FROM outbound_messages WHERE id = ?", (message_id,)).fetchone()
    assert row["status"] == "uncertain"


def test_outbound_worker_sends_via_registered_channel(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
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
                source_id="chat-1",
                channel="telegram",
                destination_id="chat-1",
                text="from fake queue",
                lease_token="lease-1",
                attempts=1,
                max_attempts=5,
            )
        ]
        self.sent: list[tuple[str, dict]] = []
        self.retries: list[tuple[str, str]] = []
        self.uncertain: list[tuple[str, str]] = []

    async def enqueue(self, **kwargs):
        return "out_fake"

    async def claim_due(self, *, channel: str, limit: int, lease_owner: str, lease_seconds: int):
        claimed, self.messages = self.messages[:limit], self.messages[limit:]
        return claimed

    async def renew_lease(self, message_id: str, *, lease_token: str, lease_seconds: int) -> bool:
        return True

    async def mark_sending(self, message_id: str, *, lease_token: str) -> bool:
        return True

    async def mark_sent(
        self,
        message_id: str,
        *,
        lease_token: str,
        result: dict | None = None,
    ) -> bool:
        self.sent.append((message_id, result or {}))
        return True

    async def mark_uncertain(
        self,
        message_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        self.uncertain.append((message_id, last_error))
        return True

    async def mark_retry(
        self,
        message_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str:
        self.retries.append((message_id, last_error))
        return "retrying"


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


def test_outbound_sender_failure_is_not_retried_automatically():
    class FailingSender:
        async def send(self, message: OutboundMessage) -> SendResult:
            raise TimeoutError("outcome unknown")

    async def run() -> FakeOutboundQueue:
        stop_event = asyncio.Event()
        queue = FakeOutboundQueue()

        async def stop_when_uncertain() -> None:
            while not queue.uncertain:
                await asyncio.sleep(0.01)
            stop_event.set()

        stopper = asyncio.create_task(stop_when_uncertain())
        await asyncio.wait_for(
            run_outbound_queue_worker(
                queue,
                stop_event,
                registry=OutboundRegistry({"telegram": FailingSender()}),
                channel="telegram",
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stopper
        return queue

    queue = asyncio.run(run())

    assert queue.retries == []
    assert queue.uncertain[0][0] == "out_1"
