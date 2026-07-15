import asyncio
import json

import pytest

from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.outbound.contracts import OutboundMessage, SendResult
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.outbound.store import (
    claim_due,
    enqueue_outbound,
    mark_dead_letter,
    mark_retry,
    mark_sending,
)
from soveren_agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import TELEGRAM_TEXT_LIMIT, enqueue_telegram_text


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


@pytest.mark.parametrize(
    ("limit", "lease_seconds", "message"),
    [
        (0, 30, "limit must be positive"),
        (-1, 30, "limit must be positive"),
        (1, 0, "lease_seconds must be positive"),
        (1, -1, "lease_seconds must be positive"),
    ],
)
def test_outbound_queue_rejects_invalid_claim_settings_before_mutation(
    tmp_path,
    limit,
    lease_seconds,
    message,
):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="hello:invalid-claim",
        now=100,
    )
    assert message_id is not None

    with pytest.raises(ValueError, match=message):
        claim_due(
            conn,
            channel="telegram",
            limit=limit,
            lease_owner="worker",
            lease_seconds=lease_seconds,
            now=100,
        )

    row = conn.execute(
        "SELECT status, attempts, lease_token, lease_until FROM outbound_messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    assert tuple(row) == ("queued", 0, None, None)


def test_outbound_queue_persists_permanent_failure_as_dead_letter(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="invalid destination",
        idempotency_key="hello:permanent-failure",
        now=100,
    )
    assert message_id is not None
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker",
        lease_seconds=30,
        now=100,
    )

    assert mark_dead_letter(
        conn,
        message_id,
        lease_token=claimed[0]["lease_token"],
        last_error="destination rejected",
        now=101,
    )
    row = conn.execute(
        "SELECT status, last_error, lease_owner, lease_until, lease_token"
        " FROM outbound_messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    assert tuple(row) == ("dead_letter", "destination rejected", None, None, None)


def test_outbound_queue_tenant_scope_fences_claim_and_expired_cleanup(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    selected_a = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-a",
        channel="telegram-selection",
        destination_id="chat-a",
        text="a",
        idempotency_key="tenant-a:selection",
        now=100,
    )
    selected_b = enqueue_outbound(
        conn,
        tenant_id="tenant-b",
        source_id="chat-b",
        channel="telegram-selection",
        destination_id="chat-b",
        text="b",
        idempotency_key="tenant-b:selection",
        now=100,
    )
    assert selected_a is not None
    assert selected_b is not None

    claimed = claim_due(
        conn,
        channel="telegram-selection",
        limit=10,
        lease_owner="tenant-a-worker",
        lease_seconds=10,
        tenant_id="tenant-a",
        now=100,
    )
    assert [row["id"] for row in claimed] == [selected_a]
    assert conn.execute(
        "SELECT status FROM outbound_messages WHERE id = ?",
        (selected_b,),
    ).fetchone()[0] == "queued"

    sending_ids: dict[str, str] = {}
    for tenant_id in ("tenant-a", "tenant-b"):
        message_id = enqueue_outbound(
            conn,
            tenant_id=tenant_id,
            source_id=f"chat-{tenant_id}",
            channel="telegram",
            destination_id=f"chat-{tenant_id}",
            text=tenant_id,
            idempotency_key=f"{tenant_id}:sending",
            now=100,
        )
        assert message_id is not None
        sending_ids[tenant_id] = message_id
        leased = claim_due(
            conn,
            channel="telegram",
            limit=1,
            lease_owner=f"{tenant_id}-worker",
            lease_seconds=10,
            tenant_id=tenant_id,
            now=100,
        )
        assert mark_sending(conn, message_id, lease_token=leased[0]["lease_token"], now=100)

    assert claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="tenant-a-worker",
        lease_seconds=10,
        tenant_id="tenant-a",
        now=111,
    ) == []
    sending_states = {
        row["tenant_id"]: row["status"]
        for row in conn.execute(
            "SELECT tenant_id, status FROM outbound_messages WHERE id IN (?, ?)",
            (sending_ids["tenant-a"], sending_ids["tenant-b"]),
        ).fetchall()
    }
    assert sending_states == {"tenant-a": "uncertain", "tenant-b": "sending"}

    exhausted_ids: dict[str, str] = {}
    for tenant_id in ("tenant-a", "tenant-b"):
        message_id = enqueue_outbound(
            conn,
            tenant_id=tenant_id,
            source_id=f"chat-{tenant_id}",
            channel="telegram-dead",
            destination_id=f"chat-{tenant_id}",
            text=tenant_id,
            idempotency_key=f"{tenant_id}:expired",
            max_attempts=1,
            now=100,
        )
        assert message_id is not None
        exhausted_ids[tenant_id] = message_id
        assert claim_due(
            conn,
            channel="telegram-dead",
            limit=1,
            lease_owner=f"{tenant_id}-worker",
            lease_seconds=10,
            tenant_id=tenant_id,
            now=100,
        )

    assert claim_due(
        conn,
        channel="telegram-dead",
        limit=1,
        lease_owner="tenant-a-worker",
        lease_seconds=10,
        tenant_id="tenant-a",
        now=111,
    ) == []
    exhausted_states = {
        row["tenant_id"]: row["status"]
        for row in conn.execute(
            "SELECT tenant_id, status FROM outbound_messages WHERE id IN (?, ?)",
            (exhausted_ids["tenant-a"], exhausted_ids["tenant-b"]),
        ).fetchall()
    }
    assert exhausted_states == {"tenant-a": "dead_letter", "tenant-b": "leased"}


def test_outbound_queue_rejects_empty_tenant_scope(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    with pytest.raises(ValueError, match="tenant_id must be non-empty"):
        claim_due(
            conn,
            channel="telegram",
            limit=1,
            lease_owner="worker",
            lease_seconds=30,
            tenant_id=" ",
            now=100,
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


def test_expired_outbound_lease_is_dead_lettered_after_max_attempts(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="hello",
        idempotency_key="expired-final",
        max_attempts=1,
        now=100,
    )
    assert message_id is not None
    assert claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=10,
        now=100,
    )

    assert claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=10,
        now=111,
    ) == []
    row = conn.execute(
        "SELECT status, attempts, lease_token, last_error FROM outbound_messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    assert row["status"] == "dead_letter"
    assert row["attempts"] == 1
    assert row["lease_token"] is None
    assert row["last_error"] == "outbound lease expired after the maximum number of attempts"


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
        self.retries: list[tuple[str, str, int]] = []
        self.uncertain: list[tuple[str, str]] = []
        self.dead_letters: list[tuple[str, str]] = []
        self.claimed_tenant_ids: list[str | None] = []

    async def enqueue(self, **kwargs):
        return "out_fake"

    async def claim_due(
        self,
        *,
        channel: str,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        tenant_id: str | None = None,
    ):
        self.claimed_tenant_ids.append(tenant_id)
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

    async def mark_dead_letter(
        self,
        message_id: str,
        *,
        lease_token: str,
        last_error: str,
    ) -> bool:
        self.dead_letters.append((message_id, last_error))
        return True

    async def mark_retry(
        self,
        message_id: str,
        *,
        lease_token: str,
        run_after: int,
        last_error: str,
    ) -> str:
        self.retries.append((message_id, last_error, run_after))
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
                tenant_id="tenant-a",
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
    assert queue.claimed_tenant_ids
    assert set(queue.claimed_tenant_ids) == {"tenant-a"}


@pytest.mark.parametrize(
    ("batch_size", "lease_seconds", "message"),
    [
        (0, 60, "batch_size must be positive"),
        (1, 0, "lease_seconds must be positive"),
    ],
)
def test_outbound_queue_worker_rejects_invalid_claim_settings(
    batch_size,
    lease_seconds,
    message,
):
    async def run() -> None:
        with pytest.raises(ValueError, match=message):
            await run_outbound_queue_worker(
                FakeOutboundQueue(),
                asyncio.Event(),
                registry=OutboundRegistry({"telegram": RecordingSender()}),
                channel="telegram",
                batch_size=batch_size,
                lease_seconds=lease_seconds,
            )

    asyncio.run(run())


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


def test_outbound_retryable_result_uses_result_delay(monkeypatch):
    import soveren_agent_platform.outbound.worker as worker_module

    class RetryableSender:
        async def send(self, message: OutboundMessage) -> SendResult:
            return SendResult.retryable_failure("rate limited", retry_after_s=7)

    monkeypatch.setattr(worker_module.time, "time", lambda: 100)

    async def run() -> FakeOutboundQueue:
        stop_event = asyncio.Event()
        queue = FakeOutboundQueue()

        async def stop_when_retrying() -> None:
            while not queue.retries:
                await asyncio.sleep(0.01)
            stop_event.set()

        stopper = asyncio.create_task(stop_when_retrying())
        await asyncio.wait_for(
            run_outbound_queue_worker(
                queue,
                stop_event,
                registry=OutboundRegistry({"telegram": RetryableSender()}),
                channel="telegram",
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stopper
        return queue

    queue = asyncio.run(run())

    assert queue.retries == [("out_1", "rate limited", 107)]
    assert queue.uncertain == []
    assert queue.dead_letters == []


def test_outbound_permanent_result_is_dead_lettered_without_retry():
    class PermanentFailureSender:
        async def send(self, message: OutboundMessage) -> SendResult:
            return SendResult.permanent_failure("destination rejected")

    async def run() -> FakeOutboundQueue:
        stop_event = asyncio.Event()
        queue = FakeOutboundQueue()

        async def stop_when_dead_lettered() -> None:
            while not queue.dead_letters:
                await asyncio.sleep(0.01)
            stop_event.set()

        stopper = asyncio.create_task(stop_when_dead_lettered())
        await asyncio.wait_for(
            run_outbound_queue_worker(
                queue,
                stop_event,
                registry=OutboundRegistry({"telegram": PermanentFailureSender()}),
                channel="telegram",
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stopper
        return queue

    queue = asyncio.run(run())

    assert queue.dead_letters == [("out_1", "destination rejected")]
    assert queue.retries == []
    assert queue.uncertain == []


def test_enqueue_telegram_text_creates_stable_durable_parts(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    conn.close()
    text = "a" * TELEGRAM_TEXT_LIMIT + "bc"

    async def enqueue_twice() -> tuple[tuple[str | None, ...], tuple[str | None, ...]]:
        async with await SQLiteOutboundQueue.open(db_path) as queue:
            first = await enqueue_telegram_text(
                queue,
                tenant_id="tenant-a",
                source_id="chat-1",
                destination_id="chat-1",
                text=text,
                idempotency_key="answer:1",
                payload={"disable_web_page_preview": True},
            )
            replay = await enqueue_telegram_text(
                queue,
                tenant_id="tenant-a",
                source_id="chat-1",
                destination_id="chat-1",
                text=text,
                idempotency_key="answer:1",
                payload={"disable_web_page_preview": True},
            )
            return first, replay

    first, replay = asyncio.run(enqueue_twice())
    conn = open_sqlite(db_path)
    rows = conn.execute(
        "SELECT text, idempotency_key, payload_json FROM outbound_messages ORDER BY rowid"
    ).fetchall()

    assert len(first) == 2
    assert all(message_id is not None for message_id in first)
    assert replay == (None, None)
    assert "".join(row["text"] for row in rows) == text
    assert [len(row["text"]) for row in rows] == [TELEGRAM_TEXT_LIMIT, 2]
    assert [row["idempotency_key"] for row in rows] == ["answer:1:part:1", "answer:1:part:2"]
    assert [json.loads(row["payload_json"])["_soveren_telegram_part"] for row in rows] == [
        {"index": 1, "count": 2},
        {"index": 2, "count": 2},
    ]


def test_enqueue_telegram_text_rejects_long_parse_mode_before_enqueue():
    class RecordingQueue:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def enqueue(self, **kwargs):
            self.calls.append(kwargs)
            return "out-1"

    queue = RecordingQueue()

    with pytest.raises(ValueError, match="parse_mode"):
        asyncio.run(
            enqueue_telegram_text(
                queue,  # type: ignore[arg-type]
                tenant_id="tenant-a",
                source_id="chat-1",
                destination_id="chat-1",
                text="x" * (TELEGRAM_TEXT_LIMIT + 1),
                idempotency_key="formatted:1",
                payload={"parse_mode": "HTML"},
            )
        )

    assert queue.calls == []
