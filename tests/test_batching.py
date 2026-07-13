import asyncio
import json
import time

import pytest

import soveren_agent_platform.batching.store as batch_store_module
from soveren_agent_platform.batching import InboundMessage
from soveren_agent_platform.batching.contracts import BatchDecision, BatchState, MessageFeatures
from soveren_agent_platform.batching.rules import decide_batch
from soveren_agent_platform.batching.store import append_inbound_message, batch_payload, load_state, route_batch
from soveren_agent_platform.batching.worker import run_batching_queue_worker, run_batching_worker
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.queue.contracts import QueueEvent
from soveren_agent_platform.queue.durable import enqueue
from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite
from soveren_agent_platform.telegram import TelegramInboundMessage, enqueue_telegram_message


def test_batching_store_appends_and_decides_flush_by_count(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    batch_id = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-1",
            raw_event_id="raw-1",
            source_event_id="update-1",
            text="сделай отчет",
            payload={"from_first_name": "Ivan"},
            message_at=100,
        ),
    )
    assert batch_id is not None
    state = load_state(
        conn,
        batch_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        max_count=1,
        now=100,
    )
    decision = decide_batch(state)

    assert state is not None
    assert state.message_count == 1
    assert decision.action == "flush"
    assert "max_count_reached" in decision.matched_rules


def test_raw_event_idempotency_is_conversation_scoped(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    batch_a = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-a",
            raw_event_id="same-provider-id",
            text="a",
            payload={},
            message_at=100,
        ),
    )
    batch_b = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-b",
            raw_event_id="same-provider-id",
            text="b",
            payload={},
            message_at=100,
        ),
    )

    assert batch_a is not None and batch_b is not None and batch_a != batch_b


def test_raw_event_replay_rejects_different_message(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message = InboundMessage(
        tenant_id="tenant-a",
        channel="telegram",
        source_id="chat-a",
        raw_event_id="provider-1",
        source_event_id="update-1",
        text="same",
        payload={"user": "u1"},
        message_at=100,
    )

    batch_id = append_inbound_message(conn, message)
    assert append_inbound_message(conn, message) == batch_id
    with pytest.raises(IdempotencyConflictError):
        append_inbound_message(
            conn,
            InboundMessage(
                tenant_id="tenant-a",
                channel="telegram",
                source_id="chat-a",
                raw_event_id="provider-1",
                source_event_id="update-1",
                text="different",
                payload={"user": "u1"},
                message_at=100,
            ),
        )


def test_batching_worker_routes_ready_batch_to_agent_queue(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="batching",
        message_type="InboundMessageReceived",
        payload={
            "channel": "telegram",
            "source_id": "chat-1",
            "raw_event_id": "raw-1",
            "source_event_id": "update-1",
            "text": "сделай отчет",
            "from_first_name": "Ivan",
            "message_at": 100,
        },
        idempotency_key="raw-1",
        now=100,
    )
    conn.close()
    assert event_id is not None

    async def run() -> None:
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_batching_worker(
                db_path,
                stop_event,
                quiet_window_s=100,
                max_window_s=100,
                max_count=1,
            )
        )
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(run())
    conn = open_sqlite(db_path)
    routed = conn.execute(
        "SELECT * FROM event_queue WHERE recipient = 'agent' AND message_type = 'ChatBatchReady'"
    ).fetchone()
    source = conn.execute("SELECT status FROM event_queue WHERE id = ?", (event_id,)).fetchone()

    assert source["status"] == "done"
    assert routed is not None
    payload = json.loads(routed["payload_json"])
    assert payload["batch_message_count"] == 1
    assert payload["text"] == "participant_1: сделай отчет"


def test_telegram_group_batch_uses_stable_pseudonyms_without_names_or_ids(tmp_path):
    db_path = tmp_path / "app.db"
    conn = open_sqlite(db_path)
    apply_platform_migrations(conn)
    message_at = int(time.time())
    for update_id, user_id, username, text in (
        (1, 101, "alice-private", "первая часть"),
        (2, 202, "bob-private", "вторая часть"),
    ):
        asyncio.run(
            enqueue_telegram_message(
                SQLiteEventQueue._from_connection(conn),
                TelegramInboundMessage(
                    tenant_id="tenant-a",
                    chat_id=-1001,
                    update_id=update_id,
                    user_id=user_id,
                    username=username,
                    text=text,
                    payload={
                        "message_at": message_at,
                        "from_first_name": username,
                        "from_username": username,
                    },
                ),
            )
        )
    conn.close()

    async def run() -> None:
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_batching_worker(
                db_path,
                stop_event,
                quiet_window_s=100,
                max_window_s=100,
                max_count=2,
            )
        )
        await asyncio.sleep(0.1)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(run())
    conn = open_sqlite(db_path)
    row = conn.execute(
        "SELECT payload_json FROM event_queue WHERE recipient = 'agent' AND message_type = 'ChatBatchReady'"
    ).fetchone()
    assert row is not None
    text = json.loads(row["payload_json"])["text"]
    assert text.splitlines() == [
        "participant_1: первая часть",
        "participant_2: вторая часть",
    ]
    assert "alice-private" not in text
    assert "bob-private" not in text
    assert "101" not in text
    assert "202" not in text


class FakeQueue:
    def __init__(self) -> None:
        self.events = [
            QueueEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="batching",
                message_type="InboundMessageReceived",
                payload={
                    "channel": "telegram",
                    "source_id": "chat-1",
                    "raw_event_id": "raw-1",
                    "text": "сделай отчет",
                    "message_at": 100,
                },
                lease_token="lease-1",
                attempts=1,
                max_attempts=5,
            )
        ]
        self.enqueued: list[dict] = []
        self.done: list[str] = []
        self.retries: list[tuple[str, str]] = []

    async def enqueue(self, **kwargs):
        self.enqueued.append(kwargs)
        return f"evt_out_{len(self.enqueued)}"

    async def claim_due(self, *, recipient: str, limit: int, lease_owner: str, lease_seconds: int):
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


class FakeBatchStore:
    def __init__(self) -> None:
        self.decision: BatchDecision | None = None
        self.routed: list[dict] = []

    async def append_inbound_message(self, message: InboundMessage) -> str | None:
        return "batch-1"

    async def load_state(
        self,
        batch_id: str,
        *,
        tenant_id: str,
        source_id: str,
        quiet_window_s: int,
        max_window_s: int,
        max_count: int,
    ):
        return BatchState(
            batch_id=batch_id,
            tenant_id=tenant_id,
            channel="telegram",
            source_id="chat-1",
            messages=[
                {
                    "text": "сделай отчет",
                    "from_first_name": "Ivan",
                    "raw_event_id": "raw-1",
                }
            ],
            features=[MessageFeatures()],
            now=100,
            first_message_at=100,
            last_message_at=100,
            message_count=1,
            quiet_window_s=quiet_window_s,
            max_window_s=max_window_s,
            max_count=max_count,
        )

    async def store_decision(
        self,
        batch_id: str,
        decision: BatchDecision,
        *,
        tenant_id: str,
        source_id: str,
        state=None,
    ) -> None:
        self.decision = decision

    async def route_batch(self, batch_id: str, **kwargs) -> bool:
        self.routed.append({"batch_id": batch_id, **kwargs})
        return True


def test_batching_queue_worker_uses_queue_and_batch_store_ports():
    async def run() -> tuple[FakeQueue, FakeBatchStore]:
        stop_event = asyncio.Event()
        queue = FakeQueue()
        store = FakeBatchStore()

        async def stopper():
            while not store.routed:
                await asyncio.sleep(0.01)
            stop_event.set()

        stop_task = asyncio.create_task(stopper())
        await asyncio.wait_for(
            run_batching_queue_worker(
                queue,
                store,
                stop_event,
                quiet_window_s=100,
                max_window_s=100,
                max_count=1,
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stop_task
        return queue, store

    queue, store = asyncio.run(run())

    assert store.decision is not None
    assert store.decision.action == "flush"
    assert store.routed[0]["batch_id"] == "batch-1"
    assert queue.done == ["evt_1"]
    assert store.routed[0]["message_type"] == "ChatBatchReady"
    assert store.routed[0]["payload"]["text"] == "participant_1: сделай отчет"


def test_route_batch_rolls_back_status_when_event_enqueue_fails(tmp_path, monkeypatch):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    batch_id = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-1",
            raw_event_id="raw-1",
            source_event_id="update-1",
            text="сделай отчет",
            payload={"from_first_name": "Ivan"},
            message_at=100,
        ),
    )
    assert batch_id is not None
    state = load_state(
        conn,
        batch_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        max_count=1,
        now=100,
    )
    assert state is not None

    def raise_on_enqueue(*args, **kwargs):
        raise RuntimeError("queue write failed")

    monkeypatch.setattr(batch_store_module, "enqueue", raise_on_enqueue)

    with pytest.raises(RuntimeError, match="queue write failed"):
        route_batch(
            conn,
            batch_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            recipient="agent",
            message_type="ChatBatchReady",
            payload=batch_payload(state),
            idempotency_key=f"inbound-batch:{batch_id}",
            correlation_id=batch_id,
            causation_id="evt-1",
        )

    batch = conn.execute("SELECT status FROM inbound_batches WHERE id = ?", (batch_id,)).fetchone()
    event = conn.execute(
        "SELECT * FROM event_queue WHERE recipient = 'agent' AND message_type = 'ChatBatchReady'"
    ).fetchone()
    assert batch["status"] == "collecting"
    assert event is None


def test_batch_state_and_transitions_are_conversation_fenced(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    batch_id = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-b",
            raw_event_id="raw-b",
            text="chat-b-secret",
            payload={},
            message_at=100,
        ),
    )
    assert batch_id is not None
    original_decision = conn.execute(
        "SELECT decision_json FROM inbound_batches WHERE id = ?",
        (batch_id,),
    ).fetchone()["decision_json"]

    assert (
        load_state(
            conn,
            batch_id,
            tenant_id="tenant-a",
            source_id="chat-a",
            now=100,
        )
        is None
    )
    batch_store_module.store_decision(
        conn,
        batch_id,
        BatchDecision(action="flush", wait_score=0, flush_score=1),
        tenant_id="tenant-a",
        source_id="chat-a",
    )
    assert not route_batch(
        conn,
        batch_id,
        tenant_id="tenant-a",
        source_id="chat-a",
        recipient="agent",
        message_type="ChatBatchReady",
        payload={"batch_id": batch_id},
        idempotency_key=f"foreign-batch:{batch_id}",
    )

    row = conn.execute(
        "SELECT status, decision_json FROM inbound_batches WHERE id = ?",
        (batch_id,),
    ).fetchone()
    assert row["status"] == "collecting"
    assert row["decision_json"] == original_decision
    assert (
        conn.execute(
            "SELECT 1 FROM event_queue WHERE tenant_id = 'tenant-a' AND idempotency_key = ?",
            (f"foreign-batch:{batch_id}",),
        ).fetchone()
        is None
    )
