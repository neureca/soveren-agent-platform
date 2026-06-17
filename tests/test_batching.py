import asyncio
import json

from agent_platform.batching import InboundMessage, append_inbound_message, load_state
from agent_platform.batching.rules import decide_batch
from agent_platform.batching.worker import run_batching_worker
from agent_platform.queue.durable import enqueue
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


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

    state = load_state(conn, batch_id, max_count=1, now=100)
    decision = decide_batch(state)

    assert state is not None
    assert state.message_count == 1
    assert decision.action == "flush"
    assert "max_count_reached" in decision.matched_rules


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
    assert payload["text"] == "Ivan: сделай отчет"

