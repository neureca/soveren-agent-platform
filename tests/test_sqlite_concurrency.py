import asyncio
import threading
import time

from soveren_agent_platform.queue.sqlite import SQLiteEventQueue
from soveren_agent_platform.sessions.sqlite import SQLiteSessionMailboxStore
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite, run_sqlite


def test_async_sqlite_adapters_serialize_transactions_on_one_connection(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="fake",
        backend_session_id="backend-1",
    )
    store = SQLiteSessionMailboxStore._from_connection(conn)

    async def run() -> list[tuple[str, bool]]:
        return await asyncio.gather(
            *(
                store.enqueue_prompt(
                    session_id=session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    prompt=f"prompt-{index}",
                    idempotency_key=f"prompt-{index}",
                )
                for index in range(100)
            )
        )

    results = asyncio.run(run())

    assert len({mailbox_id for mailbox_id, _ in results}) == 100
    assert all(created for _, created in results)
    assert conn.execute("SELECT COUNT(*) FROM session_mailbox").fetchone()[0] == 100


def test_cancelled_sqlite_call_finishes_started_operation_before_propagating(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    conn.execute("CREATE TABLE writes (value TEXT NOT NULL)")

    def slow_write(connection):
        time.sleep(0.05)
        connection.execute("INSERT INTO writes(value) VALUES ('done')")

    async def run() -> None:
        task = asyncio.create_task(run_sqlite(conn, slow_write))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancellation was not propagated")

    asyncio.run(run())

    assert conn.execute("SELECT value FROM writes").fetchone()[0] == "done"


def test_repeated_cancellation_still_waits_for_started_sqlite_operation(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    conn.execute("CREATE TABLE writes (value TEXT NOT NULL)")
    completed = threading.Event()

    def slow_write(connection):
        time.sleep(0.08)
        connection.execute("INSERT INTO writes(value) VALUES ('done')")
        completed.set()

    async def run() -> None:
        task = asyncio.create_task(run_sqlite(conn, slow_write))
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancellation was not propagated")

    asyncio.run(run())

    assert completed.is_set()
    assert conn.execute("SELECT value FROM writes").fetchone()[0] == "done"


def test_async_ingress_cannot_join_an_unrelated_transaction(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    transaction_started = threading.Event()
    allow_rollback = threading.Event()
    events = SQLiteEventQueue._from_connection(conn)

    def rollback_later(connection):
        connection.execute("BEGIN IMMEDIATE")
        transaction_started.set()
        allow_rollback.wait(timeout=2)
        connection.execute("ROLLBACK")

    async def run() -> str:
        transaction = asyncio.create_task(run_sqlite(conn, rollback_later))
        await asyncio.to_thread(transaction_started.wait, 2)
        enqueue = asyncio.create_task(
            events.enqueue(
                tenant_id="tenant-a",
                recipient="batching",
                message_type="InboundMessageReceived",
                payload={"text": "survives rollback"},
                idempotency_key="event-1",
            )
        )
        await asyncio.sleep(0.01)
        assert not enqueue.done()
        allow_rollback.set()
        await transaction
        event_id = await enqueue
        assert event_id is not None
        return event_id

    event_id = asyncio.run(run())

    assert conn.execute("SELECT id FROM event_queue WHERE id = ?", (event_id,)).fetchone() is not None
