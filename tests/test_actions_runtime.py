import asyncio
import json

from soveren_agent_platform.actions.contracts import ActionExecutionResult, ActionRecord
from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.sqlite import SQLiteActionStore
from soveren_agent_platform.actions.store import approve_action, get_action, insert_action
from soveren_agent_platform.actions.worker import process_action_event, run_actions_queue_worker
from soveren_agent_platform.queue.contracts import QueueEvent
from soveren_agent_platform.queue.durable import claim_due, enqueue
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(self, action: ActionRecord):
        self.calls.append(action.id)
        return ActionExecutionResult(result={"echo": action.payload["value"]})


def test_action_approval_and_worker_execution_are_idempotent(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, created = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="echo",
        payload={"value": 42},
        approval_policy="manual",
        idempotency_key="echo:42",
        now=100,
    )
    duplicate_id, duplicate_created = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="echo",
        payload={"value": 43},
        approval_policy="manual",
        idempotency_key="echo:42",
        now=101,
    )

    assert created is True
    assert duplicate_id == action_id
    assert duplicate_created is False
    assert approve_action(conn, action_id, approver_id="admin", now=102) is True

    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id},
        idempotency_key="execute:1",
        now=103,
    )
    assert event_id is not None
    row = claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=103,
    )[0]
    executor = RecordingExecutor()
    registry = ActionRegistry({"echo": executor})

    asyncio.run(process_action_event(conn, row, registry=registry))
    action = get_action(conn, action_id)
    assert action is not None
    assert action["status"] == "executed"
    assert json.loads(action["result_json"]) == {"echo": 42}

    # Retried execution event must not run the executor again.
    retry_event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id},
        idempotency_key="execute:2",
        now=104,
    )
    assert retry_event_id is not None
    retry_row = claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=104,
    )[0]
    asyncio.run(process_action_event(conn, retry_row, registry=registry))

    assert executor.calls == [action_id]


def test_auto_approval_action_starts_approved(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="read_only",
        payload={},
        approval_policy="auto",
    )

    action = get_action(conn, action_id)
    assert action is not None
    assert action["status"] == "approved"


class FakeQueue:
    def __init__(self, action_id: str) -> None:
        self.events = [
            QueueEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="actions",
                message_type="ExecuteAction",
                payload={"action_id": action_id},
            )
        ]
        self.done: list[str] = []
        self.retries: list[tuple[str, str]] = []

    async def enqueue(self, **kwargs):
        return "evt_fake"

    async def claim_due(self, *, recipient: str, limit: int, lease_owner: str, lease_seconds: int):
        claimed, self.events = self.events[:limit], self.events[limit:]
        return claimed

    async def mark_done(self, event_id: str) -> None:
        self.done.append(event_id)

    async def mark_retry(self, event_id: str, *, run_after: int, last_error: str) -> None:
        self.retries.append((event_id, last_error))


def test_actions_queue_worker_uses_durable_queue_port(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="echo",
        payload={"value": 99},
        approval_policy="auto",
        now=100,
    )
    executor = RecordingExecutor()
    registry = ActionRegistry({"echo": executor})

    async def run():
        stop_event = asyncio.Event()
        queue = FakeQueue(action_id)

        async def stopper():
            while not queue.done:
                await asyncio.sleep(0.01)
            stop_event.set()

        stop_task = asyncio.create_task(stopper())
        await asyncio.wait_for(
            run_actions_queue_worker(
                SQLiteActionStore(conn),
                queue,
                stop_event,
                registry=registry,
                idle_initial_s=0.01,
            ),
            timeout=1,
        )
        await stop_task
        return queue

    queue = asyncio.run(run())
    action = get_action(conn, action_id)

    assert action is not None
    assert action["status"] == "executed"
    assert queue.done == ["evt_1"]
    assert queue.retries == []
