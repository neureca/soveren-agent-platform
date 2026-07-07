import asyncio
import json

import pytest

from soveren_agent_platform.actions.contracts import ActionExecutionResult, ActionRecord
from soveren_agent_platform.actions.registry import ActionRegistry
from soveren_agent_platform.actions.sqlite import SQLiteActionStore
from soveren_agent_platform.actions.store import approve_action, get_action, insert_action
from soveren_agent_platform.actions.worker import (
    process_action_event,
    process_action_queue_event,
    run_actions_queue_worker,
)
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


class FailingExecutor:
    async def execute(self, action: ActionRecord):
        raise RuntimeError("temporary outage")


class PermanentFailureExecutor:
    async def execute(self, action: ActionRecord):
        return ActionExecutionResult.permanent_failure("invalid action payload")


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


def test_action_executor_exception_retries_event_without_terminal_failure(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="unstable",
        payload={},
        approval_policy="auto",
        now=100,
    )
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id},
        idempotency_key="execute:unstable",
        max_attempts=5,
        now=101,
    )
    assert event_id is not None
    row = claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=101,
    )[0]

    asyncio.run(process_action_event(conn, row, registry=ActionRegistry({"unstable": FailingExecutor()})))

    action = get_action(conn, action_id)
    event = conn.execute("SELECT status, attempts, last_error FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert action is not None
    assert action["status"] == "queued"
    assert "RuntimeError: temporary outage" in action["last_error"]
    assert event["status"] == "retrying"
    assert event["attempts"] == 1
    assert "RuntimeError: temporary outage" in event["last_error"]


def test_action_executor_exception_marks_failed_after_dead_letter(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="unstable",
        payload={},
        approval_policy="auto",
        now=100,
    )
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id},
        idempotency_key="execute:dead-letter",
        max_attempts=1,
        now=101,
    )
    assert event_id is not None
    row = claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=101,
    )[0]

    asyncio.run(process_action_event(conn, row, registry=ActionRegistry({"unstable": FailingExecutor()})))

    action = get_action(conn, action_id)
    event = conn.execute("SELECT status, attempts, last_error FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert action is not None
    assert action["status"] == "failed"
    assert "RuntimeError: temporary outage" in action["last_error"]
    assert event["status"] == "dead_letter"
    assert event["attempts"] == 1
    assert "RuntimeError: temporary outage" in event["last_error"]


def test_action_permanent_failure_result_marks_failed_without_retry(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="invalid",
        payload={},
        approval_policy="auto",
        now=100,
    )
    event_id = enqueue(
        conn,
        tenant_id="tenant-a",
        recipient="actions",
        message_type="ExecuteAction",
        payload={"action_id": action_id},
        idempotency_key="execute:invalid",
        max_attempts=5,
        now=101,
    )
    assert event_id is not None
    row = claim_due(
        conn,
        recipient="actions",
        limit=1,
        lease_owner="test",
        lease_seconds=30,
        now=101,
    )[0]

    asyncio.run(process_action_event(conn, row, registry=ActionRegistry({"invalid": PermanentFailureExecutor()})))

    action = get_action(conn, action_id)
    event = conn.execute("SELECT status, attempts, last_error FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    assert action is not None
    assert action["status"] == "failed"
    assert action["last_error"] == "invalid action payload"
    assert json.loads(action["result_json"]) == {"error": "invalid action payload"}
    assert event["status"] == "done"
    assert event["attempts"] == 1
    assert event["last_error"] is None


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

    async def mark_retry(self, event_id: str, *, run_after: int, last_error: str) -> str:
        self.retries.append((event_id, last_error))
        return "retrying"


class RefusingRetryActionStore:
    def __init__(self, *, refused_status: str) -> None:
        self.status = "approved"
        self.refused_status = refused_status
        self.failed: list[str] = []

    async def insert(self, **kwargs):
        return "act_fake", True

    async def get(self, action_id: str) -> ActionRecord | None:
        return ActionRecord(
            id=action_id,
            tenant_id="tenant-a",
            kind="unstable",
            payload={},
            status=self.status,
            approval_policy="auto",
        )

    async def approve(self, action_id: str, *, approver_id: str) -> bool:
        return False

    async def deny(self, action_id: str, *, approver_id: str) -> bool:
        return False

    async def mark_executing(self, action_id: str) -> bool:
        self.status = "executing"
        return True

    async def mark_queued(self, action_id: str, *, result: dict | None = None) -> None:
        self.status = "queued"

    async def mark_executed(self, action_id: str, *, result: dict) -> None:
        self.status = "executed"

    async def mark_failed(self, action_id: str, *, error: str) -> None:
        self.status = "failed"
        self.failed.append(error)

    async def mark_retryable(self, action_id: str, *, error: str) -> bool:
        self.status = self.refused_status
        return False


def test_retry_refusal_closes_stale_event_for_terminal_action():
    store = RefusingRetryActionStore(refused_status="executed")
    queue = FakeQueue("act_1")

    asyncio.run(
        process_action_queue_event(
            store,
            queue.events[0],
            registry=ActionRegistry({"unstable": FailingExecutor()}),
            queue=queue,
        )
    )

    assert store.status == "executed"
    assert store.failed == []
    assert queue.done == ["evt_1"]
    assert queue.retries == []


def test_retry_refusal_fails_loudly_for_non_terminal_action():
    store = RefusingRetryActionStore(refused_status="queued")
    queue = FakeQueue("act_1")

    with pytest.raises(RuntimeError, match="could not be moved to retryable state"):
        asyncio.run(
            process_action_queue_event(
                store,
                queue.events[0],
                registry=ActionRegistry({"unstable": FailingExecutor()}),
                queue=queue,
            )
        )

    assert store.status == "queued"
    assert store.failed == []
    assert queue.done == []
    assert queue.retries == []


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
