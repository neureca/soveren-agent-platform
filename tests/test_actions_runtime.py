import asyncio
import json

from agent_platform.actions.contracts import ActionExecutionResult
from agent_platform.actions.registry import ActionRegistry
from agent_platform.actions.store import approve_action, get_action, insert_action
from agent_platform.actions.worker import process_action_event
from agent_platform.queue.durable import claim_due, enqueue
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(self, conn, action):
        self.calls.append(action["id"])
        payload = json.loads(action["payload_json"])
        return ActionExecutionResult(result={"echo": payload["value"]})


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

