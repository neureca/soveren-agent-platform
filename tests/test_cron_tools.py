import asyncio
import json

from soveren_agent_platform.cron import (
    SCHEDULE_TOOL_NAMESPACE,
    SQLiteCronStore,
    register_scheduled_job_tools,
)
from soveren_agent_platform.cron.store import insert_job
from soveren_agent_platform.sessions import DynamicToolRegistry
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def _tool_params(name: str, arguments: dict[str, object]) -> dict[str, object]:
    return {
        "callId": "call-1",
        "threadId": "thread-1",
        "turnId": "turn-1",
        "namespace": SCHEDULE_TOOL_NAMESPACE,
        "tool": name,
        "arguments": arguments,
    }


def _json_result(result: dict[str, object]) -> dict[str, object]:
    content_items = result["contentItems"]
    assert isinstance(content_items, list)
    item = content_items[0]
    assert isinstance(item, dict)
    payload = json.loads(str(item["text"]))
    assert isinstance(payload, dict)
    return payload


def test_scheduled_job_tools_are_bound_to_one_conversation(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    visible_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="tomorrow-reminder",
        payload={"text": "private reminder"},
        run_at=200,
        now=90,
    )
    hidden_id, _ = insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-2",
        name="other-chat-reminder",
        payload={"text": "must stay hidden"},
        run_at=100,
        now=90,
    )
    registry = DynamicToolRegistry()
    register_scheduled_job_tools(
        registry,
        SQLiteCronStore._from_connection(conn),
        tenant_id="tenant-a",
        source_id="chat-1",
    )

    listed = _json_result(
        asyncio.run(
            registry.call(_tool_params("list_scheduled_jobs", {})),
        )
    )
    hidden_cancel = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "cancel_scheduled_job",
                    {"job_id": hidden_id},
                )
            )
        )
    )
    visible_cancel = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "cancel_scheduled_job",
                    {"job_id": visible_id},
                )
            )
        )
    )

    assert registry.conversation == ("tenant-a", "chat-1")
    assert listed == {
        "jobs": [
            {
                "id": visible_id,
                "name": "tomorrow-reminder",
                "rrule": None,
                "run_at": 200,
                "status": "pending",
                "timezone": "UTC",
            },
        ],
    }
    assert "private reminder" not in json.dumps(listed)
    assert hidden_cancel == {"job_id": hidden_id, "outcome": "not_found"}
    assert visible_cancel == {"job_id": visible_id, "outcome": "cancelled"}
    assert conn.execute(
        "SELECT status FROM cron_jobs WHERE id = ?",
        (hidden_id,),
    ).fetchone()["status"] == "pending"


def test_scheduled_job_tool_schemas_do_not_accept_model_provided_scope(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    registry = DynamicToolRegistry()
    register_scheduled_job_tools(
        registry,
        SQLiteCronStore._from_connection(conn),
        tenant_id="tenant-a",
        source_id="chat-1",
    )

    specs = {spec.name: spec for spec in registry.specs()}

    assert set(specs) == {"list_scheduled_jobs", "cancel_scheduled_job"}
    assert specs["list_scheduled_jobs"].input_schema["additionalProperties"] is False
    assert specs["cancel_scheduled_job"].input_schema["additionalProperties"] is False
    assert set(specs["list_scheduled_jobs"].input_schema["properties"]) == {
        "limit",
    }
    assert set(specs["cancel_scheduled_job"].input_schema["properties"]) == {
        "job_id",
    }
