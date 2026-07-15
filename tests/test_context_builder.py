import asyncio
import json

from soveren_agent_platform.actions.store import insert_action
from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.batching import InboundMessage
from soveren_agent_platform.batching.store import append_inbound_message
from soveren_agent_platform.context import (
    ContextLimits,
    ModelRedactionPolicy,
    redact_agent_event_for_model,
    redact_planner_context_for_model,
)
from soveren_agent_platform.context.builder import build_planner_context
from soveren_agent_platform.cron.store import insert_job
from soveren_agent_platform.outbound.store import enqueue_outbound
from soveren_agent_platform.sessions.mailbox import enqueue_prompt
from soveren_agent_platform.sessions.routing import RouteHint, SessionRouteResult, SessionSnapshot
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_rich_context_builder_collects_platform_state(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        kind="codex_cli",
        backend="codex_app_server",
        backend_session_id="thread-1",
        title="soveren-agent-platform",
        cwd="/repo",
        status="busy",
        now=100,
    )
    enqueue_prompt(
        conn,
        session_id=session_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        prompt="queued prompt",
        now=110,
    )
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="send_cli_prompt",
        source_id="chat-1",
        source_event_id="evt_1",
        payload={"text": "run tests", "token": "secret-token"},
        now=120,
    )
    conn.execute(
        "UPDATE actions SET status = 'uncertain', last_error = 'outcome unknown' WHERE id = ?",
        (action_id,),
    )
    enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="pending approval",
        idempotency_key="out-1",
        correlation_id="corr-1",
        now=130,
    )
    insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        name="daily_digest",
        payload={"chat_id": "chat-1"},
        run_at=200,
        now=140,
    )

    context = asyncio.run(
        build_planner_context(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent_core",
                message_type="ChatBatchReady",
                correlation_id="corr-1",
                payload={
                    "source_id": "chat-1",
                    "channel": "telegram",
                    "user_id": "user-1",
                    "text": "continue",
                    "batch_id": "batch-1",
                    "batch_message_count": 1,
                    "batch_messages": [{"text": "continue", "raw_event_id": "tg-1"}],
                },
            ),
            route_result=SessionRouteResult(
                snapshots=[
                    SessionSnapshot(
                        session_id=session_id,
                        kind="codex_cli",
                        backend="codex_app_server",
                        status="busy",
                        title="soveren-agent-platform",
                    )
                ],
                hint=RouteHint(action="route_existing", confidence=0.9, session_id=session_id),
            ),
        )
    )

    assert context.trigger["source_id"] == "chat-1"
    assert context.batch is not None
    assert context.batch["message_count"] == 1
    assert context.sessions[0]["mailbox"]["queued"] == 1
    assert context.mailbox[0]["prompt"] == "queued prompt"
    assert context.actions[0]["id"] == action_id
    assert context.actions[0]["status"] == "uncertain"
    assert "token" not in context.actions[0]["payload"]
    assert context.outbound[0]["text"] == "pending approval"
    assert context.cron[0]["name"] == "daily_digest"
    json.dumps(context.to_dict())


def test_rich_context_builder_honors_text_limit(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    context = asyncio.run(
        build_planner_context(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent_core",
                message_type="TelegramMessageReceived",
                payload={"source_id": "chat-1", "text": "abcdef"},
            ),
            route_result=SessionRouteResult(
                snapshots=[],
                hint=RouteHint(action="no_match", confidence=0),
            ),
            limits=ContextLimits(max_text_chars=4),
        )
    )

    assert context.batch is not None
    assert context.batch["text"] == "a..."


def test_planner_context_does_not_load_a_batch_from_another_tenant(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    batch_id = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-b",
            channel="telegram",
            source_id="chat-b",
            raw_event_id="tenant-b-message",
            text="tenant-b-secret",
            payload={},
            message_at=100,
        ),
    )
    assert batch_id is not None

    context = asyncio.run(
        build_planner_context(
            conn,
            event=AgentEvent(
                id="evt-a",
                tenant_id="tenant-a",
                recipient="agent_core",
                message_type="ChatBatchReady",
                payload={"source_id": "chat-a", "batch_id": batch_id},
            ),
            route_result=SessionRouteResult(
                snapshots=[],
                hint=RouteHint(action="no_match", confidence=0),
            ),
        )
    )

    assert context.batch is None
    assert "tenant-b-secret" not in json.dumps(context.to_dict(), ensure_ascii=False)


def test_planner_context_does_not_load_state_from_another_conversation(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    other_session_id = insert_session(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        kind="codex_cli",
        backend="codex_app_server",
        backend_session_id="thread-b",
        title="private-chat-b",
    )
    other_batch_id = append_inbound_message(
        conn,
        InboundMessage(
            tenant_id="tenant-a",
            channel="telegram",
            source_id="chat-b",
            raw_event_id="chat-b-message",
            text="chat-b-secret",
            payload={},
            message_at=100,
        ),
    )
    assert other_batch_id is not None
    insert_action(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        kind="private-action",
        payload={"secret": "chat-b-action"},
        source_event_id="evt-a",
    )
    enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        channel="telegram",
        destination_id="chat-a",
        text="chat-b-outbound",
        idempotency_key="chat-b-outbound",
        correlation_id="evt-a",
    )
    insert_job(
        conn,
        tenant_id="tenant-a",
        source_id="chat-b",
        name="chat-b-cron",
        payload={"secret": "chat-b-cron"},
        run_at=200,
    )

    context = asyncio.run(
        build_planner_context(
            conn,
            event=AgentEvent(
                id="evt-a",
                tenant_id="tenant-a",
                recipient="agent_core",
                message_type="ChatBatchReady",
                correlation_id="evt-a",
                payload={"source_id": "chat-a", "batch_id": other_batch_id},
            ),
            route_result=SessionRouteResult(
                snapshots=[
                    SessionSnapshot(
                        session_id=other_session_id,
                        kind="codex_cli",
                        backend="codex_app_server",
                        status="idle",
                        title="private-chat-b",
                    )
                ],
                hint=RouteHint(
                    action="route_existing",
                    confidence=1,
                    session_id=other_session_id,
                ),
            ),
        )
    )

    assert context.batch is None
    assert context.sessions == []
    assert context.actions == []
    assert context.outbound == []
    assert context.cron == []
    assert context.session_routing["route_hint"]["action"] == "no_match"
    assert "chat-b" not in json.dumps(context.to_dict(), ensure_ascii=False)


def test_model_redaction_removes_external_identifiers_from_planner_context(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    action_id, _ = insert_action(
        conn,
        tenant_id="tenant-a",
        kind="approve",
        source_id="123",
        source_event_id="evt_1",
        payload={},
    )
    conn.execute(
        "UPDATE actions SET approved_by = '789' WHERE id = ?",
        (action_id,),
    )
    context = asyncio.run(
        build_planner_context(
            conn,
            event=AgentEvent(
                id="evt_1",
                tenant_id="tenant-a",
                recipient="agent_core",
                message_type="ChatBatchReady",
                correlation_id="telegram:123",
                payload={
                    "source_id": "123",
                    "channel": "telegram",
                    "chat_id": 123,
                    "user_id": 789,
                    "username": "private-user",
                    "text": "deploy please",
                    "batch_messages": [
                        {
                            "text": "deploy please",
                            "user_id": 789,
                            "chat_id": 123,
                            "raw_event_id": "telegram:123:456",
                            "payload": {"raw": {"secret": "telegram-raw"}},
                        }
                    ],
                },
            ),
            route_result=SessionRouteResult(
                snapshots=[],
                hint=RouteHint(action="no_match", confidence=0),
            ),
        )
    )

    redacted_event = redact_agent_event_for_model(
        AgentEvent(
            id="evt_1",
            tenant_id="tenant-a",
            recipient="agent_core",
            message_type="ChatBatchReady",
            correlation_id="telegram:123",
            payload={
                "source_id": "123",
                "chat_id": 123,
                "user_id": 789,
                "username": "private-user",
                "text": "deploy please",
            },
        )
    )
    redacted_context = redact_planner_context_for_model(context)
    payload_dump = json.dumps(redacted_event.payload, ensure_ascii=False)
    context_dump = json.dumps(redacted_context.to_dict(), ensure_ascii=False)

    assert "deploy please" in payload_dump
    assert "deploy please" in context_dump
    assert "789" not in payload_dump
    assert "tenant-a" not in context_dump
    assert "telegram:123" not in payload_dump
    assert "telegram:123" not in context_dump
    assert "telegram-raw" not in context_dump
    assert redacted_context.trigger["user_id"] == "[redacted:user_id]"
    assert redacted_context.trigger["source_id"] == "[redacted:source_id]"
    assert redacted_context.trigger["tenant_id"] == "[redacted:tenant_id]"
    assert redacted_context.batch is not None
    assert redacted_context.batch["messages"][0]["from_user_id"] == "[redacted:from_user_id]"
    assert redacted_context.actions[0]["approved_by"] == "[redacted:approved_by]"
    assert redacted_event.tenant_id == "[redacted:tenant_id]"


def test_agent_event_model_redaction_respects_an_empty_custom_policy():
    event = AgentEvent(
        id="evt_1",
        tenant_id="tenant-a",
        recipient="agent_core",
        message_type="ChatBatchReady",
        correlation_id="corr-1",
        causation_id="cause-1",
        payload={"source_id": "chat-1"},
    )

    redacted = redact_agent_event_for_model(
        event,
        policy=ModelRedactionPolicy(redact_keys=frozenset()),
    )

    assert redacted.tenant_id == "tenant-a"
    assert redacted.correlation_id == "corr-1"
    assert redacted.causation_id == "cause-1"
    assert redacted.payload == {"source_id": "chat-1"}
