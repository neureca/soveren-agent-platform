import json

from soveren_agent_platform.actions.store import insert_action
from soveren_agent_platform.agent.contracts import AgentEvent
from soveren_agent_platform.context import ContextLimits, build_planner_context
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
    enqueue_outbound(
        conn,
        tenant_id="tenant-a",
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
        name="daily_digest",
        payload={"chat_id": "chat-1"},
        run_at=200,
        now=140,
    )

    context = build_planner_context(
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

    assert context.trigger["source_id"] == "chat-1"
    assert context.batch is not None
    assert context.batch["message_count"] == 1
    assert context.sessions[0]["mailbox"]["queued"] == 1
    assert context.mailbox[0]["prompt"] == "queued prompt"
    assert context.actions[0]["id"] == action_id
    assert "token" not in context.actions[0]["payload"]
    assert context.outbound[0]["text"] == "pending approval"
    assert context.cron[0]["name"] == "daily_digest"
    json.dumps(context.to_dict())


def test_rich_context_builder_honors_text_limit(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    context = build_planner_context(
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

    assert context.batch is not None
    assert context.batch["text"] == "a..."
