from typing import Literal

from agent_platform.decisions import (
    ActionDecisionHandler,
    BaseDecision,
    CronDecisionHandler,
    DecisionDispatcher,
    DispatchContext,
    OutboundDecisionHandler,
    SessionMailboxDecisionHandler,
)
from agent_platform.sessions.store import insert_session
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class CreateTaskDecision(BaseDecision):
    kind: Literal["create_task"]
    title: str


class SendPromptDecision(BaseDecision):
    kind: Literal["send_prompt"]
    session_id: str
    prompt: str


class ScheduleDecision(BaseDecision):
    kind: Literal["schedule"]
    name: str
    run_at: int
    text: str


def _context() -> DispatchContext:
    return DispatchContext(
        tenant_id="tenant-a",
        source_id="chat-1",
        run_id="run-1",
        source_event_id="evt-1",
        actor_id="user-1",
    )


def test_dispatch_reply_to_outbound_message(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )

    result = dispatcher.dispatch(conn, ReplyDecision(kind="reply", text="hello"), _context())
    row = conn.execute("SELECT * FROM outbound_messages WHERE id = ?", (result.id,)).fetchone()

    assert result.target == "outbound"
    assert row["channel"] == "telegram"
    assert row["destination_id"] == "chat-1"
    assert row["text"] == "hello"


def test_dispatch_auto_action_enqueues_execute_event(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "create_task",
        ActionDecisionHandler(
            action_kind="create_task",
            approval_policy="auto",
            idempotency_key=lambda decision, context: f"task:{decision.title}",
        ),
    )

    result = dispatcher.dispatch(
        conn,
        CreateTaskDecision(kind="create_task", title="Call client"),
        _context(),
    )
    action = conn.execute("SELECT * FROM actions WHERE id = ?", (result.id,)).fetchone()
    event = conn.execute(
        "SELECT * FROM event_queue WHERE recipient = 'actions' AND message_type = 'ExecuteAction'"
    ).fetchone()

    assert result.target == "action"
    assert result.status == "approved"
    assert action["kind"] == "create_task"
    assert action["status"] == "approved"
    assert event is not None


def test_dispatch_session_mailbox_prompt(tmp_path):
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
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "send_prompt",
        SessionMailboxDecisionHandler(session_id="session_id", prompt="prompt"),
    )

    result = dispatcher.dispatch(
        conn,
        SendPromptDecision(kind="send_prompt", session_id=session_id, prompt="continue"),
        _context(),
    )
    row = conn.execute("SELECT * FROM session_mailbox WHERE id = ?", (result.id,)).fetchone()

    assert result.target == "session_mailbox"
    assert row["session_id"] == session_id
    assert row["prompt"] == "continue"


def test_dispatch_cron_job(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "schedule",
        CronDecisionHandler(
            name="name",
            run_at="run_at",
            payload=lambda decision, context: {"text": decision.text, "source_id": context.source_id},
        ),
    )

    result = dispatcher.dispatch(
        conn,
        ScheduleDecision(kind="schedule", name="reminder", run_at=123, text="ping"),
        _context(),
    )
    row = conn.execute("SELECT * FROM cron_jobs WHERE id = ?", (result.id,)).fetchone()

    assert result.target == "cron"
    assert row["name"] == "reminder"
    assert row["run_at"] == 123

