import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Literal

import pytest

import soveren_agent_platform.decisions.sqlite as decision_sqlite_module
from soveren_agent_platform.decisions import (
    ActionDecisionHandler,
    BaseDecision,
    CronDecisionHandler,
    DecisionDispatcher,
    DecisionEffects,
    DispatchContext,
    DispatchResult,
    OutboundDecisionHandler,
    SessionMailboxDecisionHandler,
)
from soveren_agent_platform.decisions.sqlite import sqlite_decision_effects
from soveren_agent_platform.outbound import OutboundEnqueueResult
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


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


class FakeOutboundQueue:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def enqueue_with_result(self, **kwargs):
        self.calls.append(kwargs)
        return OutboundEnqueueResult(message_id="out_1", created=True)


class LegacyOutboundQueue:
    async def enqueue(self, **kwargs):
        return "out_legacy"


def _context() -> DispatchContext:
    return DispatchContext(
        tenant_id="tenant-a",
        source_id="chat-1",
        run_id="run-1",
        source_event_id="evt-1",
        actor_id="user-1",
    )


def test_dispatch_contract_rejects_non_json_metadata():
    with pytest.raises(TypeError, match="non-JSON value datetime"):
        DispatchResult(
            target="test",
            id="effect-1",
            metadata={"completed_at": datetime(2026, 1, 1, tzinfo=timezone.utc)},  # type: ignore[dict-item]
        )


def test_dispatcher_uses_effect_ports_without_sqlite():
    outbound = FakeOutboundQueue()
    effects = DecisionEffects(
        actions=SimpleNamespace(),
        outbound=outbound,
        events=SimpleNamespace(),
        session_mailbox=SimpleNamespace(),
        cron=SimpleNamespace(),
    )
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id=lambda decision, context: context.source_id,
            text="text",
        ),
    )

    result = asyncio.run(
        dispatcher.dispatch(
            effects,
            ReplyDecision(kind="reply", text="hello"),
            _context(),
        )
    )

    assert result.id == "out_1"
    assert outbound.calls[0]["destination_id"] == "chat-1"
    assert outbound.calls[0]["text"] == "hello"


def test_dispatcher_keeps_legacy_outbound_queue_compatibility():
    effects = DecisionEffects(
        actions=SimpleNamespace(),
        outbound=LegacyOutboundQueue(),
        events=SimpleNamespace(),
        session_mailbox=SimpleNamespace(),
        cron=SimpleNamespace(),
    )
    dispatcher = DecisionDispatcher()
    dispatcher.register(
        "reply",
        OutboundDecisionHandler(
            channel="telegram",
            destination_id="chat-1",
            text="text",
        ),
    )

    result = asyncio.run(
        dispatcher.dispatch(
            effects,
            ReplyDecision(kind="reply", text="hello"),
            _context(),
        )
    )

    assert result.id == "out_legacy"
    assert result.created is True


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

    result = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            ReplyDecision(kind="reply", text="hello"),
            _context(),
        )
    )
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

    result = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            CreateTaskDecision(kind="create_task", title="Call client"),
            _context(),
        )
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


def test_dispatch_auto_action_rolls_back_when_execute_enqueue_fails(tmp_path, monkeypatch):
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

    def raise_on_enqueue(*args, **kwargs):
        raise RuntimeError("queue write failed")

    monkeypatch.setattr(decision_sqlite_module, "enqueue", raise_on_enqueue)

    with pytest.raises(RuntimeError, match="queue write failed"):
        asyncio.run(
            dispatcher.dispatch(
                sqlite_decision_effects(conn),
                CreateTaskDecision(kind="create_task", title="Call client"),
                _context(),
            )
        )

    action = conn.execute("SELECT * FROM actions WHERE idempotency_key = ?", ("task:Call client",)).fetchone()
    event = conn.execute(
        "SELECT * FROM event_queue WHERE recipient = 'actions' AND message_type = 'ExecuteAction'"
    ).fetchone()
    assert action is None
    assert event is None


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

    result = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            SendPromptDecision(kind="send_prompt", session_id=session_id, prompt="continue"),
            _context(),
        )
    )
    row = conn.execute("SELECT * FROM session_mailbox WHERE id = ?", (result.id,)).fetchone()

    assert result.target == "session_mailbox"
    assert row["session_id"] == session_id
    assert row["prompt"] == "continue"

    replay = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            SendPromptDecision(kind="send_prompt", session_id=session_id, prompt="continue"),
            _context(),
        )
    )
    assert replay.id == result.id
    assert replay.created is False
    assert conn.execute("SELECT COUNT(*) FROM session_mailbox").fetchone()[0] == 1


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

    result = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            ScheduleDecision(kind="schedule", name="reminder", run_at=123, text="ping"),
            _context(),
        )
    )
    row = conn.execute("SELECT * FROM cron_jobs WHERE id = ?", (result.id,)).fetchone()

    assert result.target == "cron"
    assert row["name"] == "reminder"
    assert row["run_at"] == 123

    replay = asyncio.run(
        dispatcher.dispatch(
            sqlite_decision_effects(conn),
            ScheduleDecision(kind="schedule", name="reminder", run_at=123, text="ping"),
            _context(),
        )
    )
    assert replay.id == result.id
    assert replay.created is False
    assert conn.execute("SELECT COUNT(*) FROM cron_jobs").fetchone()[0] == 1
