import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_llm_checkpoint import FakeResumableBackend, NativeWorld, _journal, _request

from soveren_agent_platform.agent.worker import _handle_event
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.dialogue import DialogueInput, DialoguePlan, DialogueRoute, SQLiteDialogueStore
from soveren_agent_platform.llm.backends.session import ExecutionBudgetExceededError, SessionLlmBackend
from soveren_agent_platform.queue import SQLiteEventQueue
from soveren_agent_platform.runtime.failures import NonRetryableEventError
from soveren_agent_platform.sessions import CodexAppServerBackend, SendReceipt, SessionIdleTimeoutError
from soveren_agent_platform.sessions.backends.codex_app_server import JsonRpcStdioClient, TurnState
from soveren_agent_platform.storage import bootstrap_platform_storage


def test_total_budget_survives_retry_and_exhaustion_does_not_open_another_thread(monkeypatch):
    import soveren_agent_platform.llm.backends.session as module

    clock = [1_800_000_000.0]
    monkeypatch.setattr(module.time, "time", lambda: clock[0])

    async def scenario():
        journal, context = _journal()
        world = NativeWorld()
        request = replace(_request(journal, context), total_timeout_s=600)
        with pytest.raises(TimeoutError):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        assert journal.checkpoint.state == "paused"
        deadline = journal.checkpoint.deadline_at
        assert deadline == clock[0] + 600
        clock[0] += 601
        with pytest.raises(ExecutionBudgetExceededError):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        assert journal.checkpoint.deadline_at == deadline
        assert journal.checkpoint.state == "failed"
        assert len(world.opened) == 1 and len(world.prompts) == 1
        assert world.resumed == []
        assert len(world.aborted) == 1

    asyncio.run(scenario())


def test_completed_retry_keeps_original_deadline_and_exact_history():
    async def scenario():
        journal, context = _journal()
        world = NativeWorld()
        request = replace(_request(journal, context), total_timeout_s=600)
        with pytest.raises(TimeoutError):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        deadline = journal.checkpoint.deadline_at
        result = await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        assert result.text.endswith('"continued"}')
        assert journal.checkpoint.deadline_at == deadline
        assert journal.checkpoint.turn_index == 1
        assert len(world.opened) == 1
        assert world.prompts[0][1] != world.prompts[1][1]

    asyncio.run(scenario())


def test_active_turn_total_deadline_is_terminal_after_exact_pause():
    async def scenario():
        journal, context = _journal()
        world = NativeWorld()
        request = replace(_request(journal, context, timeout_s=1), total_timeout_s=0.02)
        with pytest.raises(ExecutionBudgetExceededError):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        assert world.pause_started.is_set()
        assert journal.checkpoint.state == "failed"
        assert len(world.aborted) == 1

    asyncio.run(scenario())


def test_native_watchdog_overrides_short_legacy_turn_limit_and_observes_progress():
    async def scenario():
        state = TurnState("turn")
        client = SimpleNamespace(last_turn=lambda _: state, release_turn=lambda *_: None)
        backend = CodexAppServerBackend(client=client, turn_timeout_s=0.005)

        async def produce():
            for _ in range(8):
                await asyncio.sleep(0.01)
                state.last_progress_at = __import__("time").perf_counter()
            state.text_parts.append("finished")
            state.done.set()

        producer = asyncio.create_task(produce())
        result = await backend.capture_delivery_with_watchdog(
            "thread",
            SendReceipt("turn"),
            timeout_s=1,
            idle_timeout_s=0.04,
        )
        await producer
        assert result.text == "finished" and not result.timed_out
        assert backend.turn_timeout_s == 0.005

    asyncio.run(scenario())


def test_idle_turn_is_interrupted_without_treating_heartbeats_as_progress():
    async def scenario():
        state = TurnState("turn")
        client = SimpleNamespace(last_turn=lambda _: state, release_turn=lambda *_: None)
        backend = CodexAppServerBackend(client=client)
        with pytest.raises(SessionIdleTimeoutError):
            await backend.capture_delivery_with_watchdog(
                "thread",
                SendReceipt("turn"),
                timeout_s=1,
                idle_timeout_s=0.015,
            )
        assert not state.done.is_set()

    asyncio.run(scenario())


def test_native_progress_is_exact_turn_and_meaningful_notification_only(monkeypatch):
    client = JsonRpcStdioClient(command=[], cwd=None, env={}, request_timeout_s=1)
    state = TurnState("turn", last_progress_at=1)
    client._turns[("thread", "turn")] = state
    monkeypatch.setattr("soveren_agent_platform.sessions.backends.codex_app_server.time.perf_counter", lambda: 9)
    for method, params in (
        ("heartbeat", {"threadId": "thread", "turnId": "turn"}),
        ("item/reasoning/textDelta", {"threadId": "other", "turnId": "turn", "delta": "x"}),
        ("item/reasoning/textDelta", {"threadId": "thread", "turnId": "other", "delta": "x"}),
        ("item/reasoning/textDelta", {"threadId": "thread", "turnId": "turn", "delta": ""}),
    ):
        client._handle_notification({"method": method, "params": params})
        assert state.last_progress_at == 1
    client._handle_notification(
        {"method": "item/reasoning/textDelta", "params": {"threadId": "thread", "turnId": "turn", "delta": "x"}}
    )
    assert state.last_progress_at == 9


@pytest.mark.parametrize(
    "maximum,outcome,error",
    [
        (2, "retrying", TimeoutError()),
        (1, "dead_letter", TimeoutError()),
        (5, "dead_letter", NonRetryableEventError("terminal")),
    ],
)
def test_queue_outcome_follows_fenced_persisted_transition(tmp_path, maximum, outcome, error):
    async def scenario():
        path = tmp_path / "app.db"
        await bootstrap_platform_storage(path)
        queue = await SQLiteEventQueue.open(path)
        await queue.enqueue(
            tenant_id="t",
            recipient="agent",
            message_type="Test",
            payload={"source_id": "s", "task_ref": "task"},
            idempotency_key="one",
            max_attempts=maximum,
            correlation_id="task",
        )
        row = (await queue.claim_due(recipient="agent", limit=1, lease_owner="owner", lease_seconds=30))[0]
        observed = []

        class Handler:
            async def handle(self, event):
                raise error

            async def on_event_outcome(self, event, value):
                saved = await queue.execution_for_reference(
                    ConversationScope("t", "s"), recipient="agent", reference_key="task_ref", reference_value="task"
                )
                assert saved.status == value
                observed.append(value)
                raise RuntimeError("presentation failed after commit")

        assert await _handle_event(queue, row, handler=Handler(), retry_backoff_s=0)
        assert observed == [outcome]
        # An obsolete worker cannot emit another terminal reaction.
        row.lease_token = "obsolete"
        assert not await _handle_event(queue, row, handler=Handler(), retry_backoff_s=0)
        assert observed == [outcome]
        assert (
            await queue.execution_for_reference(
                ConversationScope("foreign", "s"), recipient="agent", reference_key="task_ref", reference_value="task"
            )
            is None
        )
        assert (
            await queue.execution_for_reference(
                ConversationScope("t", "foreign"), recipient="agent", reference_key="task_ref", reference_value="task"
            )
            is None
        )
        await queue.close()

    asyncio.run(scenario())


def test_status_route_does_not_reopen_terminal_task_or_change_focus_or_history(tmp_path):
    async def scenario():
        path = tmp_path / "app.db"
        await bootstrap_platform_storage(path)
        store = await SQLiteDialogueStore.open(path)
        scope = ConversationScope("t", "s")

        def item(i):
            return DialogueInput(i, "actor", i, 1, "telegram")

        result = await store.apply_plan(
            DialoguePlan(
                "create",
                scope,
                (item("first"),),
                (DialogueRoute("first", "create", task_key="new:first", title="analysis"),),
            )
        )
        task_id = result.assignments[0].task_id
        await store.apply_plan(
            DialoguePlan("cancel", scope, (item("cancel"),), (DialogueRoute("cancel", "cancel", task_id=task_id),))
        )
        before = await store.get_context(scope, task_id)
        focus = await store.get_focus(scope, "actor")
        plan = DialoguePlan(
            "status",
            scope,
            (item("status"),),
            (DialogueRoute("status", "status", task_id=task_id, response_text="Cancelled"),),
        )
        first = await store.apply_plan(plan)
        assert await store.apply_plan(plan) == first
        assert await store.get_context(scope, task_id) == before
        assert await store.get_focus(scope, "actor") == focus
        assert (await store.get_task(scope, task_id)).status == "cancelled"
        await store.close()

    asyncio.run(scenario())


def test_status_snapshot_prefers_active_work_and_orders_equal_timestamp_insertions(tmp_path):
    from soveren_agent_platform.queue import durable
    from soveren_agent_platform.storage.sqlite import open_sqlite

    async def scenario():
        path = tmp_path / "order.db"
        await bootstrap_platform_storage(path)
        conn = open_sqlite(path)
        for name, identifier in [("old", "evt_ffff"), ("new", "evt_0000")]:
            identifier_created = durable.enqueue(
                conn,
                tenant_id="t",
                recipient="worker",
                message_type="Analysis",
                payload={"source_id": "s", "task_ref": "task"},
                idempotency_key=name,
                now=100,
            )
            conn.execute("UPDATE event_queue SET id=? WHERE id=?", (identifier, identifier_created))
        conn.execute("UPDATE event_queue SET status='done' WHERE id='evt_ffff'")
        conn.commit()
        queue = await SQLiteEventQueue.open(path)

        async def snapshot():
            return await queue.execution_for_reference(
                ConversationScope("t", "s"), recipient="worker", reference_key="task_ref", reference_value="task"
            )

        assert (await snapshot()).event_id == "evt_0000"
        conn.execute("UPDATE event_queue SET status='leased',lease_until=9999999999 WHERE id='evt_ffff'")
        conn.commit()
        assert (await snapshot()).event_id == "evt_ffff"
        conn.execute("UPDATE event_queue SET status='done',lease_until=NULL")
        conn.commit()
        assert (await snapshot()).event_id == "evt_0000"
        assert conn.execute("SELECT count(*) FROM event_queue WHERE correlation_id IS NULL").fetchone()[0] == 2
        with pytest.raises(ValueError):
            await queue.execution_for_reference(
                ConversationScope("t", "s"), recipient="worker", reference_key="bad.path'", reference_value="task"
            )
        conn.close()
        await queue.close()

    asyncio.run(scenario())


def test_setup_failure_keeps_deadline_before_native_thread_exists(monkeypatch):
    import soveren_agent_platform.llm.backends.session as module

    clock = [1800000000.0]
    monkeypatch.setattr(module.time, "time", lambda: clock[0])

    async def scenario():
        journal, context = _journal()
        world = NativeWorld()

        class SetupFailure(FakeResumableBackend):
            async def open(self, spec):
                raise TimeoutError("thread setup timed out")

        request = replace(_request(journal, context), total_timeout_s=600)
        with pytest.raises(TimeoutError):
            await SessionLlmBackend(SetupFailure(world), kind="codex").run(request)
        assert journal.checkpoint.session_id is None
        deadline = journal.checkpoint.deadline_at
        clock[0] += 601
        with pytest.raises(ExecutionBudgetExceededError):
            await SessionLlmBackend(FakeResumableBackend(world), kind="codex").run(request)
        assert journal.checkpoint.deadline_at == deadline
        assert journal.checkpoint.state == "failed"
        assert not world.opened

    asyncio.run(scenario())
