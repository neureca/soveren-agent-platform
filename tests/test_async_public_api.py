from __future__ import annotations

import asyncio
import inspect

import soveren_agent_platform.actions as actions
import soveren_agent_platform.approvals as approvals
import soveren_agent_platform.batching as batching
import soveren_agent_platform.context as context
import soveren_agent_platform.cron as cron
import soveren_agent_platform.decisions as decisions
import soveren_agent_platform.memory as memory
import soveren_agent_platform.outbound as outbound
import soveren_agent_platform.queue as queue
import soveren_agent_platform.reconciliation as reconciliation
import soveren_agent_platform.runs as runs
import soveren_agent_platform.runtime as runtime
import soveren_agent_platform.sessions as sessions
import soveren_agent_platform.storage as storage
import soveren_agent_platform.telegram as telegram
from soveren_agent_platform.queue import SQLiteEventQueue


def test_public_packages_do_not_export_synchronous_storage_operations() -> None:
    removed_exports = {
        actions: {
            "approve_action",
            "deny_action",
            "get_action",
            "insert_action",
            "mark_executed",
            "mark_failed",
            "mark_retryable",
            "mark_uncertain",
        },
        approvals: {"approve_action", "approve_action_and_enqueue", "deny_action"},
        batching: {"append_inbound_message", "load_state"},
        cron: {"claim_due_jobs", "complete_job", "fail_job", "insert_job"},
        memory: {"forget_memory", "get_memory", "remember", "search_memory"},
        outbound: {"enqueue_outbound"},
        runs: {"finalize_run", "insert_run"},
        context: {"RichContextBuilder", "build_planner_context"},
        runtime: {"run_planner_dispatch_turn", "run_planner_turn"},
        sessions: {
            "close_idle_sessions",
            "close_session",
            "drain_once",
            "enqueue_prompt",
            "record_session_event",
            "recover_stale_closing_sessions",
            "register_session_directory_tools",
        },
        storage: {"open_sqlite"},
        telegram: {"register_telegram_chat", "telegram_chat_registered"},
    }
    for module, names in removed_exports.items():
        for name in names:
            assert name not in module.__all__
            assert not hasattr(module, name)


def test_public_storage_entrypoints_are_async() -> None:
    assert inspect.iscoroutinefunction(storage.bootstrap_platform_storage)
    assert inspect.iscoroutinefunction(SQLiteEventQueue.open)
    assert inspect.iscoroutinefunction(telegram.SQLiteTelegramChatRegistry.open)
    assert inspect.iscoroutinefunction(telegram.enqueue_telegram_text)
    assert inspect.iscoroutinefunction(telegram.enqueue_telegram_message)
    assert inspect.iscoroutinefunction(telegram.enqueue_telegram_update)
    assert inspect.iscoroutinefunction(telegram.create_telegram_agent_app)
    assert inspect.iscoroutinefunction(sessions.SQLiteSessionLifecycle.open)
    assert inspect.iscoroutinefunction(sessions.SQLiteSessionLifecycle.close_session)
    assert inspect.iscoroutinefunction(sessions.SQLiteSessionIndexStore.open)
    assert inspect.iscoroutinefunction(runtime.PlannerRuntime.run_turn)


def test_public_package_signatures_do_not_expose_sqlite_connections() -> None:
    modules = (
        actions,
        approvals,
        batching,
        context,
        cron,
        decisions,
        memory,
        outbound,
        queue,
        reconciliation,
        runs,
        runtime,
        sessions,
        storage,
        telegram,
    )
    for module in modules:
        for name in module.__all__:
            value = getattr(module, name)
            if not callable(value):
                continue
            try:
                signature = str(inspect.signature(value))
            except (TypeError, ValueError):
                continue
            assert "sqlite3.Connection" not in signature, f"{module.__name__}.{name}{signature}"


def test_async_sqlite_adapters_open_operate_and_close(tmp_path) -> None:
    async def run() -> None:
        db_path = tmp_path / "app.db"
        await storage.bootstrap_platform_storage(db_path)

        events = await SQLiteEventQueue.open(db_path)
        async with events:
            assert not hasattr(events, "conn")
            event_id = await events.enqueue(
                tenant_id="tenant-a",
                recipient="batching",
                message_type="InboundMessageReceived",
                payload={"text": "hello"},
                idempotency_key="event-1",
            )
            assert event_id is not None

        registry = await telegram.SQLiteTelegramChatRegistry.open(db_path)
        async with registry:
            await registry.register(
                tenant_id="tenant-a",
                chat_id=123,
                registered_by_user_id=456,
            )
            assert await registry.is_registered(tenant_id="tenant-a", chat_id=123)

        adapters = [
            await context.SQLitePlannerContextBuilder.open(db_path),
            await sessions.DeterministicSessionRouter.open(db_path),
            await sessions.SQLiteSessionDirectoryTools.open(db_path),
            await sessions.SQLiteSessionIndexStore.open(db_path),
            await sessions.SQLiteSessionLifecycle.open(db_path, session_backends={}),
        ]
        for adapter in adapters:
            assert not hasattr(adapter, "conn")
            await adapter.close()

    asyncio.run(run())
