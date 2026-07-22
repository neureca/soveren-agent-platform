import asyncio
import json
import shutil
from pathlib import Path

import pytest

import soveren_agent_platform.batching.store as batching_store_module
import soveren_agent_platform.outbound.store as outbound_store_module
from soveren_agent_platform.batching import InboundMessage
from soveren_agent_platform.batching.store import append_inbound_message
from soveren_agent_platform.conversation_history import (
    CONVERSATION_HISTORY_TOOL_NAMESPACE,
    SQLiteConversationHistoryStore,
    register_conversation_history_tools,
)
from soveren_agent_platform.conversation_history.store import (
    MAX_SEARCH_QUERY_CHARS,
    MAX_SEARCH_TOKENS,
    prune_history_before,
    recent_messages,
    record_message,
    search_messages,
)
from soveren_agent_platform.idempotency import IdempotencyConflictError
from soveren_agent_platform.outbound.store import (
    claim_due,
    enqueue_outbound,
    mark_sending,
    mark_sent,
    mark_uncertain,
)
from soveren_agent_platform.reconciliation.store import resolve_outbound
from soveren_agent_platform.sessions import DynamicToolRegistry
from soveren_agent_platform.storage.migrations import (
    apply_migrations_from_dir,
    apply_platform_migrations,
)
from soveren_agent_platform.storage.sqlite import open_sqlite


def _tool_params(tool: str, arguments: dict) -> dict:
    return {
        "callId": "call-1",
        "threadId": "thread-1",
        "turnId": "turn-1",
        "namespace": CONVERSATION_HISTORY_TOOL_NAMESPACE,
        "tool": tool,
        "arguments": arguments,
    }


def _json_result(result: dict) -> dict:
    return json.loads(result["contentItems"][0]["text"])


def test_conversation_history_search_returns_scoped_neighboring_context(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    for source_id, direction, author_id, text, source_message_id, occurred_at in (
        ("chat-1", "inbound", "user-101", "Обсуждаем повышение тарифа.", "in-1", 100),
        ("chat-1", "inbound", "user-202", "Предлагаю начать с августа.", "in-2", 110),
        ("chat-1", "outbound", None, "Согласовано: поднимаем тариф с августа.", "out-1", 120),
        ("chat-1", "inbound", "user-101", "Иван подготовит уведомление.", "in-3", 130),
        ("chat-2", "inbound", "user-303", "Секретное согласовано в другом чате.", "in-4", 115),
    ):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id=source_id,
            channel="telegram",
            direction=direction,
            author_id=author_id,
            text=text,
            source_message_id=source_message_id,
            occurred_at=occurred_at,
            now=occurred_at,
        )

    hits = search_messages(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        query="согласовано",
        limit=1,
        context_before=2,
        context_after=1,
    )

    assert len(hits) == 1
    assert hits[0].match.text == "Согласовано: поднимаем тариф с августа."
    assert [item.text for item in hits[0].context] == [
        "Обсуждаем повышение тарифа.",
        "Предлагаю начать с августа.",
        "Согласовано: поднимаем тариф с августа.",
        "Иван подготовит уведомление.",
    ]
    assert all(item.source_id == "chat-1" for item in hits[0].context)


def test_conversation_history_search_handles_empty_single_character_and_unicode_queries(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    for text, source_message_id in (
        ("Plan X approved", "in-1"),
        ("ناقشنا قرار الإطلاق", "in-2"),
    ):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            direction="inbound",
            text=text,
            source_message_id=source_message_id,
            occurred_at=100,
            now=100,
        )

    assert search_messages(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        query="",
    ) == []
    assert [
        hit.match.text
        for hit in search_messages(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            query="X",
        )
    ] == ["Plan X approved"]
    assert [
        hit.match.text
        for hit in search_messages(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            query="قرار",
        )
    ] == ["ناقشنا قرار الإطلاق"]
    with pytest.raises(ValueError, match="must not exceed"):
        search_messages(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            query="x" * (MAX_SEARCH_QUERY_CHARS + 1),
        )
    with pytest.raises(ValueError, match="unique tokens"):
        search_messages(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            query=" ".join(f"token{index}" for index in range(MAX_SEARCH_TOKENS + 1)),
        )


def test_conversation_history_migration_backfills_existing_messages(tmp_path):
    old_migrations = tmp_path / "old-migrations"
    old_migrations.mkdir()
    migration_source = (
        Path(__file__).parents[1]
        / "src"
        / "soveren_agent_platform"
        / "storage"
        / "migrations"
        / "platform"
    )
    for migration in sorted(migration_source.glob("*.sql")):
        if not migration.name.startswith("025_"):
            shutil.copy(migration, old_migrations / migration.name)
    conn = open_sqlite(tmp_path / "app.db")
    apply_migrations_from_dir(conn, old_migrations, namespace="platform")
    conn.execute(
        "INSERT INTO inbound_batches"
        " (id, tenant_id, channel, source_id, status, first_message_at, last_message_at,"
        " message_count, created_at, updated_at)"
        " VALUES ('batch-1', 'tenant-a', 'telegram', 'chat-1', 'routed', 100, 100, 1, 100, 100)"
    )
    conn.execute(
        "INSERT INTO inbound_batch_messages"
        " (id, batch_id, tenant_id, channel, source_id, raw_event_id, source_event_id,"
        " payload_json, message_at, created_at)"
        " VALUES ('in-1', 'batch-1', 'tenant-a', 'telegram', 'chat-1', 'raw-1', 'event-1',"
        " ?, 100, 100)",
        (
            json.dumps(
                {
                    "user_id": 101,
                    "username": "ivan_petrov",
                    "from_first_name": "Иван",
                    "from_last_name": "Петров",
                    "text": "Старое решение",
                },
                ensure_ascii=False,
            ),
        ),
    )
    outbound_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="Старый ответ",
        idempotency_key="legacy-reply",
        run_after=100,
        now=100,
    )
    assert outbound_id is not None
    conn.execute(
        "UPDATE outbound_messages SET status = 'sent', sent_at = 110 WHERE id = ?",
        (outbound_id,),
    )

    assert apply_platform_migrations(conn) == ["025_conversation_history"]
    history = recent_messages(conn, tenant_id="tenant-a", source_id="chat-1")

    assert [
        (
            item.direction,
            item.author_id,
            item.author_username,
            item.author_display_name,
            item.text,
        )
        for item in history
    ] == [
        ("inbound", "101", "ivan_petrov", "Иван Петров", "Старое решение"),
        ("outbound", None, None, None, "Старый ответ"),
    ]
    assert len(
        search_messages(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            query="старое",
        )
    ) == 1


def test_conversation_history_record_is_idempotent_and_conversation_scoped(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    first_id, created = record_message(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        direction="inbound",
        author_id="101",
        text="same message",
        source_message_id="provider-1",
        occurred_at=100,
        now=100,
    )
    replay_id, replay_created = record_message(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        direction="inbound",
        author_id="101",
        text="same message",
        source_message_id="provider-1",
        occurred_at=100,
        now=101,
    )
    other_chat_id, other_created = record_message(
        conn,
        tenant_id="tenant-a",
        source_id="chat-2",
        channel="telegram",
        direction="inbound",
        author_id="101",
        text="private second chat",
        source_message_id="provider-1",
        occurred_at=100,
        now=100,
    )

    assert (replay_id, replay_created) == (first_id, False)
    assert other_created is True and other_chat_id != first_id
    with pytest.raises(IdempotencyConflictError):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            direction="inbound",
            author_id="101",
            text="changed replay",
            source_message_id="provider-1",
            occurred_at=100,
            now=102,
        )


def test_recent_history_cursor_does_not_skip_messages_with_the_same_timestamp(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    for index in range(3):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            direction="inbound",
            author_id="101",
            text=f"message-{index}",
            source_message_id=f"provider-{index}",
            occurred_at=100,
            now=100,
        )

    newest_page = recent_messages(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        limit=2,
    )
    older_page = recent_messages(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        limit=2,
        before_message_id=newest_page[0].id,
    )

    assert [item.text for item in newest_page] == ["message-1", "message-2"]
    assert [item.text for item in older_page] == ["message-0"]


def test_history_pruning_keeps_operational_source_records(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    inbound = InboundMessage(
        tenant_id="tenant-a",
        channel="telegram",
        source_id="chat-1",
        raw_event_id="telegram:chat-1:1",
        source_event_id="1",
        text="Old inbound message",
        payload={
            "user_id": 101,
            "username": "ivan_petrov",
            "payload": {"from_first_name": "Иван", "from_last_name": "Петров"},
        },
        message_at=100,
    )
    assert append_inbound_message(conn, inbound) is not None
    outbound_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="Old outbound message",
        idempotency_key="reply:old",
        run_after=100,
        now=100,
    )
    assert outbound_id is not None
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker",
        lease_seconds=30,
        now=100,
    )[0]
    assert mark_sending(conn, outbound_id, lease_token=claimed["lease_token"], now=101)
    assert mark_sent(conn, outbound_id, lease_token=claimed["lease_token"], now=110)

    assert prune_history_before(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        before=200,
    ) == 2
    assert recent_messages(conn, tenant_id="tenant-a", source_id="chat-1") == []
    assert conn.execute(
        "SELECT COUNT(*) FROM inbound_batch_messages WHERE tenant_id = ? AND source_id = ?",
        ("tenant-a", "chat-1"),
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT text FROM outbound_messages WHERE id = ?",
        (outbound_id,),
    ).fetchone()[0] == "Old outbound message"


def test_batching_projects_inbound_message_into_history_atomically(tmp_path, monkeypatch):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message = InboundMessage(
        tenant_id="tenant-a",
        channel="telegram",
        source_id="chat-1",
        raw_event_id="telegram:chat-1:1",
        source_event_id="1",
        text="Запомни наше решение",
        payload={
            "user_id": 101,
            "username": "ivan_petrov",
            "payload": {"from_first_name": "Иван", "from_last_name": "Петров"},
        },
        message_at=100,
    )
    assert append_inbound_message(conn, message) is not None
    history = recent_messages(conn, tenant_id="tenant-a", source_id="chat-1")
    assert [
        (
            item.direction,
            item.author_id,
            item.author_username,
            item.author_display_name,
            item.text,
        )
        for item in history
    ] == [
        ("inbound", "101", "ivan_petrov", "Иван Петров", "Запомни наше решение")
    ]

    def fail_history(*args, **kwargs):
        raise RuntimeError("history unavailable")

    monkeypatch.setattr(batching_store_module, "record_message", fail_history)
    failed = InboundMessage(
        tenant_id="tenant-a",
        channel="telegram",
        source_id="chat-1",
        raw_event_id="telegram:chat-1:2",
        text="must roll back",
        payload={"user_id": 101},
        message_at=110,
    )
    with pytest.raises(RuntimeError, match="history unavailable"):
        append_inbound_message(conn, failed)
    assert conn.execute(
        "SELECT COUNT(*) FROM inbound_batch_messages WHERE raw_event_id = ?",
        (failed.raw_event_id,),
    ).fetchone()[0] == 0


def test_outbound_history_is_recorded_only_after_confirmed_send_and_rolls_back(tmp_path, monkeypatch):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="Итоговое решение",
        idempotency_key="reply:1",
        run_after=100,
        now=100,
    )
    assert message_id is not None
    assert recent_messages(conn, tenant_id="tenant-a", source_id="chat-1") == []
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker",
        lease_seconds=30,
        now=100,
    )[0]
    assert mark_sending(conn, message_id, lease_token=claimed["lease_token"], now=101)

    original_record = outbound_store_module.record_message

    def fail_history(*args, **kwargs):
        raise RuntimeError("history unavailable")

    monkeypatch.setattr(outbound_store_module, "record_message", fail_history)
    with pytest.raises(RuntimeError, match="history unavailable"):
        mark_sent(conn, message_id, lease_token=claimed["lease_token"], now=102)
    assert conn.execute("SELECT status FROM outbound_messages WHERE id = ?", (message_id,)).fetchone()[0] == "sending"

    monkeypatch.setattr(outbound_store_module, "record_message", original_record)
    assert mark_sent(conn, message_id, lease_token=claimed["lease_token"], now=103)
    history = recent_messages(conn, tenant_id="tenant-a", source_id="chat-1")
    assert [(item.direction, item.text, item.occurred_at) for item in history] == [
        ("outbound", "Итоговое решение", 103)
    ]


def test_reconciled_sent_outbound_message_is_added_to_history(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    message_id = enqueue_outbound(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        channel="telegram",
        destination_id="chat-1",
        text="Возможно доставлено",
        idempotency_key="reply:uncertain",
        run_after=100,
        now=100,
    )
    assert message_id is not None
    claimed = claim_due(
        conn,
        channel="telegram",
        limit=1,
        lease_owner="worker",
        lease_seconds=30,
        now=100,
    )[0]
    assert mark_sending(conn, message_id, lease_token=claimed["lease_token"], now=101)
    assert mark_uncertain(
        conn,
        message_id,
        lease_token=claimed["lease_token"],
        last_error="timeout",
        now=102,
    )

    resolved = resolve_outbound(
        conn,
        message_id,
        tenant_id="tenant-a",
        source_id="chat-1",
        resolution="sent",
        request_key="reconcile:1",
        actor_id="operator",
        evidence={"provider_message_id": "42"},
        effect_at=101,
        now=103,
    )

    assert resolved.status == "sent"
    assert [item.text for item in recent_messages(conn, tenant_id="tenant-a", source_id="chat-1")] == [
        "Возможно доставлено"
    ]


def test_conversation_history_tools_are_read_only_scoped_and_pseudonymized(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    for author_id, username, display_name, text, source_message_id, occurred_at in (
        ("telegram-user-101", "ivan", "Иван", "Обсудим новый тариф", "in-1", 100),
        ("telegram-user-202", "alexey", "Алексей", "Согласовано с августа", "in-2", 110),
    ):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            direction="inbound",
            author_id=author_id,
            author_username=username,
            author_display_name=display_name,
            text=text,
            source_message_id=source_message_id,
            occurred_at=occurred_at,
            metadata={"user_id": author_id, "safe": "visible"},
            now=occurred_at,
        )
    record_message(
        conn,
        tenant_id="tenant-a",
        source_id="chat-2",
        channel="telegram",
        direction="inbound",
        author_id="telegram-user-303",
        author_username="maria",
        author_display_name="Мария",
        text="Согласовано, но в другом чате",
        source_message_id="other-1",
        occurred_at=105,
        now=105,
    )
    registry = DynamicToolRegistry()
    register_conversation_history_tools(
        registry,
        SQLiteConversationHistoryStore._from_connection(conn),
        tenant_id="tenant-a",
        source_id="chat-1",
    )

    result = _json_result(asyncio.run(registry.call(_tool_params("search_message_history", {"query": "соглас"}))))
    dumped = json.dumps(result, ensure_ascii=False)

    assert registry.conversation == ("tenant-a", "chat-1")
    assert len(result["matches"]) == 1
    assert result["matches"][0]["context"][-1]["text"] == "Согласовано с августа"
    assert result["matches"][0]["context"][0]["author"] == {
        "ref": "participant_1",
        "display_name": "Иван",
        "username": "@ivan",
    }
    assert result["matches"][0]["context"][1]["author"] == {
        "ref": "participant_2",
        "display_name": "Алексей",
        "username": "@alexey",
    }
    assert result["matches"][0]["context"][0]["metadata"]["user_id"] == "[redacted:user_id]"
    assert "telegram-user" not in dumped
    assert "другом чате" not in dumped
    assert "tenant-a" not in dumped
    assert "chat-1" not in dumped


def test_conversation_history_tools_keep_participant_labels_stable_between_pages(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    for index, (author_id, username, display_name, text) in enumerate(
        (
            ("alice", "alice_handle", "Alice", "alice-old"),
            ("bob", "bob_handle", "Bob", "bob-old"),
            ("bob", "bob_handle", "Bob", "bob-new"),
            ("alice", "alice_handle", "Alice", "alice-new"),
        )
    ):
        record_message(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            channel="telegram",
            direction="inbound",
            author_id=author_id,
            author_username=username,
            author_display_name=display_name,
            text=text,
            source_message_id=f"in-{index}",
            occurred_at=index,
            now=index,
        )
    registry = DynamicToolRegistry()
    register_conversation_history_tools(
        registry,
        SQLiteConversationHistoryStore._from_connection(conn),
        tenant_id="tenant-a",
        source_id="chat-1",
    )

    newest = _json_result(
        asyncio.run(registry.call(_tool_params("read_recent_messages", {"limit": 2})))
    )["messages"]
    older = _json_result(
        asyncio.run(
            registry.call(
                _tool_params(
                    "read_recent_messages",
                    {"limit": 2, "before_message_id": newest[0]["message_id"]},
                )
            )
        )
    )["messages"]

    assert [(item["text"], item["author"]) for item in newest] == [
        (
            "bob-new",
            {"ref": "participant_1", "display_name": "Bob", "username": "@bob_handle"},
        ),
        (
            "alice-new",
            {"ref": "participant_2", "display_name": "Alice", "username": "@alice_handle"},
        ),
    ]
    assert [(item["text"], item["author"]) for item in older] == [
        (
            "alice-old",
            {"ref": "participant_2", "display_name": "Alice", "username": "@alice_handle"},
        ),
        (
            "bob-old",
            {"ref": "participant_1", "display_name": "Bob", "username": "@bob_handle"},
        ),
    ]
