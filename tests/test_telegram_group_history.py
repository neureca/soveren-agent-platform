from __future__ import annotations

import asyncio
import json
from functools import wraps
from types import SimpleNamespace

import pytest

from soveren_agent_platform.conversation_history import SQLiteConversationHistoryStore
from soveren_agent_platform.sessions import DynamicToolRegistry
from soveren_agent_platform.storage import bootstrap_platform_storage
from soveren_agent_platform.telegram import (
    TELEGRAM_GROUP_HISTORY_TOOL_NAMESPACE,
    SQLiteTelegramChatRegistry,
    register_telegram_group_history_tools,
)


class FakeBot:
    id = 900

    def __init__(self) -> None:
        self.user_status = "member"
        self.bot_status = "administrator"
        self.fail = False

    async def get_chat_member(self, chat_id: int, user_id: int) -> SimpleNamespace:
        if self.fail:
            raise RuntimeError("Telegram unavailable")
        status = self.bot_status if user_id == self.id else self.user_status
        return SimpleNamespace(status=status, is_member=status == "restricted")

    async def get_chat(self, chat_id: int) -> SimpleNamespace:
        return SimpleNamespace(title="Рабочая группа", type="supergroup")


def run_async(test):
    @wraps(test)
    def wrapped(tmp_path):
        asyncio.run(test(tmp_path))

    return wrapped


async def _call(
    registry: DynamicToolRegistry, name: str, arguments: dict[str, object]
) -> tuple[bool, dict[str, object]]:
    response = await registry.call({
        "callId": "call-1", "threadId": "thread-1", "turnId": "turn-1",
        "namespace": TELEGRAM_GROUP_HISTORY_TOOL_NAMESPACE,
        "tool": name, "arguments": arguments,
    })
    return response["success"], json.loads(response["contentItems"][0]["text"])


@run_async
async def test_private_user_searches_only_registered_group_with_live_access(tmp_path) -> None:
    db_path = tmp_path / "platform.db"
    await bootstrap_platform_storage(db_path)
    history = await SQLiteConversationHistoryStore.open(db_path)
    groups = await SQLiteTelegramChatRegistry.open(db_path)
    bot = FakeBot()
    try:
        await groups.register(tenant_id="tenant-a", chat_id=-500, registered_by_user_id=100)
        await groups.register(tenant_id="tenant-b", chat_id=-600, registered_by_user_id=100)
        for source, text in (
            ("-500", "Срок релиза назначен на пятницу"),
            ("100", "Срок личного события назначен на субботу"),
            ("-600", "Срок чужой группы назначен на воскресенье"),
        ):
            await history.record(
                tenant_id="tenant-a", source_id=source, channel="telegram",
                direction="inbound", text=text, source_message_id=f"{source}:1",
                occurred_at=1_700_000_000,
            )
        registry = DynamicToolRegistry()
        register_telegram_group_history_tools(
            registry, history=history, groups=groups, bot=bot,
            tenant_id="tenant-a", private_chat_id=100, requester_user_id=100,
        )
        success, listed = await _call(registry, "list_searchable_groups", {})
        assert success
        assert listed["truncated"] is False
        assert listed["incomplete"] is False
        groups_list = listed["groups"]
        assert isinstance(groups_list, list) and len(groups_list) == 1
        assert groups_list[0]["title"] == "Рабочая группа"
        assert "chat_id" not in str(listed)
        ref = groups_list[0]["ref"]

        success, result = await _call(
            registry, "search_group_messages", {"group_ref": ref, "query": "Срок"}
        )
        assert success
        assert [
            message["text"]
            for hit in result["matches"]
            for message in hit["context"]
        ] == ["Срок релиза назначен на пятницу"]
        assert "-500" not in json.dumps(result)

        assert await _call(
            registry, "search_group_messages", {"group_ref": "guessed", "query": "Срок"}
        ) == (False, {"error": "group_access_denied"})
        await groups.revoke(tenant_id="tenant-a", chat_id=-500)
        assert await _call(
            registry, "search_group_messages", {"group_ref": ref, "query": "Срок"}
        ) == (False, {"error": "group_access_denied"})
    finally:
        await groups.close()
        await history.close()


@run_async
async def test_group_search_fails_closed_when_membership_changes_or_is_unknown(tmp_path) -> None:
    db_path = tmp_path / "platform.db"
    await bootstrap_platform_storage(db_path)
    history = await SQLiteConversationHistoryStore.open(db_path)
    groups = await SQLiteTelegramChatRegistry.open(db_path)
    bot = FakeBot()
    try:
        await groups.register(tenant_id="tenant-a", chat_id=-500, registered_by_user_id=100)
        registry = DynamicToolRegistry()
        register_telegram_group_history_tools(
            registry, history=history, groups=groups, bot=bot,
            tenant_id="tenant-a", private_chat_id=100, requester_user_id=100,
        )
        _, listed = await _call(registry, "list_searchable_groups", {})
        ref = listed["groups"][0]["ref"]
        args = {"group_ref": ref, "query": "релиз"}
        bot.user_status = "left"
        assert await _call(registry, "search_group_messages", args) == (
            False, {"error": "group_access_denied"}
        )
        bot.user_status = "member"
        bot.bot_status = "member"
        assert await _call(registry, "search_group_messages", args) == (
            False, {"error": "group_access_unavailable"}
        )
        bot.bot_status = "administrator"
        bot.fail = True
        assert await _call(registry, "search_group_messages", args) == (
            False, {"error": "group_access_unavailable"}
        )
    finally:
        await groups.close()
        await history.close()


@run_async
async def test_group_history_tools_require_private_scope(tmp_path) -> None:
    db_path = tmp_path / "platform.db"
    await bootstrap_platform_storage(db_path)
    history = await SQLiteConversationHistoryStore.open(db_path)
    groups = await SQLiteTelegramChatRegistry.open(db_path)
    try:
        for chat_id, user_id in [(-500, 100), (100, 200)]:
            with pytest.raises(ValueError, match="private Telegram chat"):
                register_telegram_group_history_tools(
                    DynamicToolRegistry(), history=history, groups=groups,
                    bot=FakeBot(), tenant_id="tenant-a",
                    private_chat_id=chat_id, requester_user_id=user_id,
                )
        registry = DynamicToolRegistry()
        registry.bind_conversation(tenant_id="tenant-a", source_id="-500")
        with pytest.raises(ValueError, match="another conversation"):
            register_telegram_group_history_tools(
                registry, history=history, groups=groups, bot=FakeBot(),
                tenant_id="tenant-a", private_chat_id=100, requester_user_id=100,
            )
    finally:
        await groups.close()
        await history.close()
