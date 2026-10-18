"""Explicit, access-checked Telegram group history tools for private chats."""

from __future__ import annotations

import logging
import secrets
from typing import Any, Protocol

from soveren_agent_platform.conversation_history import ConversationHistoryStore
from soveren_agent_platform.conversation_history.store import MAX_SEARCH_QUERY_CHARS
from soveren_agent_platform.conversation_history.tools import _search_hits_payload
from soveren_agent_platform.model_boundary import ModelRedactionPolicy
from soveren_agent_platform.sessions import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)
from soveren_agent_platform.telegram.contracts import TelegramChatRegistry

log = logging.getLogger(__name__)

TELEGRAM_GROUP_HISTORY_TOOL_NAMESPACE = "platform_telegram_groups"
MAX_SEARCHABLE_GROUPS = 20


class TelegramGroupMembershipReader(Protocol):
    id: int

    async def get_chat_member(self, chat_id: int, user_id: int) -> Any: ...

    async def get_chat(self, chat_id: int) -> Any: ...


def register_telegram_group_history_tools(
    registry: DynamicToolRegistry,
    *,
    history: ConversationHistoryStore,
    groups: TelegramChatRegistry,
    bot: TelegramGroupMembershipReader,
    tenant_id: str,
    private_chat_id: int,
    requester_user_id: int,
    model_redaction_policy: ModelRedactionPolicy | None = None,
) -> None:
    """Expose group search only on this user's trusted private Telegram scope.

    The caller must derive both IDs from authenticated Telegram ingress, and
    explicitly opt in to exposing these tools. Registration alone never grants
    access to a group: each read also checks registration and live membership.
    """
    if private_chat_id <= 0 or private_chat_id != requester_user_id:
        raise ValueError("group history tools require the requester's private Telegram chat")
    registry.bind_conversation(tenant_id=tenant_id, source_id=str(private_chat_id))
    references: dict[str, int] = {}
    participant_labels: dict[str, str] = {}

    async def list_groups(call: DynamicToolCall) -> DynamicToolResult:
        if call.arguments not in (None, {}):
            return _failure("invalid_arguments")
        references.clear()
        try:
            chat_ids = await groups.list_registered(
                tenant_id=tenant_id, limit=MAX_SEARCHABLE_GROUPS + 1
            )
        except Exception:
            log.exception("Telegram group registry listing failed")
            return _failure("groups_unavailable")
        visible: list[dict[str, str]] = []
        incomplete = False
        for chat_id in chat_ids[:MAX_SEARCHABLE_GROUPS]:
            access = await _can_read_group(
                groups, bot, tenant_id=tenant_id,
                chat_id=chat_id, user_id=requester_user_id,
            )
            if access is None:
                incomplete = True
                continue
            if not access:
                continue
            try:
                chat = await bot.get_chat(chat_id)
            except Exception:
                log.warning("Telegram group title unavailable chat_id=%s", chat_id)
                incomplete = True
                continue
            title = getattr(chat, "title", None)
            if (
                getattr(chat, "type", None) not in {"group", "supergroup"}
                or not isinstance(title, str)
                or not title.strip()
            ):
                incomplete = True
                continue
            ref = secrets.token_urlsafe(12)
            references[ref] = chat_id
            visible.append({"ref": ref, "title": title[:256]})
        return DynamicToolResult.json({
            "groups": visible,
            "truncated": len(chat_ids) > MAX_SEARCHABLE_GROUPS,
            "incomplete": incomplete,
        })

    async def search_group(call: DynamicToolCall) -> DynamicToolResult:
        args = call.arguments if isinstance(call.arguments, dict) else {}
        ref = args.get("group_ref")
        query = args.get("query")
        if (
            not isinstance(ref, str)
            or not isinstance(query, str)
            or not query.strip()
            or len(query) > MAX_SEARCH_QUERY_CHARS
            or set(args) != {"group_ref", "query"}
        ):
            return _failure("invalid_arguments")
        chat_id = references.get(ref)
        if chat_id is None:
            return _failure("group_access_denied")
        access = await _can_read_group(
            groups, bot, tenant_id=tenant_id,
            chat_id=chat_id, user_id=requester_user_id,
        )
        if access is None:
            return _failure("group_access_unavailable")
        if not access:
            return _failure("group_access_denied")
        try:
            hits = await history.search(
                tenant_id=tenant_id,
                source_id=str(chat_id),
                query=query,
                limit=10,
                context_before=1,
                context_after=1,
            )
        except ValueError:
            return _failure("invalid_query")
        except Exception:
            log.exception("Telegram group history search failed chat_id=%s", chat_id)
            return _failure("search_unavailable")
        return DynamicToolResult.json(_search_hits_payload(
            hits,
            labels=participant_labels,
            model_redaction_policy=model_redaction_policy,
        ))

    registry.register(
        DynamicToolSpec(
            name="list_searchable_groups",
            namespace=TELEGRAM_GROUP_HISTORY_TOOL_NAMESPACE,
            description="List registered Telegram groups this private-chat user can currently access.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        ),
        list_groups,
    )
    registry.register(
        DynamicToolSpec(
            name="search_group_messages",
            namespace=TELEGRAM_GROUP_HISTORY_TOOL_NAMESPACE,
            description=(
                "Search stored messages of one listed Telegram group. First call "
                "list_searchable_groups and use its opaque ref. Access is rechecked on every search."
            ),
            input_schema={
                "type": "object",
                "required": ["group_ref", "query"],
                "properties": {
                    "group_ref": {"type": "string", "minLength": 1},
                    "query": {"type": "string", "minLength": 1, "maxLength": MAX_SEARCH_QUERY_CHARS},
                },
                "additionalProperties": False,
            },
        ),
        search_group,
    )


async def _can_read_group(
    groups: TelegramChatRegistry,
    bot: TelegramGroupMembershipReader,
    *,
    tenant_id: str,
    chat_id: int,
    user_id: int,
) -> bool | None:
    if isinstance(chat_id, bool) or not isinstance(chat_id, int) or chat_id >= 0:
        return False
    try:
        if not await groups.is_registered(tenant_id=tenant_id, chat_id=chat_id):
            return False
        bot_id = bot.id
        if not isinstance(bot_id, int) or bot_id <= 0:
            return None
        bot_member = await bot.get_chat_member(chat_id=chat_id, user_id=bot_id)
        if getattr(bot_member, "status", None) not in {"creator", "administrator"}:
            return None
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
    except Exception:
        log.warning("Telegram group membership check unavailable chat_id=%s", chat_id)
        return None
    status = getattr(member, "status", None)
    if status == "restricted":
        return getattr(member, "is_member", False) is True
    return status in {"creator", "administrator", "member"}


def _failure(code: str) -> DynamicToolResult:
    return DynamicToolResult.json({"error": code}, success=False)
