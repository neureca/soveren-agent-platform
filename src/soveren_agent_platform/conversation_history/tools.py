"""Read-only dynamic tools for conversation history."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.conversation_history.contracts import (
    ConversationHistoryStore,
    ConversationMessage,
    ConversationSearchHit,
)
from soveren_agent_platform.conversation_history.store import MAX_SEARCH_QUERY_CHARS
from soveren_agent_platform.model_boundary import ModelRedactionPolicy, redact_value_for_model
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)

CONVERSATION_HISTORY_TOOL_NAMESPACE = "platform.conversation"


def register_conversation_history_tools(
    registry: DynamicToolRegistry,
    store: ConversationHistoryStore,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None = None,
) -> None:
    participant_labels: dict[str, str] = {}
    registry.bind_conversation(tenant_id=tenant_id, source_id=source_id)
    registry.register(
        DynamicToolSpec(
            name="read_recent_messages",
            namespace=CONVERSATION_HISTORY_TOOL_NAMESPACE,
            description="Read recent messages from the current conversation only.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                    "before_message_id": {"type": "string", "minLength": 1},
                },
            },
        ),
        lambda call: _recent_messages_tool(
            store,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
            model_redaction_policy=model_redaction_policy,
            participant_labels=participant_labels,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="search_message_history",
            namespace=CONVERSATION_HISTORY_TOOL_NAMESPACE,
            description=(
                "Search message history in the current conversation and return each match with nearby context."
            ),
            input_schema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string", "maxLength": MAX_SEARCH_QUERY_CHARS},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20},
                    "context_before": {"type": "integer", "minimum": 0, "maximum": 10},
                    "context_after": {"type": "integer", "minimum": 0, "maximum": 10},
                    "since": {"type": "integer", "minimum": 0},
                    "until": {"type": "integer", "minimum": 0},
                },
            },
        ),
        lambda call: _search_messages_tool(
            store,
            call,
            tenant_id=tenant_id,
            source_id=source_id,
            model_redaction_policy=model_redaction_policy,
            participant_labels=participant_labels,
        ),
    )


async def _recent_messages_tool(
    store: ConversationHistoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None,
    participant_labels: dict[str, str],
) -> DynamicToolResult:
    args = _args(call)
    messages = await store.recent(
        tenant_id=tenant_id,
        source_id=source_id,
        limit=_integer(args.get("limit"), default=20, minimum=1, maximum=50),
        before_message_id=_optional_string(args.get("before_message_id")),
    )
    return DynamicToolResult.json({
        "messages": [
            _message_payload(
                message,
                labels=participant_labels,
                model_redaction_policy=model_redaction_policy,
            )
            for message in messages
        ]
    })


async def _search_messages_tool(
    store: ConversationHistoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    source_id: str,
    model_redaction_policy: ModelRedactionPolicy | None,
    participant_labels: dict[str, str],
) -> DynamicToolResult:
    args = _args(call)
    hits = await store.search(
        tenant_id=tenant_id,
        source_id=source_id,
        query=_query(args.get("query")),
        limit=_integer(args.get("limit"), default=10, minimum=1, maximum=20),
        context_before=_integer(args.get("context_before"), default=3, minimum=0, maximum=10),
        context_after=_integer(args.get("context_after"), default=3, minimum=0, maximum=10),
        since=_optional_integer(args.get("since"), minimum=0),
        until=_optional_integer(args.get("until"), minimum=0),
    )
    return DynamicToolResult.json({
        "matches": [
            _hit_payload(
                hit,
                labels=participant_labels,
                model_redaction_policy=model_redaction_policy,
            )
            for hit in hits
        ]
    })


def _hit_payload(
    hit: ConversationSearchHit,
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    return {
        "match_message_id": hit.match.id,
        "context": [
            {
                **_message_payload(
                    message,
                    labels=labels,
                    model_redaction_policy=model_redaction_policy,
                ),
                "matched": message.id == hit.match.id,
            }
            for message in hit.context
        ],
    }


def _message_payload(
    message: ConversationMessage,
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    metadata = redact_value_for_model(message.metadata, policy=model_redaction_policy)
    return {
        "message_id": message.id,
        "channel": message.channel,
        "direction": message.direction,
        "author": _author_payload(message, labels),
        "text": message.text,
        "occurred_at": message.occurred_at,
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def _author_label(message: ConversationMessage, labels: dict[str, str]) -> str:
    if message.direction == "outbound":
        return "agent"
    if message.author_id is None:
        return "participant_unknown"
    return labels.setdefault(message.author_id, f"participant_{len(labels) + 1}")


def _author_payload(message: ConversationMessage, labels: dict[str, str]) -> dict[str, str]:
    reference = _author_label(message, labels)
    payload = {
        "ref": reference,
        "display_name": message.author_display_name or reference,
    }
    if message.author_username is not None:
        payload["username"] = f"@{message.author_username}"
    return payload


def _args(call: DynamicToolCall) -> dict[str, Any]:
    return call.arguments if isinstance(call.arguments, dict) else {}


def _integer(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"value must be an integer between {minimum} and {maximum}")
    return value


def _optional_integer(value: Any, *, minimum: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"value must be an integer greater than or equal to {minimum}")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be a non-empty string")
    return value.strip()


def _query(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("query must be a string")
    if len(value) > MAX_SEARCH_QUERY_CHARS:
        raise ValueError(f"query must not exceed {MAX_SEARCH_QUERY_CHARS} characters")
    return value
