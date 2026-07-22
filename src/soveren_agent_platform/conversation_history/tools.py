"""Read-only dynamic tools for conversation history."""

from __future__ import annotations

import json
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
MAX_HISTORY_TOOL_OUTPUT_BYTES = 256 * 1024
MAX_HISTORY_MESSAGE_TEXT_BYTES = 8 * 1024
MAX_HISTORY_METADATA_BYTES = 2 * 1024
MAX_HISTORY_PRESENTATION_BYTES = 512
MAX_HISTORY_CHANNEL_BYTES = 256


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
    return DynamicToolResult.json(
        _recent_messages_payload(
            messages,
            labels=participant_labels,
            model_redaction_policy=model_redaction_policy,
        )
    )


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
    return DynamicToolResult.json(
        _search_hits_payload(
            hits,
            labels=participant_labels,
            model_redaction_policy=model_redaction_policy,
        )
    )


def _recent_messages_payload(
    messages: list[ConversationMessage],
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    candidates = [
        _message_payload(
            message,
            labels=labels,
            model_redaction_policy=model_redaction_policy,
        )
        for message in messages
    ]
    selected: list[dict[str, Any]] = []
    for candidate in reversed(candidates):
        proposed = [candidate, *selected]
        if _json_size(_recent_result(proposed, total=len(candidates))) > MAX_HISTORY_TOOL_OUTPUT_BYTES:
            break
        selected = proposed
    return _recent_result(selected, total=len(candidates))


def _recent_result(messages: list[dict[str, Any]], *, total: int) -> dict[str, Any]:
    truncated = len(messages) < total
    result: dict[str, Any] = {"messages": messages, "truncated": truncated}
    if truncated and messages:
        result["next_before_message_id"] = messages[0]["message_id"]
    return result


def _search_hits_payload(
    hits: list[ConversationSearchHit],
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    context_truncated = False
    for hit in hits:
        candidate = _bounded_hit_payload(
            hit,
            labels=labels,
            model_redaction_policy=model_redaction_policy,
        )
        proposed = [*selected, candidate]
        if _json_size({"matches": proposed, "truncated": False}) > MAX_HISTORY_TOOL_OUTPUT_BYTES:
            break
        selected = proposed
        context_truncated = context_truncated or bool(candidate.get("context_truncated"))
    return {
        "matches": selected,
        "truncated": len(selected) < len(hits) or context_truncated,
    }


def _bounded_hit_payload(
    hit: ConversationSearchHit,
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    context = [
        {
            **_message_payload(
                message,
                labels=labels,
                model_redaction_policy=model_redaction_policy,
            ),
            "matched": message.id == hit.match.id,
        }
        for message in hit.context
    ]
    match_index = next(index for index, message in enumerate(context) if message["matched"])
    context_truncated = False
    while len(context) > 1:
        payload = _hit_result(hit.match.id, context, context_truncated=context_truncated)
        if _json_size({"matches": [payload], "truncated": False}) <= MAX_HISTORY_TOOL_OUTPUT_BYTES:
            return payload
        left_distance = match_index
        right_distance = len(context) - match_index - 1
        if left_distance >= right_distance and left_distance > 0:
            context.pop(0)
            match_index -= 1
        else:
            context.pop()
        context_truncated = True
    return _hit_result(hit.match.id, context, context_truncated=context_truncated)


def _hit_result(
    match_message_id: str,
    context: list[dict[str, Any]],
    *,
    context_truncated: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "match_message_id": match_message_id,
        "context": context,
    }
    if context_truncated:
        result["context_truncated"] = True
    return result


def _message_payload(
    message: ConversationMessage,
    *,
    labels: dict[str, str],
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    metadata = redact_value_for_model(message.metadata, policy=model_redaction_policy)
    metadata = metadata if isinstance(metadata, dict) else {}
    metadata_truncated = _json_size(metadata) > MAX_HISTORY_METADATA_BYTES
    if metadata_truncated:
        metadata = {}
    text, text_truncated = _clip_utf8(message.text, MAX_HISTORY_MESSAGE_TEXT_BYTES)
    channel, channel_truncated = _clip_utf8(message.channel, MAX_HISTORY_CHANNEL_BYTES)
    payload: dict[str, Any] = {
        "message_id": message.id,
        "channel": channel,
        "direction": message.direction,
        "author": _author_payload(message, labels),
        "text": text,
        "occurred_at": message.occurred_at,
        "metadata": metadata,
    }
    if text_truncated:
        payload["text_truncated"] = True
    if metadata_truncated:
        payload["metadata_truncated"] = True
    if channel_truncated:
        payload["channel_truncated"] = True
    return payload


def _author_label(message: ConversationMessage, labels: dict[str, str]) -> str:
    if message.direction == "outbound":
        return "agent"
    identity = _author_identity(message)
    if identity is None:
        return "participant_unknown"
    return labels.setdefault(identity, f"participant_{len(labels) + 1}")


def _author_identity(message: ConversationMessage) -> str | None:
    if message.author_id is not None and str(message.author_id).strip():
        return f"id:{str(message.author_id).strip()}"
    if message.author_username is not None and message.author_username.strip():
        return f"username:{message.author_username.strip().lstrip('@').casefold()}"
    return None


def _author_payload(message: ConversationMessage, labels: dict[str, str]) -> dict[str, Any]:
    reference = _author_label(message, labels)
    display_name, display_name_truncated = _clip_utf8(
        message.author_display_name or reference,
        MAX_HISTORY_PRESENTATION_BYTES,
    )
    payload: dict[str, Any] = {
        "ref": reference,
        "display_name": display_name,
    }
    if message.author_username is not None:
        username, username_truncated = _clip_utf8(
            f"@{message.author_username}",
            MAX_HISTORY_PRESENTATION_BYTES,
        )
        payload["username"] = username
    else:
        username_truncated = False
    if display_name_truncated or username_truncated:
        payload["truncated"] = True
    return payload


def _clip_utf8(value: str, maximum_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum_bytes:
        return value, False
    return encoded[:maximum_bytes].decode("utf-8", errors="ignore"), True


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


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
