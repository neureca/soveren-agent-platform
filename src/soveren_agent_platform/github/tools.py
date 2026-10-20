"""Conversation-bound tools. Authorization is supplied by the application."""

from __future__ import annotations

import json
from typing import Any

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.sessions import DynamicToolCall, DynamicToolRegistry, DynamicToolResult, DynamicToolSpec
from soveren_agent_platform.sessions.backends.codex_tools import ToolCallable

from .client import GitHubConnector
from .contracts import GitHubError, GitHubErrorCode

NAMESPACE = "platform_github"
_STR = {"type": "string", "minLength": 1, "maxLength": 1024}
_INT = {"type": "integer", "minimum": 1}
_SCHEMAS: dict[str, tuple[dict[str, Any], list[str], str]] = {
    "resolve_ref": (
        {"ref": _STR},
        ["ref"],
        "Pin an explicitly authorized ref to a commit SHA; no default branch fallback.",
    ),
    "list_tree": (
        {"commit": _STR, "prefix": {"type": "string", "maxLength": 1024}, "offset": {"type": "integer", "minimum": 0}},
        ["commit"],
        "Discover paths at a pinned commit. Partial trees are explicit; continue with next_offset.",
    ),
    "read_file": (
        {
            "commit": _STR,
            "path": _STR,
            "start_line": _INT,
            "end_line": _INT,
            "max_bytes": {"type": "integer", "minimum": 1, "maximum": 24000},
        },
        ["commit", "path"],
        "Read a regular UTF-8 file at a pinned commit, at most 200 lines and 24000 bytes, with citations.",
    ),
    "read_pull_request": (
        {"number": _INT, "offset": {"type": "integer", "minimum": 0}},
        ["number"],
        "Pin authorized PR metadata and read bounded merge-base-to-head diff; patches may be incomplete.",
    ),
}


def register_github_tools(
    registry: DynamicToolRegistry,
    connector: GitHubConnector,
    *,
    scope: ConversationScope,
) -> None:
    if scope != connector.access.conversation:
        raise GitHubError(GitHubErrorCode.DENIED)
    registry.bind_conversation(tenant_id=scope.tenant_id, source_id=scope.source_id)
    operations = {op for repo in connector.access.repositories for op in repo.operations}
    for operation in sorted(operations):
        properties, required, description = _SCHEMAS[operation]
        fields = {"repository": _STR, **properties}
        registry.register(
            DynamicToolSpec(
                name=operation,
                namespace=NAMESPACE,
                description=description,
                input_schema={
                    "type": "object",
                    "properties": fields,
                    "required": ["repository", *required],
                    "additionalProperties": False,
                },
            ),
            _handler(connector, scope, operation, fields, {"repository", *required}),
        )


def _handler(
    connector: GitHubConnector,
    scope: ConversationScope,
    operation: str,
    fields: dict[str, Any],
    required: set[str],
) -> ToolCallable:
    async def handle(call: DynamicToolCall) -> DynamicToolResult:
        try:
            args = call.arguments
            if not isinstance(args, dict) or not required <= args.keys() or args.keys() - fields.keys():
                raise GitHubError(GitHubErrorCode.INVALID)
            for name, value in args.items():
                field = fields[name]
                if field["type"] == "string":
                    if not isinstance(value, str) or not field.get("minLength", 0) <= len(value) <= field["maxLength"]:
                        raise GitHubError(GitHubErrorCode.INVALID)
                elif type(value) is not int or value < field["minimum"] or value > field.get("maximum", 1_000_000):
                    raise GitHubError(GitHubErrorCode.INVALID)
            payload = await connector.execute(scope, operation, args)
            encoded = json.dumps(payload, ensure_ascii=False)
            if len(encoded.encode("utf-8")) > 192 * 1024:
                raise GitHubError(GitHubErrorCode.TOO_LARGE)
            return DynamicToolResult.text(encoded)
        except GitHubError as exc:
            return DynamicToolResult.json({"error": exc.code.value}, success=False)
        except Exception:
            # Do not let registry.log.exception serialize credential-bearing provider errors.
            return DynamicToolResult.json({"error": GitHubErrorCode.PROTOCOL.value}, success=False)

    return handle
