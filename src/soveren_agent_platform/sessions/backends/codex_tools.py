"""Codex app-server dynamic tool contracts."""
from __future__ import annotations

import inspect
import json
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from soveren_agent_platform.execution import (
    DynamicToolAllowlist,
    ToolExecutionContext,
    validate_dynamic_tool_allowlist,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DynamicToolSpec:
    name: str
    description: str
    input_schema: Any
    namespace: str | None = None
    defer_loading: bool = False

    def __post_init__(self) -> None:
        _validate_dynamic_tool_namespace(self.namespace)

    def to_app_server(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }
        if self.namespace is not None:
            payload["namespace"] = self.namespace
        if self.defer_loading:
            payload["deferLoading"] = True
        return payload


@dataclass(frozen=True, slots=True)
class DynamicToolCall:
    call_id: str
    thread_id: str
    turn_id: str
    tool: str
    arguments: Any
    namespace: str | None = None
    execution_context: ToolExecutionContext | None = None

    @classmethod
    def from_app_server(
        cls,
        params: dict[str, Any],
        *,
        execution_context: ToolExecutionContext | None = None,
    ) -> DynamicToolCall:
        return cls(
            call_id=str(params["callId"]),
            thread_id=str(params["threadId"]),
            turn_id=str(params["turnId"]),
            tool=str(params["tool"]),
            arguments=params.get("arguments"),
            namespace=params.get("namespace"),
            execution_context=execution_context,
        )


@dataclass(frozen=True, slots=True)
class DynamicToolResult:
    success: bool
    content_items: list[dict[str, Any]]

    @classmethod
    def text(cls, text: str, *, success: bool = True) -> DynamicToolResult:
        return cls(success=success, content_items=[{"type": "inputText", "text": text}])

    @classmethod
    def json(cls, value: Any, *, success: bool = True) -> DynamicToolResult:
        return cls.text(json.dumps(value, ensure_ascii=False), success=success)

    def to_app_server(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "contentItems": self.content_items,
        }


class DynamicToolHandler(Protocol):
    def __call__(self, call: DynamicToolCall) -> DynamicToolResult | Awaitable[DynamicToolResult] | str | dict:
        ...


ToolCallable = Callable[[DynamicToolCall], DynamicToolResult | Awaitable[DynamicToolResult] | str | dict]


class DynamicToolRegistry:
    """Registry of dynamic tools exposed to Codex app-server threads."""

    def __init__(self) -> None:
        self._tools: dict[tuple[str | None, str], tuple[DynamicToolSpec, ToolCallable]] = {}
        self._conversation: tuple[str, str] | None = None

    @property
    def conversation(self) -> tuple[str, str] | None:
        return self._conversation

    def bind_conversation(self, *, tenant_id: str, source_id: str) -> None:
        if not tenant_id.strip() or not source_id.strip():
            raise ValueError("tenant_id and source_id must be non-empty")
        requested = (tenant_id, source_id)
        if self._conversation is not None and self._conversation != requested:
            raise ValueError(
                "dynamic tool registry is already bound to another conversation"
            )
        self._conversation = requested

    def register(self, spec: DynamicToolSpec, handler: ToolCallable) -> None:
        key = (spec.namespace, spec.name)
        if key in self._tools:
            namespace = f"{spec.namespace}/" if spec.namespace else ""
            raise ValueError(f"dynamic tool already registered: {namespace}{spec.name}")
        self._tools[key] = (spec, handler)

    def specs(
        self,
        *,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> list[DynamicToolSpec]:
        selected = validate_dynamic_tool_allowlist(allowlist)
        if selected is None:
            return [item[0] for item in self._tools.values()]
        unknown = set(selected) - self._tools.keys()
        if unknown:
            raise ValueError(
                "dynamic tool allowlist contains unknown selectors: "
                f"{sorted(unknown, key=lambda key: (key[0] or '', key[1]))!r}"
            )
        return [self._tools[key][0] for key in selected]

    def app_server_specs(
        self,
        *,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> list[dict[str, Any]]:
        return [spec.to_app_server() for spec in self.specs(allowlist=allowlist)]

    async def call(
        self,
        params: dict[str, Any],
        *,
        execution_context: ToolExecutionContext | None = None,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> dict[str, Any]:
        reference = str(params.get("callId") or "unknown")
        try:
            call = DynamicToolCall.from_app_server(
                params,
                execution_context=execution_context,
            )
            return (await self.dispatch(call, allowlist=allowlist)).to_app_server()
        except Exception:
            log.exception(
                "dynamic tool failed call_id=%s namespace=%s tool=%s",
                reference,
                params.get("namespace"),
                params.get("tool"),
            )
            return DynamicToolResult.text(
                f"Dynamic tool failed. Reference: {reference}",
                success=False,
            ).to_app_server()

    async def dispatch(
        self,
        call: DynamicToolCall,
        *,
        allowlist: DynamicToolAllowlist | None = None,
    ) -> DynamicToolResult:
        """Dispatch a host-resolved call while preserving its trusted context.

        Callers that route through a nested registry should pass the existing
        ``DynamicToolCall`` here. Its execution context is trusted only because
        the host attached it; this method never derives context from arguments.
        """
        try:
            selected = validate_dynamic_tool_allowlist(allowlist)
            requested_key = (call.namespace, call.tool)
            if selected is not None and requested_key not in selected:
                return DynamicToolResult.text(
                    f"Dynamic tool is not allowed for this execution: {call.tool}",
                    success=False,
                )
            if call.execution_context is not None:
                expected_scope = (
                    call.execution_context.scope.tenant_id,
                    call.execution_context.scope.source_id,
                )
                if expected_scope != self._conversation:
                    return DynamicToolResult.text(
                        "Dynamic tool execution context does not match its conversation.",
                        success=False,
                    )
            handler = self._tools.get(requested_key)
            if handler is None and call.namespace is not None:
                fallback_key = (None, call.tool)
                if selected is None or fallback_key in selected:
                    handler = self._tools.get(fallback_key)
            if handler is None:
                return DynamicToolResult.text(
                    f"Dynamic tool is not registered: {call.tool}",
                    success=False,
                )
            raw = handler[1](call)
            if inspect.isawaitable(raw):
                raw = await raw
            return _normalize_result(raw)
        except Exception:
            log.exception(
                "dynamic tool failed call_id=%s namespace=%s tool=%s",
                call.call_id,
                call.namespace,
                call.tool,
            )
            return DynamicToolResult.text(
                f"Dynamic tool failed. Reference: {call.call_id}",
                success=False,
            )


def normalize_dynamic_tool_specs(values: Sequence[DynamicToolSpec | dict[str, Any]]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, DynamicToolSpec):
            payloads.append(value.to_app_server())
        elif isinstance(value, dict):
            _validate_dynamic_tool_namespace(value.get("namespace"))
            payloads.append(dict(value))
        else:
            raise TypeError(f"unsupported dynamic tool spec: {type(value).__name__}")
    return payloads


def _normalize_result(value: DynamicToolResult | str | dict) -> DynamicToolResult:
    if isinstance(value, DynamicToolResult):
        return value
    if isinstance(value, str):
        return DynamicToolResult.text(value)
    if isinstance(value, dict):
        return DynamicToolResult.json(value)
    raise TypeError(f"unsupported dynamic tool result: {type(value).__name__}")


def _validate_dynamic_tool_namespace(namespace: object) -> None:
    if namespace is None:
        return
    if not isinstance(namespace, str) or re.fullmatch(r"[a-zA-Z0-9_-]+", namespace) is None:
        raise ValueError(
            "dynamic tool namespace must match ^[a-zA-Z0-9_-]+$ "
            f"to match Codex app-server: {namespace!r}"
        )
