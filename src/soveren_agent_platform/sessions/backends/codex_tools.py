"""Codex app-server dynamic tool contracts."""
from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DynamicToolSpec:
    name: str
    description: str
    input_schema: Any
    namespace: str | None = None
    defer_loading: bool = False

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

    @classmethod
    def from_app_server(cls, params: dict[str, Any]) -> DynamicToolCall:
        return cls(
            call_id=str(params["callId"]),
            thread_id=str(params["threadId"]),
            turn_id=str(params["turnId"]),
            tool=str(params["tool"]),
            arguments=params.get("arguments"),
            namespace=params.get("namespace"),
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

    def specs(self) -> list[DynamicToolSpec]:
        return [item[0] for item in self._tools.values()]

    def app_server_specs(self) -> list[dict[str, Any]]:
        return [spec.to_app_server() for spec in self.specs()]

    async def call(self, params: dict[str, Any]) -> dict[str, Any]:
        reference = str(params.get("callId") or "unknown")
        try:
            call = DynamicToolCall.from_app_server(params)
            handler = self._tools.get((call.namespace, call.tool))
            if handler is None and call.namespace is not None:
                handler = self._tools.get((None, call.tool))
            if handler is None:
                result = DynamicToolResult.text(
                    f"Dynamic tool is not registered: {call.tool}",
                    success=False,
                )
                return result.to_app_server()
            raw = handler[1](call)
            if inspect.isawaitable(raw):
                raw = await raw
            return _normalize_result(raw).to_app_server()
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


def normalize_dynamic_tool_specs(values: Sequence[DynamicToolSpec | dict[str, Any]]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, DynamicToolSpec):
            payloads.append(value.to_app_server())
        elif isinstance(value, dict):
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
