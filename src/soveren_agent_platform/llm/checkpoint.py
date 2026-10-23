"""Typed durable checkpoints for one lease-fenced LLM execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, cast, runtime_checkable

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import (
    DynamicToolAllowlist,
    ToolExecutionContext,
    validate_dynamic_tool_allowlist,
)
from soveren_agent_platform.json_types import JsonObject, JsonValue, require_json_object
from soveren_agent_platform.sessions.backend import SendReceipt

CheckpointState = Literal[
    "opened",
    "sending",
    "accepted",
    "pausing",
    "paused",
    "completed",
    "cancelled",
    "failed",
]


@dataclass(frozen=True, slots=True)
class LlmCheckpointBinding:
    """Trusted host inputs that must match before a native thread is resumed."""

    run_id: str
    backend_kind: str
    model: str
    conversation_scope: ConversationScope
    cwd: str
    env_home: str
    task_id: str | None
    execution_id: str | None
    dynamic_tool_allowlist: DynamicToolAllowlist | None
    output_schema: JsonObject | None

    def __post_init__(self) -> None:
        for name in ("run_id", "backend_kind", "model", "cwd", "env_home"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.conversation_scope, ConversationScope):
            raise TypeError("conversation_scope must be a ConversationScope")
        if self.task_id is not None and not isinstance(self.task_id, str):
            raise TypeError("task_id must be a string or None")
        if self.execution_id is not None and not isinstance(self.execution_id, str):
            raise TypeError("execution_id must be a string or None")
        object.__setattr__(
            self,
            "dynamic_tool_allowlist",
            validate_dynamic_tool_allowlist(self.dynamic_tool_allowlist),
        )
        if self.output_schema is not None:
            object.__setattr__(
                self,
                "output_schema",
                require_json_object(self.output_schema, label="checkpoint output_schema"),
            )

    @classmethod
    def for_request(
        cls,
        *,
        run_id: str,
        backend_kind: str,
        model: str,
        cwd: str,
        env_home: str,
        conversation_scope: ConversationScope | None,
        execution_context: ToolExecutionContext | None,
        dynamic_tool_allowlist: DynamicToolAllowlist | None,
        output_schema: JsonObject | None,
    ) -> LlmCheckpointBinding:
        if conversation_scope is None:
            raise ValueError("a durable LLM checkpoint requires a trusted conversation scope")
        if execution_context is not None and execution_context.scope != conversation_scope:
            raise ValueError("LLM execution context does not match checkpoint conversation scope")
        return cls(
            run_id=run_id,
            backend_kind=backend_kind,
            model=model,
            conversation_scope=conversation_scope,
            cwd=cwd,
            env_home=env_home,
            task_id=execution_context.task_id if execution_context is not None else None,
            execution_id=execution_context.execution_id if execution_context is not None else None,
            dynamic_tool_allowlist=dynamic_tool_allowlist,
            output_schema=output_schema,
        )

    def to_json(self) -> JsonObject:
        allowlist: list[JsonValue] | None = None
        if self.dynamic_tool_allowlist is not None:
            allowlist = []
            for namespace, name in self.dynamic_tool_allowlist:
                allowlist.append([namespace, name])
        return {
            "run_id": self.run_id,
            "backend_kind": self.backend_kind,
            "model": self.model,
            "conversation_scope": {
                "tenant_id": self.conversation_scope.tenant_id,
                "source_id": self.conversation_scope.source_id,
            },
            "cwd": self.cwd,
            "env_home": self.env_home,
            "task_id": self.task_id,
            "execution_id": self.execution_id,
            "dynamic_tool_allowlist": allowlist,
            "output_schema": self.output_schema,
        }

    @classmethod
    def from_json(cls, value: object) -> LlmCheckpointBinding:
        payload = require_json_object(value, label="LLM checkpoint binding")
        scope_value = require_json_object(
            payload.get("conversation_scope"),
            label="LLM checkpoint conversation_scope",
        )
        allowlist_value = payload.get("dynamic_tool_allowlist")
        allowlist: DynamicToolAllowlist | None
        if allowlist_value is None:
            allowlist = None
        else:
            if not isinstance(allowlist_value, list):
                raise TypeError("LLM checkpoint dynamic_tool_allowlist must be a list or None")
            normalized: list[tuple[str | None, str]] = []
            for selector in allowlist_value:
                if not isinstance(selector, list) or len(selector) != 2:
                    raise TypeError("LLM checkpoint tool selector must contain two items")
                namespace, name = selector
                if namespace is not None and not isinstance(namespace, str):
                    raise TypeError("LLM checkpoint tool namespace must be a string or None")
                if not isinstance(name, str):
                    raise TypeError("LLM checkpoint tool name must be a string")
                normalized.append((namespace, name))
            allowlist = tuple(normalized)
        schema_value = payload.get("output_schema")
        schema = (
            require_json_object(schema_value, label="LLM checkpoint output_schema")
            if schema_value is not None
            else None
        )
        return cls(
            run_id=_string(payload, "run_id"),
            backend_kind=_string(payload, "backend_kind"),
            model=_string(payload, "model"),
            conversation_scope=ConversationScope(
                tenant_id=_string(scope_value, "tenant_id"),
                source_id=_string(scope_value, "source_id"),
            ),
            cwd=_string(payload, "cwd"),
            env_home=_string(payload, "env_home"),
            task_id=_optional_string(payload, "task_id"),
            execution_id=_optional_string(payload, "execution_id"),
            dynamic_tool_allowlist=allowlist,
            output_schema=schema,
        )


@dataclass(frozen=True, slots=True)
class LlmExecutionCheckpoint:
    """One resumable native thread and the exact acknowledged turn, if any."""

    binding: LlmCheckpointBinding
    state: CheckpointState
    session_id: str | None
    receipt: SendReceipt | None = None
    turn_index: int = 0
    text: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {
            "opened",
            "sending",
            "accepted",
            "pausing",
            "paused",
            "completed",
            "cancelled",
            "failed",
        }:
            raise ValueError(f"unsupported LLM checkpoint state: {self.state!r}")
        if self.session_id is not None and (
            not isinstance(self.session_id, str) or not self.session_id.strip()
        ):
            raise ValueError("session_id must be a non-empty string or None")
        if not isinstance(self.turn_index, int) or isinstance(self.turn_index, bool) or self.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if self.text is not None and not isinstance(self.text, str):
            raise TypeError("checkpoint text must be a string or None")
        if self.state in {"opened", "sending"} and self.receipt is not None:
            raise ValueError(f"{self.state} checkpoint cannot contain a delivery receipt")
        if self.state in {"accepted", "pausing", "paused"} and self.receipt is None:
            raise ValueError(f"{self.state} checkpoint requires an acknowledged delivery receipt")
        if self.state == "completed" and (self.session_id is None or self.text is None):
            raise ValueError("completed checkpoint requires a session id and final text")
        if not isinstance(self.binding, LlmCheckpointBinding):
            raise TypeError("binding must be an LlmCheckpointBinding")

    def to_json(self) -> JsonObject:
        receipt: JsonObject | None = None
        if self.receipt is not None:
            receipt = {
                "backend_operation_id": self.receipt.backend_operation_id,
                "metadata": (
                    require_json_object(self.receipt.metadata, label="SendReceipt.metadata")
                    if self.receipt.metadata is not None
                    else None
                ),
            }
        return {
            "version": 1,
            "binding": self.binding.to_json(),
            "state": self.state,
            "session_id": self.session_id,
            "receipt": receipt,
            "turn_index": self.turn_index,
            "text": self.text,
        }

    @classmethod
    def from_json(cls, value: object) -> LlmExecutionCheckpoint:
        payload = require_json_object(value, label="LLM execution checkpoint")
        if payload.get("version") != 1:
            raise ValueError("unsupported LLM checkpoint version")
        state = payload.get("state")
        if not isinstance(state, str):
            raise TypeError("LLM checkpoint state must be a string")
        receipt_value = payload.get("receipt")
        receipt: SendReceipt | None
        if receipt_value is None:
            receipt = None
        else:
            receipt_payload = require_json_object(receipt_value, label="LLM checkpoint receipt")
            operation_id = _optional_string(receipt_payload, "backend_operation_id")
            metadata_value = receipt_payload.get("metadata")
            metadata = (
                require_json_object(metadata_value, label="LLM checkpoint receipt metadata")
                if metadata_value is not None
                else None
            )
            receipt = SendReceipt(backend_operation_id=operation_id, metadata=metadata)
        session_id = _optional_string(payload, "session_id")
        text = _optional_string(payload, "text")
        turn_index = payload.get("turn_index")
        if not isinstance(turn_index, int) or isinstance(turn_index, bool):
            raise TypeError("LLM checkpoint turn_index must be an integer")
        return cls(
            binding=LlmCheckpointBinding.from_json(payload.get("binding")),
            state=cast(CheckpointState, state),
            session_id=session_id,
            receipt=receipt,
            turn_index=turn_index,
            text=text,
        )


@runtime_checkable
class LlmExecutionJournal(Protocol):
    """Host-owned, lease-bound storage for one typed execution checkpoint."""

    run_id: str

    async def load(self) -> LlmExecutionCheckpoint | None: ...

    async def save(self, checkpoint: LlmExecutionCheckpoint) -> None: ...


def _string(payload: JsonObject, name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"LLM checkpoint {name} must be a string")
    return value


def _optional_string(payload: JsonObject, name: str) -> str | None:
    value = payload.get(name)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"LLM checkpoint {name} must be a string or None")
    return value
