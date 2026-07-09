"""Dynamic tools for explicit platform memory access."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from soveren_agent_platform.memory.contracts import MemoryRecord, MemoryStore
from soveren_agent_platform.model_boundary import ModelRedactionPolicy, redact_value_for_model
from soveren_agent_platform.sessions.backends.codex_tools import (
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
)

MEMORY_TOOL_NAMESPACE = "platform.memory"


@dataclass(frozen=True, slots=True)
class MemoryToolAccess:
    scope: str | None = None
    subject_id: str | None = None
    allow_scope_override: bool = False
    allow_subject_override: bool = False


def register_memory_tools(
    registry: DynamicToolRegistry,
    store: MemoryStore,
    *,
    tenant_id: str,
    access: MemoryToolAccess | None = None,
    allow_write: bool = False,
    model_redaction_policy: ModelRedactionPolicy | None = None,
) -> None:
    access = access or MemoryToolAccess()
    registry.register(
        DynamicToolSpec(
            name="search_memory",
            namespace=MEMORY_TOOL_NAMESPACE,
            description="Search explicitly stored platform memory records.",
            input_schema=_search_schema(access),
        ),
        lambda call: _search_memory_tool(
            store,
            call,
            tenant_id=tenant_id,
            access=access,
            model_redaction_policy=model_redaction_policy,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="get_memory",
            namespace=MEMORY_TOOL_NAMESPACE,
            description="Read one explicitly stored platform memory record by id.",
            input_schema={
                "type": "object",
                "required": ["memory_id"],
                "properties": {"memory_id": {"type": "string"}},
            },
        ),
        lambda call: _get_memory_tool(
            store,
            call,
            tenant_id=tenant_id,
            access=access,
            model_redaction_policy=model_redaction_policy,
        ),
    )
    if not allow_write:
        return
    registry.register(
        DynamicToolSpec(
            name="remember",
            namespace=MEMORY_TOOL_NAMESPACE,
            description="Store a durable memory record after app policy allows memory writes.",
            input_schema=_remember_schema(access),
        ),
        lambda call: _remember_tool(
            store,
            call,
            tenant_id=tenant_id,
            access=access,
        ),
    )
    registry.register(
        DynamicToolSpec(
            name="forget",
            namespace=MEMORY_TOOL_NAMESPACE,
            description="Soft-delete one explicitly stored platform memory record.",
            input_schema={
                "type": "object",
                "required": ["memory_id"],
                "properties": {"memory_id": {"type": "string"}},
            },
        ),
        lambda call: _forget_tool(store, call, tenant_id=tenant_id, access=access),
    )


async def _search_memory_tool(
    store: MemoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    access: MemoryToolAccess,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> DynamicToolResult:
    args = _args(call)
    resolved = _resolve_access(args, access, require_subject=False)
    if not resolved.success:
        return _access_denied(resolved.reason)
    records = await store.search(
        tenant_id=tenant_id,
        query=str(args.get("query") or ""),
        scope=resolved.scope,
        subject_id=resolved.subject_id,
        kind=_optional_str(args.get("kind")),
        limit=_limit(args.get("limit"), default=10),
    )
    return DynamicToolResult.json({
        "memories": [
            _record_payload(record, model_redaction_policy=model_redaction_policy)
            for record in records
        ]
    })


async def _get_memory_tool(
    store: MemoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    access: MemoryToolAccess,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> DynamicToolResult:
    memory_id = str(_args(call).get("memory_id") or "")
    record = await store.get(memory_id, tenant_id=tenant_id)
    if record is not None and not _record_allowed(record, access):
        record = None
    return DynamicToolResult.json({
        "memory": (
            _record_payload(record, model_redaction_policy=model_redaction_policy)
            if record is not None
            else None
        )
    })


async def _remember_tool(
    store: MemoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    access: MemoryToolAccess,
) -> DynamicToolResult:
    args = _args(call)
    resolved = _resolve_access(args, access, require_subject=True)
    if not resolved.success:
        return _access_denied(resolved.reason, created=False)
    memory_id, created = await store.remember(
        tenant_id=tenant_id,
        scope=resolved.scope or "",
        subject_id=resolved.subject_id or "",
        text=str(args.get("text") or ""),
        kind=str(args.get("kind") or "note"),
        metadata=_metadata(args.get("metadata")),
        confidence=_confidence(args.get("confidence")),
        idempotency_key=_optional_str(args.get("idempotency_key")),
        expires_at=_optional_int(args.get("expires_at")),
    )
    return DynamicToolResult.json({"memory_id": memory_id, "created": created})


async def _forget_tool(
    store: MemoryStore,
    call: DynamicToolCall,
    *,
    tenant_id: str,
    access: MemoryToolAccess,
) -> DynamicToolResult:
    memory_id = str(_args(call).get("memory_id") or "")
    record = await store.get(memory_id, tenant_id=tenant_id)
    if record is None or not _record_allowed(record, access):
        return DynamicToolResult.json({"memory_id": memory_id, "forgotten": False})
    forgotten = await store.forget(memory_id, tenant_id=tenant_id)
    return DynamicToolResult.json({"memory_id": memory_id, "forgotten": forgotten})


@dataclass(frozen=True, slots=True)
class _ResolvedAccess:
    success: bool
    scope: str | None = None
    subject_id: str | None = None
    reason: str = ""


def _resolve_access(args: dict[str, Any], access: MemoryToolAccess, *, require_subject: bool) -> _ResolvedAccess:
    requested_scope = _optional_str(args.get("scope"))
    requested_subject_id = _optional_str(args.get("subject_id"))
    scope = _resolve_field(
        name="scope",
        configured=access.scope,
        requested=requested_scope,
        allow_override=access.allow_scope_override,
    )
    if not scope.success:
        return _ResolvedAccess(False, reason=scope.reason)
    subject_id = _resolve_field(
        name="subject_id",
        configured=access.subject_id,
        requested=requested_subject_id,
        allow_override=access.allow_subject_override,
    )
    if not subject_id.success:
        return _ResolvedAccess(False, reason=subject_id.reason)
    if require_subject and (scope.value is None or subject_id.value is None):
        return _ResolvedAccess(False, reason="scope and subject_id are required")
    return _ResolvedAccess(True, scope=scope.value, subject_id=subject_id.value)


@dataclass(frozen=True, slots=True)
class _ResolvedField:
    success: bool
    value: str | None = None
    reason: str = ""


def _resolve_field(
    *,
    name: str,
    configured: str | None,
    requested: str | None,
    allow_override: bool,
) -> _ResolvedField:
    if configured is None:
        return _ResolvedField(True, requested)
    if allow_override:
        return _ResolvedField(True, requested or configured)
    if requested is not None and requested != configured:
        return _ResolvedField(False, reason=f"{name} is outside the registered memory access policy")
    return _ResolvedField(True, configured)


def _record_allowed(record: MemoryRecord, access: MemoryToolAccess) -> bool:
    if access.scope is not None and not access.allow_scope_override and record.scope != access.scope:
        return False
    if access.subject_id is not None and not access.allow_subject_override and record.subject_id != access.subject_id:
        return False
    return True


def _access_denied(reason: str, **extra: Any) -> DynamicToolResult:
    return DynamicToolResult.json({**extra, "reason": reason}, success=False)


def _search_schema(access: MemoryToolAccess) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "query": {"type": "string"},
        "kind": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
    }
    _add_access_properties(properties, access)
    return {"type": "object", "properties": properties}


def _remember_schema(access: MemoryToolAccess) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "text": {"type": "string"},
        "kind": {"type": "string"},
        "metadata": {"type": "object"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "idempotency_key": {"type": "string"},
        "expires_at": {"type": "integer"},
    }
    _add_access_properties(properties, access)
    return {"type": "object", "required": ["text"], "properties": properties}


def _add_access_properties(properties: dict[str, Any], access: MemoryToolAccess) -> None:
    if access.scope is None or access.allow_scope_override:
        properties["scope"] = {"type": "string"}
    if access.subject_id is None or access.allow_subject_override:
        properties["subject_id"] = {"type": "string"}


def _record_payload(
    record: MemoryRecord,
    *,
    model_redaction_policy: ModelRedactionPolicy | None,
) -> dict[str, Any]:
    payload = asdict(record)
    for key in (
        "tenant_id",
        "subject_id",
        "source_id",
        "source_event_id",
        "created_by",
        "idempotency_key",
        "deleted_at",
    ):
        payload.pop(key, None)
    redacted = redact_value_for_model(payload, policy=model_redaction_policy)
    return redacted if isinstance(redacted, dict) else {}


def _args(call: DynamicToolCall) -> dict[str, Any]:
    return call.arguments if isinstance(call.arguments, dict) else {}


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _metadata(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _confidence(value: Any) -> float:
    if isinstance(value, int | float):
        return max(0.0, min(float(value), 1.0))
    return 1.0


def _optional_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _limit(value: Any, *, default: int) -> int:
    if isinstance(value, int):
        return max(1, min(value, 50))
    return default
