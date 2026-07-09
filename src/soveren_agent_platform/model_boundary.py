"""Shared redaction primitives for data crossing the model boundary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DEFAULT_MODEL_REDACT_KEYS = frozenset({
    "batch_raw_event_ids",
    "causation_id",
    "chat_id",
    "correlation_id",
    "destination_id",
    "from_first_name",
    "from_user_id",
    "from_username",
    "raw",
    "raw_event_id",
    "source_event_id",
    "source_id",
    "update_id",
    "user_id",
    "username",
})


@dataclass(frozen=True, slots=True)
class ModelRedactionPolicy:
    """Controls which structured fields are removed before calling an LLM."""

    redact_keys: frozenset[str] = DEFAULT_MODEL_REDACT_KEYS
    replacement_prefix: str = "[redacted"

    def replacement(self, key: str) -> str:
        return f"{self.replacement_prefix}:{key}]"


def redact_value_for_model(value: Any, *, policy: ModelRedactionPolicy | None = None) -> Any:
    active_policy = policy or ModelRedactionPolicy()
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if key in active_policy.redact_keys:
                redacted[key] = active_policy.replacement(key)
            else:
                redacted[key] = redact_value_for_model(raw_value, policy=active_policy)
        return redacted
    if isinstance(value, list):
        return [redact_value_for_model(item, policy=active_policy) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value_for_model(item, policy=active_policy) for item in value)
    return value
