"""Shared idempotency conflict semantics for durable commands."""

from __future__ import annotations

import hashlib
import json
from typing import Any


class IdempotencyConflictError(ValueError):
    """An idempotency key was reused for a different command."""

    def __init__(self, *, resource: str, key: str, existing_id: str) -> None:
        self.resource = resource
        self.key = key
        self.existing_id = existing_id
        super().__init__(f"{resource} idempotency key was reused with different input")


def idempotency_fingerprint(value: Any) -> str:
    """Return a stable digest for one immutable command input."""
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def require_idempotent_replay(
    matches: bool,
    *,
    resource: str,
    key: str,
    existing_id: str,
) -> None:
    if not matches:
        raise IdempotencyConflictError(resource=resource, key=key, existing_id=existing_id)


def stored_json_matches(payload: str | None, expected: Any) -> bool:
    try:
        return json.loads(payload or "null") == expected
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
