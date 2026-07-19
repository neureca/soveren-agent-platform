"""Durable Telegram text delivery helpers."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.outbound.contracts import OutboundQueue, OutboundRequest

TELEGRAM_TEXT_LIMIT = 4096
_PART_METADATA_KEY = "_soveren_telegram_part"


def split_telegram_text(
    text: str,
    *,
    limit: int = TELEGRAM_TEXT_LIMIT,
) -> tuple[str, ...]:
    """Split text deterministically without changing its content."""
    if not isinstance(text, str):
        raise TypeError("text must be a string")
    if not text:
        raise ValueError("text must be non-empty")
    if limit < 1:
        raise ValueError("limit must be positive")
    return tuple(text[offset : offset + limit] for offset in range(0, len(text), limit))


async def enqueue_telegram_text(
    queue: OutboundQueue,
    *,
    tenant_id: str,
    source_id: str,
    destination_id: str,
    text: str,
    idempotency_key: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    run_after: int | None = None,
    max_attempts: int = 5,
    correlation_id: str | None = None,
) -> tuple[str | None, ...]:
    """Enqueue each Telegram-safe text part as its own durable effect."""
    if not idempotency_key.strip():
        raise ValueError("idempotency_key must be non-empty")
    base_payload = dict(payload or {})
    if _PART_METADATA_KEY in base_payload:
        raise ValueError(f"payload key {_PART_METADATA_KEY!r} is reserved")
    parts = split_telegram_text(text)
    if len(parts) > 1 and base_payload.get("parse_mode") is not None:
        raise ValueError("long Telegram text with parse_mode must be rendered or split by the app")
    requests: list[OutboundRequest] = []
    for index, part in enumerate(parts, start=1):
        part_payload = {
            **base_payload,
            _PART_METADATA_KEY: {"index": index, "count": len(parts)},
        }
        requests.append(
            OutboundRequest(
                tenant_id=tenant_id,
                source_id=source_id,
                channel="telegram",
                destination_id=destination_id,
                text=part,
                idempotency_key=f"{idempotency_key}:part:{index}",
                payload=part_payload,
                priority=priority,
                run_after=run_after,
                max_attempts=max_attempts,
                correlation_id=correlation_id,
                ordering_key=idempotency_key if len(parts) > 1 else None,
                ordering_position=index if len(parts) > 1 else None,
            )
        )
    return await queue.enqueue_many(requests)
