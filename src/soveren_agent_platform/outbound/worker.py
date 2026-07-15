"""Worker for outbound channel messages."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from pathlib import Path
from typing import Any

from soveren_agent_platform.outbound.contracts import (
    OutboundMessage,
    OutboundQueue,
    SendNotStartedError,
    SendResult,
)
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker

log = logging.getLogger(__name__)

LEASE_SECONDS = 60
BATCH_SIZE = 5
IDLE_INITIAL_S = 1.0
IDLE_MAX_S = 10.0
RETRY_BACKOFF_S = 30


def lease_owner(channel: str) -> str:
    return f"{socket.gethostname()}/outbound/{channel}"


async def run_outbound_worker(
    db_path: Path,
    stop_event: asyncio.Event,
    *,
    registry: OutboundRegistry,
    channel: str,
    tenant_id: str | None = None,
) -> None:
    async with await SQLiteOutboundQueue.open(db_path) as queue:
        await run_outbound_queue_worker(
            queue,
            stop_event,
            registry=registry,
            channel=channel,
            tenant_id=tenant_id,
        )


async def run_outbound_queue_worker(
    queue: OutboundQueue,
    stop_event: asyncio.Event,
    *,
    registry: OutboundRegistry,
    channel: str,
    tenant_id: str | None = None,
    batch_size: int = BATCH_SIZE,
    lease_seconds: int = LEASE_SECONDS,
    retry_backoff_s: int = RETRY_BACKOFF_S,
    idle_initial_s: float = IDLE_INITIAL_S,
    idle_max_s: float = IDLE_MAX_S,
) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    if tenant_id is not None and not tenant_id.strip():
        raise ValueError("tenant_id must be non-empty when provided")
    owner = lease_owner(channel)
    sender = registry.get(channel)

    async def claim() -> list[OutboundMessage]:
        if tenant_id is None:
            return await queue.claim_due(
                channel=channel,
                limit=batch_size,
                lease_owner=owner,
                lease_seconds=lease_seconds,
            )
        return await queue.claim_due(
            channel=channel,
            limit=batch_size,
            lease_owner=owner,
            lease_seconds=lease_seconds,
            tenant_id=tenant_id,
        )

    await run_polling_worker(
        stop_event,
        config=PollingWorkerConfig(
            name=f"outbound:{channel}",
            idle_initial_s=idle_initial_s,
            idle_max_s=idle_max_s,
        ),
        claim=claim,
        process=lambda message: _send_message(
            queue,
            message,
            sender=sender,
            retry_backoff_s=retry_backoff_s,
        ),
        renew_lease=lambda message: queue.renew_lease(
            message.id,
            lease_token=message.lease_token,
            lease_seconds=lease_seconds,
        ),
        lease_renew_interval_s=max(0.1, lease_seconds / 3),
    )


async def _send_message(
    queue: OutboundQueue,
    message: OutboundMessage,
    *,
    sender: Any,
    retry_backoff_s: int,
) -> None:
    if not await queue.mark_sending(message.id, lease_token=message.lease_token):
        log.error("outbound lease lost before send id=%s", message.id)
        return
    try:
        result = _validate_send_result(await sender.send(message))
        await _apply_send_result(
            queue,
            message,
            result=result,
            retry_backoff_s=retry_backoff_s,
        )
    except SendNotStartedError as exc:
        log.warning("outbound send did not start id=%s channel=%s", message.id, message.channel)
        await queue.mark_retry(
            message.id,
            lease_token=message.lease_token,
            run_after=int(time.time()) + retry_backoff_s,
            last_error=f"{type(exc).__name__}: {exc}",
        )
    except Exception as exc:
        log.exception("outbound send outcome is uncertain id=%s channel=%s", message.id, message.channel)
        await queue.mark_uncertain(
            message.id,
            lease_token=message.lease_token,
            last_error=f"{type(exc).__name__}: {exc}",
        )


async def _apply_send_result(
    queue: OutboundQueue,
    message: OutboundMessage,
    *,
    result: SendResult,
    retry_backoff_s: int,
) -> None:
    if result.status == "sent":
        marked = await queue.mark_sent(
            message.id,
            lease_token=message.lease_token,
            result=result.metadata,
        )
        if not marked:
            log.error("outbound send completed without owned lease id=%s", message.id)
        return
    if result.status == "retryable_failure":
        await queue.mark_retry(
            message.id,
            lease_token=message.lease_token,
            run_after=int(time.time())
            + (result.retry_after_s if result.retry_after_s is not None else retry_backoff_s),
            last_error=result.error or "outbound send failed transiently",
        )
        return
    if result.status == "permanent_failure":
        marked = await queue.mark_dead_letter(
            message.id,
            lease_token=message.lease_token,
            last_error=result.error or "outbound send failed permanently",
        )
        if not marked:
            log.error("outbound permanent failure could not be persisted id=%s", message.id)
        return
    raise ValueError(f"unsupported outbound send result status: {result.status!r}")


def _validate_send_result(value: object) -> SendResult:
    if not isinstance(value, SendResult):
        raise TypeError("channel sender must return SendResult")
    if value.status not in {"sent", "retryable_failure", "permanent_failure"}:
        raise ValueError(f"unsupported outbound send result status: {value.status!r}")
    if not isinstance(value.metadata, dict):
        raise TypeError("outbound send result metadata must be a dictionary")
    if value.error is not None and not isinstance(value.error, str):
        raise TypeError("outbound send result error must be a string or None")
    if value.retry_after_s is not None and (
        isinstance(value.retry_after_s, bool)
        or not isinstance(value.retry_after_s, int)
        or value.retry_after_s < 0
    ):
        raise ValueError("outbound send result retry_after_s must be a non-negative integer or None")
    return value
