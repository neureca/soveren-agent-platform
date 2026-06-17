"""Reusable cooperative polling worker loop."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")

log = logging.getLogger(__name__)

ClaimBatch = Callable[[], Awaitable[Sequence[T]]]
ProcessItem = Callable[[T], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PollingWorkerConfig:
    name: str
    idle_initial_s: float = 1.0
    idle_max_s: float = 10.0


async def run_polling_worker(
    stop_event: asyncio.Event,
    *,
    config: PollingWorkerConfig,
    claim: ClaimBatch[T],
    process: ProcessItem[T],
) -> None:
    """Run `claim`/`process` until stopped, with exponential idle backoff."""
    idle = config.idle_initial_s
    log.info("polling worker started name=%s", config.name)
    try:
        while not stop_event.is_set():
            try:
                items = list(await claim())
            except Exception:
                log.exception("polling worker claim failed name=%s", config.name)
                items = []
            if not items:
                await sleep_or_stop(stop_event, idle)
                idle = min(idle * 2, config.idle_max_s)
                continue
            idle = config.idle_initial_s
            for item in items:
                await process(item)
    finally:
        log.info("polling worker stopped name=%s", config.name)


async def sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
