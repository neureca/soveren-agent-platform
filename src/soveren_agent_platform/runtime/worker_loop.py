"""Reusable cooperative polling worker loop."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

T = TypeVar("T")

log = logging.getLogger(__name__)

DEFAULT_MAX_CONSECUTIVE_FAILURES = 5

ClaimBatch = Callable[[], Awaitable[Sequence[T]]]
ProcessItem = Callable[[T], Awaitable[None]]
RenewLease = Callable[[T], Awaitable[bool]]


class LeaseLostError(RuntimeError):
    """The worker no longer owns the durable lease for an item."""


@dataclass(frozen=True, slots=True)
class PollingWorkerConfig:
    name: str
    idle_initial_s: float = 1.0
    idle_max_s: float = 10.0
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES


@dataclass(slots=True)
class ConsecutiveFailureGuard:
    """Distinguish recoverable one-off failures from a persistently broken worker."""

    limit: int = DEFAULT_MAX_CONSECUTIVE_FAILURES
    count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.limit, int) or isinstance(self.limit, bool) or self.limit < 1:
            raise ValueError("consecutive failure limit must be a positive integer")

    def record_failure(self) -> int:
        self.count += 1
        return self.count

    def reset(self) -> None:
        self.count = 0

    @property
    def exhausted(self) -> bool:
        return self.count >= self.limit


async def run_polling_worker(
    stop_event: asyncio.Event,
    *,
    config: PollingWorkerConfig,
    claim: ClaimBatch[T],
    process: ProcessItem[T],
    renew_lease: RenewLease[T] | None = None,
    lease_renew_interval_s: float | None = None,
) -> None:
    """Run `claim`/`process` until stopped, with exponential idle backoff."""
    idle = config.idle_initial_s
    claim_failures = ConsecutiveFailureGuard(config.max_consecutive_failures)
    log.info("polling worker started name=%s", config.name)
    try:
        while not stop_event.is_set():
            try:
                items = list(await claim())
            except Exception:
                failure_count = claim_failures.record_failure()
                log.exception(
                    "polling worker claim failed name=%s consecutive_failures=%d/%d",
                    config.name,
                    failure_count,
                    claim_failures.limit,
                )
                if claim_failures.exhausted:
                    raise
                items = []
            else:
                claim_failures.reset()
            if not items:
                await sleep_or_stop(stop_event, idle)
                idle = min(idle * 2, config.idle_max_s)
                continue
            idle = config.idle_initial_s
            if renew_lease is None:
                for item in items:
                    await process(item)
                continue
            if lease_renew_interval_s is None or lease_renew_interval_s <= 0:
                raise ValueError("lease_renew_interval_s must be positive when lease renewal is enabled")
            guards = [
                _LeaseGuard(
                    item=item,
                    renew=renew_lease,
                    interval_s=lease_renew_interval_s,
                    worker_name=config.name,
                )
                for item in items
            ]
            try:
                await asyncio.gather(*(guard.start() for guard in guards))
                for guard in guards:
                    try:
                        await guard.process(process)
                    except LeaseLostError:
                        log.error("polling worker lost lease name=%s item=%r", config.name, guard.item)
            finally:
                await asyncio.gather(*(guard.close() for guard in guards))
    finally:
        log.info("polling worker stopped name=%s", config.name)


async def sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


class _LeaseGuard[T]:
    def __init__(
        self,
        *,
        item: T,
        renew: RenewLease[T],
        interval_s: float,
        worker_name: str,
    ) -> None:
        self.item = item
        self._renew = renew
        self._interval_s = interval_s
        self._worker_name = worker_name
        self._lost = asyncio.Event()
        self._heartbeat: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not await self._renew_once():
            return
        self._heartbeat = asyncio.create_task(self._heartbeat_loop())

    async def process(self, process: ProcessItem[T]) -> None:
        if self._lost.is_set():
            raise LeaseLostError("lease was lost before processing started")
        process_task: asyncio.Future[None] = asyncio.ensure_future(process(self.item))
        lost_task = asyncio.create_task(self._lost.wait())
        try:
            done, _ = await asyncio.wait((process_task, lost_task), return_when=asyncio.FIRST_COMPLETED)
            if process_task in done:
                await process_task
                return
            raise LeaseLostError("lease was lost while processing")
        finally:
            for task in (process_task, lost_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(process_task, lost_task, return_exceptions=True)

    async def close(self) -> None:
        if self._heartbeat is None:
            return
        self._heartbeat.cancel()
        await asyncio.gather(self._heartbeat, return_exceptions=True)
        self._heartbeat = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            if not await self._renew_once():
                return

    async def _renew_once(self) -> bool:
        try:
            renewed = await self._renew(self.item)
        except Exception:
            log.exception("lease renewal failed name=%s item=%r", self._worker_name, self.item)
            renewed = False
        if not renewed:
            self._lost.set()
        return renewed
