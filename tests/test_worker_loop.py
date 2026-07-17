import asyncio

import pytest

from soveren_agent_platform.runtime.worker_loop import PollingWorkerConfig, run_polling_worker


def test_polling_worker_processes_claimed_items_until_stopped():
    async def run():
        stop_event = asyncio.Event()
        batches = [[1, 2], []]
        processed: list[int] = []

        async def claim():
            return batches.pop(0) if batches else []

        async def process(item: int):
            processed.append(item)
            if item == 2:
                stop_event.set()

        await run_polling_worker(
            stop_event,
            config=PollingWorkerConfig(name="test", idle_initial_s=0.01),
            claim=claim,
            process=process,
        )
        return processed

    assert asyncio.run(run()) == [1, 2]


def test_polling_worker_renews_waiting_items_in_a_claimed_batch():
    async def run() -> dict[int, int]:
        stop_event = asyncio.Event()
        claimed = False
        renewals = {1: 0, 2: 0}

        async def claim() -> list[int]:
            nonlocal claimed
            if claimed:
                return []
            claimed = True
            return [1, 2]

        async def renew(item: int) -> bool:
            renewals[item] += 1
            return True

        async def process(item: int) -> None:
            if item == 1:
                await asyncio.sleep(0.05)
                return
            stop_event.set()

        await run_polling_worker(
            stop_event,
            config=PollingWorkerConfig(name="test", idle_initial_s=0.01),
            claim=claim,
            process=process,
            renew_lease=renew,
            lease_renew_interval_s=0.01,
        )
        return renewals

    renewals = asyncio.run(run())

    assert renewals[1] >= 2
    assert renewals[2] >= 2


def test_polling_worker_cancels_processing_when_lease_is_lost():
    async def run() -> bool:
        stop_event = asyncio.Event()
        claimed = False
        renewal_count = 0
        cancelled = False

        async def claim() -> list[int]:
            nonlocal claimed
            if claimed:
                return []
            claimed = True
            return [1]

        async def renew(item: int) -> bool:
            nonlocal renewal_count
            renewal_count += 1
            return renewal_count == 1

        async def process(item: int) -> None:
            nonlocal cancelled
            try:
                await asyncio.Event().wait()
            finally:
                cancelled = True
                stop_event.set()

        await run_polling_worker(
            stop_event,
            config=PollingWorkerConfig(name="test", idle_initial_s=0.01),
            claim=claim,
            process=process,
            renew_lease=renew,
            lease_renew_interval_s=0.01,
        )
        return cancelled

    assert asyncio.run(run()) is True


def test_polling_worker_resets_claim_failure_budget_after_success():
    async def run() -> list[int]:
        stop_event = asyncio.Event()
        outcomes: list[RuntimeError | list[int]] = [
            RuntimeError("temporary-1"),
            [1],
            RuntimeError("temporary-2"),
            [2],
        ]
        processed: list[int] = []

        async def claim() -> list[int]:
            outcome = outcomes.pop(0)
            if isinstance(outcome, RuntimeError):
                raise outcome
            return outcome

        async def process(item: int) -> None:
            processed.append(item)
            if item == 2:
                stop_event.set()

        await run_polling_worker(
            stop_event,
            config=PollingWorkerConfig(
                name="test",
                idle_initial_s=0,
                idle_max_s=0,
                max_consecutive_failures=2,
            ),
            claim=claim,
            process=process,
        )
        return processed

    assert asyncio.run(run()) == [1, 2]


def test_polling_worker_raises_after_consecutive_claim_failure_limit():
    async def run() -> int:
        stop_event = asyncio.Event()
        calls = 0

        async def claim() -> list[int]:
            nonlocal calls
            calls += 1
            raise RuntimeError("storage unavailable")

        async def process(item: int) -> None:
            raise AssertionError("nothing should be claimed")

        with pytest.raises(RuntimeError, match="storage unavailable"):
            await run_polling_worker(
                stop_event,
                config=PollingWorkerConfig(
                    name="test",
                    idle_initial_s=0,
                    idle_max_s=0,
                    max_consecutive_failures=3,
                ),
                claim=claim,
                process=process,
            )
        return calls

    assert asyncio.run(run()) == 3


def test_polling_worker_rejects_non_positive_claim_failure_limit():
    async def run() -> None:
        with pytest.raises(ValueError, match="positive integer"):
            await run_polling_worker(
                asyncio.Event(),
                config=PollingWorkerConfig(name="test", max_consecutive_failures=0),
                claim=lambda: asyncio.sleep(0, result=[]),
                process=lambda item: asyncio.sleep(0),
            )

    asyncio.run(run())
