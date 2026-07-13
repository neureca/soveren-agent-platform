import asyncio

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
