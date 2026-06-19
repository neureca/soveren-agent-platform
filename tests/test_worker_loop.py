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
