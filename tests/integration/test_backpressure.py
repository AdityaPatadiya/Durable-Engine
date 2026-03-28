"""Integration tests for backpressure behavior."""

import asyncio

import pytest

from durable_engine.ingestion.record import Record
from durable_engine.resilience.backpressure import BackpressureQueue


class TestBackpressure:
    @pytest.mark.asyncio
    async def test_queue_blocks_when_full(self) -> None:
        queue = BackpressureQueue(max_size=3, name="test")

        for i in range(3):
            await queue.put(Record.from_dict({"i": i}, "f", i))

        assert queue.is_full

        # Try to put another — should block
        put_done = False

        async def delayed_put():
            nonlocal put_done
            await queue.put(Record.from_dict({"i": 99}, "f", 99))
            put_done = True

        task = asyncio.create_task(delayed_put())
        await asyncio.sleep(0.05)
        assert not put_done  # Still blocked

        # Consume one to unblock
        await queue.get()
        await asyncio.sleep(0.05)
        assert put_done

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_get_batch(self) -> None:
        queue = BackpressureQueue(max_size=100, name="test")

        for i in range(10):
            await queue.put(Record.from_dict({"i": i}, "f", i))

        batch = await queue.get_batch(batch_size=5, timeout=0.5)
        assert len(batch) == 5

    @pytest.mark.asyncio
    async def test_close_signals_consumers(self) -> None:
        queue = BackpressureQueue(max_size=10, name="test")
        await queue.put(Record.from_dict({"a": 1}, "f", 1))
        await queue.close()

        r1 = await queue.get()
        assert r1 is not None
        r2 = await queue.get()
        assert r2 is None  # Poison pill

    @pytest.mark.asyncio
    async def test_total_counts(self) -> None:
        queue = BackpressureQueue(max_size=100, name="test")

        for i in range(5):
            await queue.put(Record.from_dict({"i": i}, "f", i))

        for _ in range(5):
            await queue.get()

        assert queue.total_put == 5
        assert queue.total_get == 5
