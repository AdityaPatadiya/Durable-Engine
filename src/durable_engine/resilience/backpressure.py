"""Bounded async queue with backpressure support."""

import asyncio

import structlog

from durable_engine.ingestion.record import Record

logger = structlog.get_logger()


class BackpressureQueue:
    """Bounded async queue that applies backpressure when full.

    When the queue reaches max_size, the producer (put) will block
    until consumers (get) make space — preventing OOM.
    """

    def __init__(self, max_size: int, name: str = "") -> None:
        self._queue: asyncio.Queue[Record | None] = asyncio.Queue(maxsize=max_size)
        self._name = name
        self._total_put = 0
        self._total_get = 0
        self._closed = False

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def is_full(self) -> bool:
        return self._queue.full()

    @property
    def is_empty(self) -> bool:
        return self._queue.empty()

    @property
    def total_put(self) -> int:
        return self._total_put

    @property
    def total_get(self) -> int:
        return self._total_get

    async def put(self, record: Record) -> None:
        """Put a record into the queue. Blocks if full (backpressure)."""
        await self._queue.put(record)
        self._total_put += 1

        if self._queue.full():
            logger.debug("backpressure_active", queue=self._name, size=self._queue.qsize())

    async def get(self) -> Record | None:
        """Get a record from the queue. Returns None when closed and empty."""
        item = await self._queue.get()
        if item is not None:
            self._total_get += 1
        self._queue.task_done()
        return item

    async def get_batch(self, batch_size: int, wait_seconds: float = 0.1) -> list[Record]:
        """Get up to batch_size records, waiting up to wait_seconds for the first one."""
        batch: list[Record] = []

        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=wait_seconds)
            if first is None:
                self._queue.task_done()
                return batch
            batch.append(first)
            self._total_get += 1
            self._queue.task_done()
        except TimeoutError:
            return batch

        while len(batch) < batch_size:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    self._queue.task_done()
                    break
                batch.append(item)
                self._total_get += 1
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

        return batch

    async def close(self) -> None:
        """Signal that no more items will be added."""
        self._closed = True
        await self._queue.put(None)

    async def drain(self) -> None:
        """Wait until all items have been processed."""
        await self._queue.join()
