"""Pipeline wiring: ingestion -> fan-out to dispatchers."""

import asyncio

import structlog

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record
from durable_engine.orchestrator.dispatcher import SinkDispatcher

logger = structlog.get_logger()


class IngestionPipeline:
    """Reads records from any source (file or live stream) and fans them out."""

    def __init__(
        self,
        source: RecordSource,
        dispatchers: dict[str, SinkDispatcher],
        batch_size: int = 500,
    ) -> None:
        self._source = source
        self._dispatchers = dispatchers
        self._batch_size = batch_size
        self._records_ingested = 0

    @property
    def records_ingested(self) -> int:
        return self._records_ingested

    async def run(self, shutdown_event: asyncio.Event) -> None:
        """Read records and fan-out to all dispatcher queues."""
        logger.info(
            "pipeline_starting",
            source=self._source.source_name,
            source_type=type(self._source).__name__,
            streaming=self._source.is_streaming,
            sinks=list(self._dispatchers.keys()),
        )

        await self._source.setup()

        try:
            await self._ingest_loop(shutdown_event)
        finally:
            await self._source.teardown()

            for dispatcher in self._dispatchers.values():
                await dispatcher.queue.close()

        logger.info(
            "pipeline_ingestion_complete",
            total_records=self._records_ingested,
        )

    async def _ingest_loop(self, shutdown_event: asyncio.Event) -> None:
        """Core ingestion loop — reads from source, dispatches to sinks.

        For streaming sources, we race each record read against the shutdown event
        so Ctrl+C terminates promptly.
        """
        batch: list[Record] = []
        reader_iter = self._source.read_records_async().__aiter__()

        while not shutdown_event.is_set():
            try:
                # Race: get next record vs shutdown signal
                read_task = asyncio.create_task(reader_iter.__anext__())
                shutdown_task = asyncio.create_task(shutdown_event.wait())

                done, pending = await asyncio.wait(
                    {read_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel whichever didn't finish
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, StopAsyncIteration):
                        pass

                if shutdown_task in done:
                    logger.info("pipeline_shutdown_requested")
                    break

                if read_task in done:
                    try:
                        record = read_task.result()
                    except StopAsyncIteration:
                        break  # Source exhausted (file finished)

                    batch.append(record)
                    self._records_ingested += 1

                    if len(batch) >= self._batch_size:
                        await self._dispatch_batch(batch)
                        batch = []

            except StopAsyncIteration:
                break

        # Dispatch remaining records
        if batch:
            await self._dispatch_batch(batch)

    async def _dispatch_batch(self, batch: list[Record]) -> None:
        """Fan-out a batch of records to all dispatcher queues."""
        for record in batch:
            tasks = [
                dispatcher.queue.put(record)
                for dispatcher in self._dispatchers.values()
            ]
            await asyncio.gather(*tasks)
