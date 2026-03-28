"""Pipeline wiring: ingestion -> fan-out to dispatchers."""

import asyncio
from collections.abc import Generator

import structlog

from durable_engine.ingestion.base import FileReader
from durable_engine.ingestion.record import Record
from durable_engine.orchestrator.dispatcher import SinkDispatcher

logger = structlog.get_logger()


class IngestionPipeline:
    """Reads records from the source and fans them out to all sink dispatchers."""

    def __init__(
        self,
        reader: FileReader,
        dispatchers: dict[str, SinkDispatcher],
        batch_size: int = 500,
    ) -> None:
        self._reader = reader
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
            source=str(self._reader.file_path),
            sinks=list(self._dispatchers.keys()),
        )

        loop = asyncio.get_event_loop()

        # Run the blocking file read in a thread to not block the event loop
        reader_gen = self._reader.read_records()

        batch: list[Record] = []
        for record in reader_gen:
            if shutdown_event.is_set():
                logger.info("pipeline_shutdown_requested")
                break

            batch.append(record)
            self._records_ingested += 1

            if len(batch) >= self._batch_size:
                await self._dispatch_batch(batch)
                batch = []

        # Dispatch remaining records
        if batch:
            await self._dispatch_batch(batch)

        # Signal all dispatchers that ingestion is complete
        for dispatcher in self._dispatchers.values():
            await dispatcher.queue.close()

        logger.info(
            "pipeline_ingestion_complete",
            total_records=self._records_ingested,
        )

    async def _dispatch_batch(self, batch: list[Record]) -> None:
        """Fan-out a batch of records to all dispatcher queues."""
        for record in batch:
            tasks = [
                dispatcher.queue.put(record)
                for dispatcher in self._dispatchers.values()
            ]
            await asyncio.gather(*tasks)
