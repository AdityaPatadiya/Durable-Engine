"""Core fan-out engine — the main orchestrator."""

import asyncio

import structlog

from durable_engine.config.models import CoreEngineConfig, SinkConfig
from durable_engine.ingestion.base import FileReader
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.orchestrator.dispatcher import SinkDispatcher
from durable_engine.orchestrator.pipeline import IngestionPipeline
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.sinks.base import BaseSink
from durable_engine.transformation.transformer_registry import TransformerRegistry

logger = structlog.get_logger()


class FanOutEngine:
    """Core engine that orchestrates the entire fan-out pipeline."""

    def __init__(
        self,
        config: CoreEngineConfig,
        reader: FileReader,
        transformer_registry: TransformerRegistry,
        sinks: dict[str, BaseSink],
        metrics: MetricsCollector,
        dlq: DeadLetterQueue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._config = config
        self._reader = reader
        self._transformer_registry = transformer_registry
        self._sinks = sinks
        self._metrics = metrics
        self._dlq = dlq
        self._shutdown_event = shutdown_event
        self._dispatchers: dict[str, SinkDispatcher] = {}
        self._pipeline: IngestionPipeline | None = None

    async def start(self) -> None:
        """Start the fan-out engine."""
        logger.info(
            "engine_starting",
            sinks=list(self._sinks.keys()),
            batch_size=self._config.batch_size,
            workers=self._config.worker_count,
        )

        # Create dispatchers for each sink
        for sink_name, sink in self._sinks.items():
            # Get the sink config to find the transformer name
            transformer_name = self._get_transformer_name(sink)
            transformer = self._transformer_registry.get(transformer_name)

            dispatcher = SinkDispatcher(
                sink_name=sink_name,
                sink=sink,
                transformer=transformer,
                config=sink.config,
                metrics=self._metrics,
                dlq=self._dlq,
                queue_size=self._config.max_queue_size,
            )
            self._dispatchers[sink_name] = dispatcher

        # Start all dispatcher workers
        start_tasks = [
            dispatcher.start_workers(num_workers=self._config.worker_count)
            for dispatcher in self._dispatchers.values()
        ]
        await asyncio.gather(*start_tasks)

        # Create and run the ingestion pipeline
        self._pipeline = IngestionPipeline(
            reader=self._reader,
            dispatchers=self._dispatchers,
            batch_size=self._config.batch_size,
        )

        await self._pipeline.run(self._shutdown_event)

        # Wait for all dispatchers to finish processing
        stop_tasks = [
            dispatcher.stop() for dispatcher in self._dispatchers.values()
        ]
        await asyncio.gather(*stop_tasks)

        self._metrics.set_ingested_count(self._pipeline.records_ingested)

        logger.info(
            "engine_finished",
            records_ingested=self._pipeline.records_ingested,
            dlq_total=self._dlq.get_total_count(),
        )

    def _get_transformer_name(self, sink: BaseSink) -> str:
        """Extract the transformer name from the sink's config."""
        return sink.config.transformer

    async def cleanup(self) -> None:
        """Clean up resources."""
        for dispatcher in self._dispatchers.values():
            try:
                await dispatcher.stop()
            except Exception:
                logger.exception("cleanup_error", sink=dispatcher.sink_name)
