"""Application bootstrap and orchestrator."""

import asyncio

import structlog

from durable_engine.config.models import EngineConfig
from durable_engine.ingestion.reader_factory import ReaderFactory
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.observability.reporter import MetricsReporter
from durable_engine.orchestrator.engine import FanOutEngine
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.sinks.sink_factory import SinkFactory
from durable_engine.transformation.transformer_registry import TransformerRegistry

logger = structlog.get_logger()


class DurableEngine:
    """Top-level application that wires all components together."""

    def __init__(self, config: EngineConfig) -> None:
        self.config = config
        self._shutdown_event = asyncio.Event()
        self._metrics = MetricsCollector()

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    async def run(self) -> int:
        logger.info("engine_initializing", engine_name=self.config.engine.name)

        source = ReaderFactory.create(self.config.ingestion)
        transformer_registry = TransformerRegistry()
        dlq = DeadLetterQueue(self.config.dlq)
        sinks = SinkFactory.create_all(self.config.sinks, self._metrics)

        if not sinks:
            logger.error("no_sinks_enabled")
            return 1

        reporter = MetricsReporter(
            metrics=self._metrics,
            config=self.config.observability.reporter,
            sinks=sinks,
        )

        engine = FanOutEngine(
            config=self.config.engine,
            reader=source,
            transformer_registry=transformer_registry,
            sinks=sinks,
            metrics=self._metrics,
            dlq=dlq,
            shutdown_event=self._shutdown_event,
        )

        reporter_task = asyncio.create_task(reporter.start())

        try:
            await engine.start()
            logger.info("engine_completed_successfully")
            return 0
        except Exception:
            logger.exception("engine_failed")
            return 1
        finally:
            self._shutdown_event.set()
            reporter_task.cancel()
            try:
                await reporter_task
            except asyncio.CancelledError:
                pass

            # Give dispatchers a bounded time to drain
            try:
                await asyncio.wait_for(
                    engine.cleanup(),
                    timeout=self.config.engine.shutdown_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning("cleanup_timed_out")

            reporter.print_final_summary()
            logger.info("engine_shutdown_complete")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
