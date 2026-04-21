"""Application bootstrap and orchestrator."""

import asyncio
import contextlib

import structlog

from durable_engine.config.models import EngineConfig
from durable_engine.ingestion.reader_factory import ReaderFactory
from durable_engine.observability.health import HealthCheckServer
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.observability.prometheus_server import PrometheusServer
from durable_engine.observability.reporter import MetricsReporter
from durable_engine.orchestrator.engine import FanOutEngine
from durable_engine.resilience.checkpoint import CheckpointManager
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
        self._health_server: HealthCheckServer | None = None
        self._prometheus_server: PrometheusServer | None = None

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    async def run(self) -> int:
        logger.info("engine_initializing", engine_name=self.config.engine.name)

        ingestion_enabled = self.config.ingestion.enabled
        source = ReaderFactory.create(self.config.ingestion) if ingestion_enabled else None
        transformer_registry = TransformerRegistry()
        dlq = DeadLetterQueue(self.config.dlq)
        checkpoint = (
            CheckpointManager(self.config.ingestion.checkpoint) if ingestion_enabled else None
        )
        sinks = SinkFactory.create_all(self.config.sinks, self._metrics)

        if ingestion_enabled and not sinks:
            logger.error("no_sinks_enabled")
            return 1

        if not ingestion_enabled:
            logger.info(
                "standby_mode_enabled",
                source_type=self.config.ingestion.source_type,
            )

        # Start Prometheus metrics server
        if self.config.observability.metrics.enabled:
            self._metrics.enable_prometheus()
            self._prometheus_server = PrometheusServer(self.config.observability.metrics)
            await self._prometheus_server.start()

        # Start health check server
        if self.config.observability.health.enabled:
            self._health_server = HealthCheckServer(
                config=self.config.observability.health,
                metrics=self._metrics,
                sinks=sinks,
            )
            await self._health_server.start()

        reporter = MetricsReporter(
            metrics=self._metrics,
            config=self.config.observability.reporter,
            sinks=sinks,
        )

        engine: FanOutEngine | None = None
        if ingestion_enabled and source is not None:
            engine = FanOutEngine(
                config=self.config.engine,
                reader=source,
                transformer_registry=transformer_registry,
                sinks=sinks,
                metrics=self._metrics,
                dlq=dlq,
                shutdown_event=self._shutdown_event,
                checkpoint=checkpoint,
            )

        reporter_task = asyncio.create_task(reporter.start())

        try:
            if engine is not None:
                await engine.start()
                logger.info("engine_completed_successfully")
            else:
                logger.info("standby_waiting_for_shutdown")
                await self._shutdown_event.wait()
                logger.info("standby_shutdown_complete")
            return 0
        except Exception:
            logger.exception("engine_failed")
            return 1
        finally:
            self._shutdown_event.set()
            reporter_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reporter_task

            # Give dispatchers a bounded time to drain
            if engine is not None:
                try:
                    await asyncio.wait_for(
                        engine.cleanup(),
                        timeout=self.config.engine.shutdown_timeout_seconds,
                    )
                except TimeoutError:
                    logger.warning("cleanup_timed_out")

            # Stop observability servers
            if self._health_server:
                await self._health_server.stop()
            if self._prometheus_server:
                await self._prometheus_server.stop()

            reporter.print_final_summary()
            logger.info("engine_shutdown_complete")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
