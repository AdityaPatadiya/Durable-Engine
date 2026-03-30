"""Integration tests for the full ingestion pipeline."""

import asyncio
from pathlib import Path

import pytest

from durable_engine.config.models import (
    CircuitBreakerConfig,
    DlqConfig,
    RateLimitConfig,
    RetryConfig,
    SimulationConfig,
    SinkConfig,
)
from durable_engine.ingestion.csv_reader import CsvFileReader
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.orchestrator.dispatcher import SinkDispatcher
from durable_engine.orchestrator.pipeline import IngestionPipeline
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.sinks.rest_sink import RestApiSink
from durable_engine.transformation.json_transformer import JsonTransformer


class TestPipeline:
    @pytest.mark.asyncio
    async def test_full_pipeline_processes_all_records(
        self, sample_csv_path: Path, tmp_dir: Path
    ) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        metrics = MetricsCollector()
        metrics.register_sink("test_rest")
        dlq = DeadLetterQueue(DlqConfig(output_dir=str(tmp_dir / "dlq")))

        sink_config = SinkConfig(
            type="rest",
            transformer="json",
            rate_limit=RateLimitConfig(requests_per_second=10000, burst_size=20000),
            retry=RetryConfig(max_retries=3, base_delay_ms=10, max_delay_ms=50),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=100),
            simulation=SimulationConfig(latency_ms=1, latency_jitter_ms=0, error_rate=0.0),
            concurrency=5,
            batch_size=10,
        )

        sink = RestApiSink("test_rest", sink_config)
        transformer = JsonTransformer()

        dispatcher = SinkDispatcher(
            sink_name="test_rest",
            sink=sink,
            transformer=transformer,
            config=sink_config,
            metrics=metrics,
            dlq=dlq,
            queue_size=1000,
        )

        dispatchers = {"test_rest": dispatcher}

        # Start workers
        await dispatcher.start_workers(num_workers=2)

        # Run pipeline
        pipeline = IngestionPipeline(reader, dispatchers, batch_size=5)
        shutdown = asyncio.Event()
        await pipeline.run(shutdown)

        # Wait for processing
        await dispatcher.stop()

        # Verify all records processed
        assert pipeline.records_ingested == 10
        sink_metrics = metrics.get_sink_metrics("test_rest")
        assert sink_metrics is not None
        assert sink_metrics.total == 10  # All records accounted for
