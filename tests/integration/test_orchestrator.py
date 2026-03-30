"""Integration tests for the orchestrator with mock sinks."""

import asyncio
from pathlib import Path

import pytest

from durable_engine.config.models import (
    CircuitBreakerConfig,
    CoreEngineConfig,
    DlqConfig,
    RateLimitConfig,
    RetryConfig,
    SimulationConfig,
    SinkConfig,
)
from durable_engine.ingestion.csv_reader import CsvFileReader
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.orchestrator.engine import FanOutEngine
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.sinks.mq_sink import MessageQueueSink
from durable_engine.sinks.rest_sink import RestApiSink
from durable_engine.transformation.transformer_registry import TransformerRegistry


class TestOrchestrator:
    @pytest.mark.asyncio
    async def test_fan_out_to_multiple_sinks(self, sample_csv_path: Path, tmp_dir: Path) -> None:
        no_error_sim = SimulationConfig(latency_ms=1, latency_jitter_ms=0, error_rate=0.0)
        fast_rate = RateLimitConfig(requests_per_second=10000, burst_size=20000)
        fast_retry = RetryConfig(max_retries=1, base_delay_ms=10, max_delay_ms=50)
        loose_cb = CircuitBreakerConfig(failure_threshold=100)

        rest_config = SinkConfig(
            type="rest",
            transformer="json",
            rate_limit=fast_rate,
            retry=fast_retry,
            circuit_breaker=loose_cb,
            simulation=no_error_sim,
            concurrency=5,
            batch_size=10,
        )
        mq_config = SinkConfig(
            type="mq",
            transformer="xml",
            rate_limit=fast_rate,
            retry=fast_retry,
            circuit_breaker=loose_cb,
            simulation=no_error_sim,
            concurrency=5,
            batch_size=10,
            topic="test-topic",
            partitions=2,
        )

        metrics = MetricsCollector()
        metrics.register_sink("rest")
        metrics.register_sink("mq")

        sinks = {
            "rest": RestApiSink("rest", rest_config),
            "mq": MessageQueueSink("mq", mq_config),
        }

        reader = CsvFileReader(str(sample_csv_path))
        registry = TransformerRegistry()
        dlq = DeadLetterQueue(DlqConfig(output_dir=str(tmp_dir / "dlq")))

        engine_config = CoreEngineConfig(batch_size=5, max_queue_size=100, worker_count=2)

        engine = FanOutEngine(
            config=engine_config,
            reader=reader,
            transformer_registry=registry,
            sinks=sinks,
            metrics=metrics,
            dlq=dlq,
            shutdown_event=asyncio.Event(),
        )

        await engine.start()

        # Each sink should have processed all 10 records
        rest_m = metrics.get_sink_metrics("rest")
        mq_m = metrics.get_sink_metrics("mq")
        assert rest_m is not None and rest_m.total == 10
        assert mq_m is not None and mq_m.total == 10
