"""Performance tests for throughput benchmarking."""

import time
from pathlib import Path

import pytest

from durable_engine.config.models import (
    CircuitBreakerConfig,
    RateLimitConfig,
    RetryConfig,
    SimulationConfig,
    SinkConfig,
)
from durable_engine.ingestion.record import Record
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.orchestrator.dispatcher import SinkDispatcher
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.sinks.rest_sink import RestApiSink
from durable_engine.transformation.json_transformer import JsonTransformer


class TestThroughput:
    @pytest.mark.asyncio
    async def test_minimum_throughput(self, tmp_dir: Path) -> None:
        """Verify the engine can process at least 100 records/sec with a fast sink."""
        num_records = 500

        sink_config = SinkConfig(
            type="rest",
            transformer="json",
            rate_limit=RateLimitConfig(requests_per_second=100000, burst_size=200000),
            retry=RetryConfig(max_retries=0),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=1000),
            simulation=SimulationConfig(latency_ms=0, latency_jitter_ms=0, error_rate=0.0),
            concurrency=50,
            batch_size=100,
        )

        metrics = MetricsCollector()
        metrics.register_sink("perf_test")
        dlq = DeadLetterQueue(
            config=__import__("durable_engine.config.models", fromlist=["DlqConfig"]).DlqConfig(
                output_dir=str(tmp_dir)
            )
        )

        sink = RestApiSink("perf_test", sink_config)
        transformer = JsonTransformer()

        dispatcher = SinkDispatcher(
            sink_name="perf_test",
            sink=sink,
            transformer=transformer,
            config=sink_config,
            metrics=metrics,
            dlq=dlq,
            queue_size=10000,
        )

        await dispatcher.start_workers(num_workers=4)

        start = time.monotonic()

        for i in range(num_records):
            record = Record.from_dict(
                {"id": str(i), "name": f"User {i}"},
                "perf_test.csv",
                i,
            )
            await dispatcher.queue.put(record)

        await dispatcher.stop()
        elapsed = time.monotonic() - start

        throughput = num_records / elapsed
        print(
            f"\nThroughput: {throughput:.0f} records/sec ({num_records} records in {elapsed:.2f}s)"
        )
        assert throughput > 100, f"Throughput {throughput:.0f} rec/s below minimum 100 rec/s"
