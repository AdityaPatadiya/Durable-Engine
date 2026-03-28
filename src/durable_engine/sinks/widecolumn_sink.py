"""Mock Wide-Column DB sink simulating Cassandra/ScyllaDB async UPSERT."""

import asyncio
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()

CONSISTENCY_LATENCY_FACTOR = {
    "ONE": 0.5,
    "QUORUM": 1.0,
    "LOCAL_QUORUM": 0.8,
    "EACH_QUORUM": 1.5,
    "ALL": 2.0,
}


class WideColumnDbSink(BaseSink):
    """Simulates async UPSERT operations to a Cassandra-like wide-column DB."""

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._keyspace = config.keyspace
        self._table = config.table
        self._consistency = config.consistency_level
        self._latency_factor = CONSISTENCY_LATENCY_FACTOR.get(self._consistency, 1.0)
        self._batch_buffer: list[bytes] = []

    async def _do_connect(self) -> None:
        await asyncio.sleep(0.02)
        logger.debug(
            "widecolumn_connected",
            keyspace=self._keyspace,
            table=self._table,
            consistency=self._consistency,
        )

    async def _do_send(self, data: bytes) -> SinkResult:
        latency = max(
            0.001,
            (self._sim.latency_ms * self._latency_factor
             + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms))
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                error = random.choice([
                    "WriteTimeoutException",
                    "UnavailableException",
                    "OverloadedException",
                ])
                return SinkResult.fail(
                    f"CQL {error} (consistency={self._consistency})",
                    retryable=True,
                )
            else:
                error = random.choice([
                    "InvalidQueryException",
                    "UnauthorizedException",
                ])
                return SinkResult.fail(
                    f"CQL {error} (keyspace={self._keyspace})",
                    retryable=False,
                )

        return SinkResult.ok()

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Simulate batch UPSERT (UNLOGGED BATCH in CQL)."""
        latency_per_record = max(0.0005, (self._sim.latency_ms * self._latency_factor * 0.3) / 1000.0)
        batch_latency = latency_per_record * len(batch)
        await asyncio.sleep(min(batch_latency, 1.0))

        results: list[SinkResult] = []
        for _ in batch:
            if random.random() < self._sim.error_rate:
                results.append(SinkResult.fail("CQL WriteTimeoutException", retryable=True))
                self._records_failed += 1
            else:
                results.append(SinkResult.ok())
                self._records_sent += 1
        return results
