"""Mock REST API sink simulating HTTP/2 POST requests."""

import asyncio
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()


class RestApiSink(BaseSink):
    """Simulates sending records as HTTP/2 POST requests to a REST API."""

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation

    async def _do_connect(self) -> None:
        await asyncio.sleep(0.01)
        logger.debug("rest_sink_connected", endpoint=self.config.endpoint)

    async def _do_send(self, data: bytes) -> SinkResult:
        latency = max(
            0.001,
            (self._sim.latency_ms + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms))
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                status_code = random.choice([429, 500, 502, 503])
                return SinkResult.fail(
                    f"HTTP {status_code}: Transient error",
                    retryable=True,
                )
            else:
                status_code = random.choice([400, 401, 403, 422])
                return SinkResult.fail(
                    f"HTTP {status_code}: Permanent error",
                    retryable=False,
                )

        return SinkResult.ok()

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Simulate batch POST — sends concurrently within the batch."""
        tasks = [self.send(data) for data in batch]
        return list(await asyncio.gather(*tasks))
