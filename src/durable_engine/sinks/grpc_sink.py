"""Mock gRPC sink simulating unary and streaming calls."""

import asyncio
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()


class GrpcSink(BaseSink):
    """Simulates sending records via gRPC (unary or bidirectional streaming)."""

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._streaming_mode = config.streaming_mode
        self._stream_buffer: list[bytes] = []

    async def _do_connect(self) -> None:
        await asyncio.sleep(0.005)
        logger.debug(
            "grpc_sink_connected",
            endpoint=self.config.endpoint,
            mode=self._streaming_mode,
        )

    async def _do_send(self, data: bytes) -> SinkResult:
        if self._streaming_mode == "bidirectional":
            self._stream_buffer.append(data)
            if len(self._stream_buffer) >= self.config.batch_size:
                return await self._flush_stream()
            return SinkResult.ok()

        # Unary mode
        latency = max(
            0.001,
            (self._sim.latency_ms + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms))
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                code = random.choice(["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"])
                return SinkResult.fail(f"gRPC {code}", retryable=True)
            else:
                code = random.choice(["INVALID_ARGUMENT", "PERMISSION_DENIED", "NOT_FOUND"])
                return SinkResult.fail(f"gRPC {code}", retryable=False)

        return SinkResult.ok()

    async def _flush_stream(self) -> SinkResult:
        """Flush the streaming buffer."""
        count = len(self._stream_buffer)
        latency = max(0.001, (self._sim.latency_ms * 0.5) / 1000.0)
        await asyncio.sleep(latency)

        self._stream_buffer.clear()

        if random.random() < self._sim.error_rate:
            return SinkResult.fail("gRPC UNAVAILABLE: Stream flush failed", retryable=True)

        logger.debug("grpc_stream_flushed", records=count)
        return SinkResult.ok()

    async def _do_flush(self) -> None:
        if self._stream_buffer:
            await self._flush_stream()

    async def _do_close(self) -> None:
        if self._stream_buffer:
            await self._flush_stream()
