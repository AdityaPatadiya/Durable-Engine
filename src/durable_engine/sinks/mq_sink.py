"""Mock Message Queue sink simulating Kafka/RabbitMQ publishing."""

import asyncio
import hashlib
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()


class MessageQueueSink(BaseSink):
    """Simulates publishing records to a Kafka/RabbitMQ topic."""

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._topic = config.topic
        self._partitions = config.partitions
        self._pending_acks = 0

    def _compute_partition(self, data: bytes) -> int:
        """Consistent partition assignment based on data hash."""
        hash_val = int(hashlib.md5(data).hexdigest(), 16)
        return hash_val % self._partitions

    async def _do_connect(self) -> None:
        await asyncio.sleep(0.01)
        logger.debug(
            "mq_sink_connected",
            topic=self._topic,
            partitions=self._partitions,
        )

    async def _do_send(self, data: bytes) -> SinkResult:
        partition = self._compute_partition(data)

        latency = max(
            0.001,
            (self._sim.latency_ms + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms))
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                error = random.choice([
                    "LEADER_NOT_AVAILABLE",
                    "REQUEST_TIMED_OUT",
                    "NOT_ENOUGH_REPLICAS",
                ])
                return SinkResult.fail(f"MQ {error} (partition={partition})", retryable=True)
            else:
                error = random.choice([
                    "MESSAGE_TOO_LARGE",
                    "TOPIC_AUTHORIZATION_FAILED",
                ])
                return SinkResult.fail(f"MQ {error} (partition={partition})", retryable=False)

        self._pending_acks += 1
        logger.debug("mq_message_sent", topic=self._topic, partition=partition)
        return SinkResult.ok()

    async def _do_flush(self) -> None:
        if self._pending_acks > 0:
            await asyncio.sleep(0.005)
            logger.debug("mq_acks_flushed", count=self._pending_acks)
            self._pending_acks = 0

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Batch produce — send all, then await acks."""
        tasks = [self.send(data) for data in batch]
        results = list(await asyncio.gather(*tasks))
        await self._do_flush()
        return results
