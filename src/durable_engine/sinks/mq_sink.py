"""Message Queue sink — mock simulation and live Kafka/RabbitMQ clients."""

import asyncio
import hashlib
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()


class MessageQueueSink(BaseSink):
    """Publishes records to a message queue (Kafka or RabbitMQ).

    Supports two modes:
    - mock: Simulates message publishing with configurable latency/errors
    - live: Publishes to real Kafka (aiokafka) or RabbitMQ (aio-pika)
    """

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._mode = config.mode
        self._topic = config.topic
        self._partitions = config.partitions
        self._mq_type = config.mq_type
        self._pending_acks = 0
        # Live mode state
        self._kafka_producer = None
        self._rabbitmq_connection = None
        self._rabbitmq_channel = None
        self._rabbitmq_exchange = None

    def _compute_partition(self, data: bytes) -> int:
        """Consistent partition assignment based on data hash."""
        hash_val = int(hashlib.md5(data).hexdigest(), 16)
        return hash_val % self._partitions

    async def _do_connect(self) -> None:
        if self._mode == "live":
            if self._mq_type == "kafka":
                await self._connect_kafka()
            elif self._mq_type == "rabbitmq":
                await self._connect_rabbitmq()
            else:
                raise ValueError(f"Unsupported MQ type: {self._mq_type}")
        else:
            await asyncio.sleep(0.01)
            logger.debug("mq_sink_mock_connected", topic=self._topic)

    async def _connect_kafka(self) -> None:
        """Connect to a real Kafka cluster."""
        from aiokafka import AIOKafkaProducer
        from aiokafka.helpers import create_ssl_context

        # Build SSL context if TLS is enabled
        ssl_context = None
        if self.config.tls.enabled:
            ssl_context = create_ssl_context(
                cafile=self.config.tls.ca_path or None,
                certfile=self.config.tls.cert_path or None,
                keyfile=self.config.tls.key_path or None,
            )

        # Build SASL config for auth
        sasl_mechanism = None
        sasl_plain_username = None
        sasl_plain_password = None
        security_protocol = "PLAINTEXT"

        if self.config.auth.type == "basic":
            sasl_mechanism = "PLAIN"
            sasl_plain_username = self.config.auth.username
            sasl_plain_password = self.config.auth.password
            security_protocol = "SASL_SSL" if self.config.tls.enabled else "SASL_PLAINTEXT"
        elif self.config.tls.enabled:
            security_protocol = "SSL"

        self._kafka_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.endpoint,
            security_protocol=security_protocol,
            ssl_context=ssl_context,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            acks="all",
            max_batch_size=self.config.batch_size * 1024,
            linger_ms=10,
            request_timeout_ms=self.config.timeout_seconds * 1000,
            retry_backoff_ms=100,
        )
        await self._kafka_producer.start()
        logger.info("kafka_producer_connected", brokers=self.config.endpoint, topic=self._topic)

    async def _connect_rabbitmq(self) -> None:
        """Connect to a real RabbitMQ broker."""
        import aio_pika

        # Build connection URL
        url = self.config.endpoint
        if not url.startswith("amqp"):
            scheme = "amqps" if self.config.tls.enabled else "amqp"
            user = self.config.auth.username or "guest"
            passwd = self.config.auth.password or "guest"
            url = f"{scheme}://{user}:{passwd}@{self.config.endpoint}/"

        self._rabbitmq_connection = await aio_pika.connect_robust(url)
        self._rabbitmq_channel = await self._rabbitmq_connection.channel()
        await self._rabbitmq_channel.set_qos(prefetch_count=self.config.concurrency)

        exchange_name = self.config.rabbitmq_exchange or "durable_engine"
        self._rabbitmq_exchange = await self._rabbitmq_channel.declare_exchange(
            exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        logger.info(
            "rabbitmq_connected",
            endpoint=self.config.endpoint,
            exchange=exchange_name,
        )

    async def _do_send(self, data: bytes) -> SinkResult:
        if self._mode == "live":
            if self._mq_type == "kafka":
                return await self._send_kafka(data)
            else:
                return await self._send_rabbitmq(data)
        return await self._send_mock(data)

    async def _send_kafka(self, data: bytes) -> SinkResult:
        """Send a record to a real Kafka topic."""
        from aiokafka.errors import KafkaError, KafkaTimeoutError

        try:
            partition = self._compute_partition(data) if self._partitions > 1 else None
            await self._kafka_producer.send_and_wait(
                topic=self._topic,
                value=data,
                partition=partition,
            )
            return SinkResult.ok()

        except KafkaTimeoutError as e:
            return SinkResult.fail(f"Kafka timeout: {e}", retryable=True)
        except KafkaError as e:
            retryable = e.retriable if hasattr(e, "retriable") else True
            return SinkResult.fail(f"Kafka error: {e}", retryable=retryable)
        except Exception as e:
            return SinkResult.fail(f"Kafka unexpected error: {e}", retryable=True)

    async def _send_rabbitmq(self, data: bytes) -> SinkResult:
        """Send a record to a real RabbitMQ exchange."""
        import aio_pika

        try:
            routing_key = self.config.rabbitmq_routing_key or self._topic
            message = aio_pika.Message(
                body=data,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/octet-stream",
            )
            await self._rabbitmq_exchange.publish(message, routing_key=routing_key)
            return SinkResult.ok()

        except aio_pika.exceptions.AMQPError as e:
            return SinkResult.fail(f"RabbitMQ error: {e}", retryable=True)
        except Exception as e:
            return SinkResult.fail(f"RabbitMQ unexpected error: {e}", retryable=True)

    async def _send_mock(self, data: bytes) -> SinkResult:
        """Simulate message publishing."""
        partition = self._compute_partition(data)
        latency = max(
            0.001,
            (
                self._sim.latency_ms
                + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms)
            )
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                error = random.choice(
                    ["LEADER_NOT_AVAILABLE", "REQUEST_TIMED_OUT", "NOT_ENOUGH_REPLICAS"]
                )
                return SinkResult.fail(f"MQ {error} (partition={partition})", retryable=True)
            else:
                error = random.choice(["MESSAGE_TOO_LARGE", "TOPIC_AUTHORIZATION_FAILED"])
                return SinkResult.fail(f"MQ {error} (partition={partition})", retryable=False)

        self._pending_acks += 1
        logger.debug("mq_message_sent", topic=self._topic, partition=partition)
        return SinkResult.ok()

    async def _do_flush(self) -> None:
        if self._mode == "live" and self._mq_type == "kafka" and self._kafka_producer:
            await self._kafka_producer.flush()
        elif self._mode == "mock" and self._pending_acks > 0:
            await asyncio.sleep(0.005)
            logger.debug("mq_acks_flushed", count=self._pending_acks)
            self._pending_acks = 0

    async def _do_close(self) -> None:
        if self._kafka_producer is not None:
            await self._kafka_producer.stop()
            self._kafka_producer = None
            logger.debug("kafka_producer_closed", sink=self.name)

        if self._rabbitmq_connection is not None:
            await self._rabbitmq_connection.close()
            self._rabbitmq_connection = None
            self._rabbitmq_channel = None
            self._rabbitmq_exchange = None
            logger.debug("rabbitmq_connection_closed", sink=self.name)

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Batch produce — send all concurrently, then flush."""
        tasks = [self.send(data) for data in batch]
        results = list(await asyncio.gather(*tasks))
        await self._do_flush()
        return results
