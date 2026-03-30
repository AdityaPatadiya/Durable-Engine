"""Kafka consumer source — reads real-time records from a Kafka topic."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import structlog

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer

logger = structlog.get_logger()


class KafkaSource(RecordSource):
    """Consumes records from a Kafka topic in real-time.

    Requires: pip install -e ".[kafka]"

    The engine runs continuously, consuming messages as they arrive.
    Supports JSON, JSONL, or raw bytes from the topic.
    """

    def __init__(
        self,
        brokers: str,
        topic: str,
        group_id: str = "durable-engine",
        data_format: str = "json",
        auto_offset_reset: str = "earliest",
        auth_username: str = "",
        auth_password: str = "",
        tls_enabled: bool = False,
        tls_ca_path: str = "",
    ) -> None:
        super().__init__()
        self._brokers = brokers
        self._topic = topic
        self._group_id = group_id
        self._data_format = data_format
        self._auto_offset_reset = auto_offset_reset
        self._auth_username = auth_username
        self._auth_password = auth_password
        self._tls_enabled = tls_enabled
        self._tls_ca_path = tls_ca_path
        self._consumer: AIOKafkaConsumer | None = None

    @property
    def source_name(self) -> str:
        return f"kafka://{self._brokers}/{self._topic}"

    @property
    def is_streaming(self) -> bool:
        return True

    async def setup(self) -> None:
        from aiokafka import AIOKafkaConsumer
        from aiokafka.helpers import create_ssl_context

        ssl_context = None
        security_protocol = "PLAINTEXT"
        sasl_mechanism = None
        sasl_plain_username = None
        sasl_plain_password = None

        if self._tls_enabled:
            ssl_context = create_ssl_context(cafile=self._tls_ca_path or None)
            security_protocol = "SSL"

        if self._auth_username:
            sasl_mechanism = "PLAIN"
            sasl_plain_username = self._auth_username
            sasl_plain_password = self._auth_password
            security_protocol = "SASL_SSL" if self._tls_enabled else "SASL_PLAINTEXT"

        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._brokers,
            group_id=self._group_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            security_protocol=security_protocol,
            ssl_context=ssl_context,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            value_deserializer=None,
        )
        await self._consumer.start()
        logger.info(
            "kafka_source_connected",
            brokers=self._brokers,
            topic=self._topic,
            group_id=self._group_id,
        )

    def _parse_message(self, value: bytes, offset: int, partition: int) -> Record:
        """Parse a Kafka message value into a Record."""
        if self._data_format == "json":
            try:
                data = json.loads(value)
                if not isinstance(data, dict):
                    data = {"value": data}
            except json.JSONDecodeError:
                data = {"raw": value.decode("utf-8", errors="replace")}
        else:
            data = {"raw": value.decode("utf-8", errors="replace")}

        return Record.from_dict(
            data=data,
            source_file=f"kafka://{self._topic}/p{partition}",
            line_number=offset,
        )

    async def read_records_async(self) -> AsyncIterator[Record]:
        """Consume messages from Kafka indefinitely."""
        assert self._consumer is not None
        async for msg in self._consumer:
            record = self._parse_message(msg.value, msg.offset, msg.partition)
            self._records_read += 1
            yield record

    async def teardown(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("kafka_source_disconnected", topic=self._topic)
