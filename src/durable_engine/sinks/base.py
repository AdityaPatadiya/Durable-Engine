"""Abstract base sink with lifecycle hooks (Template Method pattern)."""

from abc import ABC, abstractmethod

import structlog

from durable_engine.config.models import SinkConfig

logger = structlog.get_logger()


class SinkResult:
    """Result of sending a record to a sink."""

    __slots__ = ("success", "error", "retryable")

    def __init__(self, success: bool, error: str | None = None, retryable: bool = False) -> None:
        self.success = success
        self.error = error
        self.retryable = retryable

    @classmethod
    def ok(cls) -> "SinkResult":
        return cls(success=True)

    @classmethod
    def fail(cls, error: str, retryable: bool = True) -> "SinkResult":
        return cls(success=False, error=error, retryable=retryable)


class BaseSink(ABC):
    """Abstract base class for all sinks. Uses Template Method for lifecycle."""

    def __init__(self, name: str, config: SinkConfig) -> None:
        self.name = name
        self.config = config
        self._connected = False
        self._records_sent = 0
        self._records_failed = 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def records_sent(self) -> int:
        return self._records_sent

    @property
    def records_failed(self) -> int:
        return self._records_failed

    async def connect(self) -> None:
        """Establish connection to the sink endpoint."""
        logger.info("sink_connecting", sink=self.name, endpoint=self.config.endpoint)
        await self._do_connect()
        self._connected = True
        logger.info("sink_connected", sink=self.name)

    async def send(self, data: bytes) -> SinkResult:
        """Send a single transformed record to the sink."""
        if not self._connected:
            return SinkResult.fail("Sink not connected", retryable=True)

        result = await self._do_send(data)
        if result.success:
            self._records_sent += 1
        else:
            self._records_failed += 1
        return result

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Send a batch of records. Override for batch-optimized implementations."""
        results = []
        for data in batch:
            results.append(await self.send(data))
        return results

    async def flush(self) -> None:
        """Flush any buffered data."""
        await self._do_flush()

    async def close(self) -> None:
        """Close the sink connection."""
        logger.info("sink_closing", sink=self.name, sent=self._records_sent)
        await self._do_flush()
        await self._do_close()
        self._connected = False
        logger.info("sink_closed", sink=self.name)

    @abstractmethod
    async def _do_connect(self) -> None: ...

    @abstractmethod
    async def _do_send(self, data: bytes) -> SinkResult: ...

    async def _do_flush(self) -> None:
        pass

    async def _do_close(self) -> None:
        pass

    async def __aenter__(self) -> "BaseSink":
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
