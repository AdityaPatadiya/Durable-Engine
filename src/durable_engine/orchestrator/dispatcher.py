"""Dispatches transformed records to sink workers with resilience."""

import asyncio

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.ingestion.record import Record
from durable_engine.ingestion.validator import RecordValidator
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.resilience.backpressure import BackpressureQueue
from durable_engine.resilience.circuit_breaker import CircuitBreaker, CircuitOpenError
from durable_engine.resilience.dlq import DeadLetterQueue
from durable_engine.resilience.rate_limiter import TokenBucketRateLimiter
from durable_engine.resilience.retry import RetryHandler
from durable_engine.sinks.base import BaseSink, SinkResult
from durable_engine.transformation.base import Transformer

logger = structlog.get_logger()


class SinkDispatcher:
    """Manages the dispatch pipeline for a single sink:
    queue → transform → rate-limit → retry → circuit-break → send.
    """

    def __init__(
        self,
        sink_name: str,
        sink: BaseSink,
        transformer: Transformer,
        config: SinkConfig,
        metrics: MetricsCollector,
        dlq: DeadLetterQueue,
        queue_size: int,
    ) -> None:
        self.sink_name = sink_name
        self._sink = sink
        self._transformer = transformer
        self._config = config
        self._metrics = metrics
        self._dlq = dlq

        self._queue = BackpressureQueue(max_size=queue_size, name=sink_name)
        self._rate_limiter = TokenBucketRateLimiter(
            rate=config.rate_limit.requests_per_second,
            burst_size=config.rate_limit.burst_size,
        )
        self._retry_handler = RetryHandler(config.retry, sink_name=sink_name)
        self._circuit_breaker = CircuitBreaker(config.circuit_breaker, sink_name=sink_name)
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._validator = RecordValidator()
        self._workers: list[asyncio.Task] = []

    @property
    def queue(self) -> BackpressureQueue:
        return self._queue

    async def start_workers(self, num_workers: int) -> None:
        """Start consumer worker tasks."""
        await self._sink.connect()
        for i in range(num_workers):
            task = asyncio.create_task(
                self._worker_loop(worker_id=i),
                name=f"worker-{self.sink_name}-{i}",
            )
            self._workers.append(task)
        logger.info("dispatcher_started", sink=self.sink_name, workers=num_workers)

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker loop: consume from queue, transform, send with resilience."""
        while True:
            batch = await self._queue.get_batch(
                batch_size=self._config.batch_size,
                timeout=0.5,
            )

            if not batch:
                if self._queue._closed and self._queue.is_empty:
                    break
                continue

            for record in batch:
                await self._process_record(record)

    async def _process_record(self, record: Record) -> None:
        """Process a single record through the full pipeline."""
        # Validate record before processing
        result = self._validator.validate(record)
        if not result.valid:
            self._metrics.record_failure(self.sink_name)
            self._dlq.write(
                record=record,
                sink_name=self.sink_name,
                error=f"Validation failed: {'; '.join(result.errors)}",
                attempts=0,
            )
            return

        record = result.sanitized_record or record

        async with self._semaphore:
            try:
                await self._circuit_breaker.check()
            except CircuitOpenError:
                self._metrics.record_failure(self.sink_name)
                self._dlq.write(
                    record=record,
                    sink_name=self.sink_name,
                    error="Circuit breaker open",
                    attempts=0,
                )
                return

            try:
                data = self._transformer.transform(record)
            except Exception as e:
                self._metrics.record_failure(self.sink_name)
                self._dlq.write(
                    record=record,
                    sink_name=self.sink_name,
                    error=f"Transformation error: {e}",
                    attempts=0,
                )
                return

            await self._rate_limiter.acquire()

            async def _send() -> SinkResult:
                return await self._sink.send(data)

            result, attempts = await self._retry_handler.execute_with_retry(
                operation=_send,
                record_id=record.record_id,
            )

            if result.success:
                self._metrics.record_success(self.sink_name)
                await self._circuit_breaker.record_success()
                self._rate_limiter.adapt_on_success()
            else:
                self._metrics.record_failure(self.sink_name)
                await self._circuit_breaker.record_failure()
                self._rate_limiter.adapt_on_failure()
                self._dlq.write(
                    record=record,
                    sink_name=self.sink_name,
                    error=result.error or "Unknown error",
                    attempts=attempts,
                )

    async def stop(self) -> None:
        """Signal workers to stop and wait for completion."""
        await self._queue.close()

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

        await self._sink.flush()
        await self._sink.close()
        logger.info("dispatcher_stopped", sink=self.sink_name)
