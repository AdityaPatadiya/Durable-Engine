"""Retry logic with exponential backoff and jitter."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

from durable_engine.config.models import RetryConfig
from durable_engine.sinks.base import SinkResult

logger = structlog.get_logger()

T = TypeVar("T")


class RetryHandler:
    """Handles retries with exponential backoff and jitter."""

    def __init__(self, config: RetryConfig, sink_name: str = "") -> None:
        self._max_retries = config.max_retries
        self._base_delay = config.base_delay_ms / 1000.0
        self._max_delay = config.max_delay_ms / 1000.0
        self._multiplier = config.backoff_multiplier
        self._sink_name = sink_name

    def _compute_delay(self, attempt: int) -> float:
        """Compute delay with exponential backoff + full jitter."""
        delay = self._base_delay * (self._multiplier**attempt)
        delay = min(delay, self._max_delay)
        jitter = random.uniform(0, delay)
        return jitter

    async def execute_with_retry(
        self,
        operation: Callable[[], Awaitable[SinkResult]],
        record_id: str = "",
    ) -> tuple[SinkResult, int]:
        """Execute an operation with retry logic.

        Returns:
            Tuple of (final result, number of attempts made).
        """
        last_result = SinkResult.fail("No attempts made")

        for attempt in range(self._max_retries + 1):
            result = await operation()

            if result.success:
                if attempt > 0:
                    logger.debug(
                        "retry_succeeded",
                        sink=self._sink_name,
                        record_id=record_id,
                        attempt=attempt + 1,
                    )
                return result, attempt + 1

            last_result = result

            if not result.retryable:
                logger.warning(
                    "non_retryable_error",
                    sink=self._sink_name,
                    record_id=record_id,
                    error=result.error,
                )
                return result, attempt + 1

            if attempt < self._max_retries:
                delay = self._compute_delay(attempt)
                logger.debug(
                    "retry_scheduled",
                    sink=self._sink_name,
                    record_id=record_id,
                    attempt=attempt + 1,
                    delay_s=round(delay, 3),
                    error=result.error,
                )
                await asyncio.sleep(delay)

        logger.warning(
            "max_retries_exhausted",
            sink=self._sink_name,
            record_id=record_id,
            attempts=self._max_retries + 1,
            error=last_result.error,
        )
        return last_result, self._max_retries + 1
