"""Unit tests for retry handler."""

import pytest

from durable_engine.config.models import RetryConfig
from durable_engine.resilience.retry import RetryHandler
from durable_engine.sinks.base import SinkResult


class TestRetryHandler:
    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self) -> None:
        config = RetryConfig(max_retries=3, base_delay_ms=10)
        handler = RetryHandler(config, sink_name="test")
        call_count = 0

        async def operation() -> SinkResult:
            nonlocal call_count
            call_count += 1
            return SinkResult.ok()

        result, attempts = await handler.execute_with_retry(operation)
        assert result.success
        assert attempts == 1
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_transient_error(self) -> None:
        config = RetryConfig(max_retries=3, base_delay_ms=10, max_delay_ms=50)
        handler = RetryHandler(config, sink_name="test")
        call_count = 0

        async def operation() -> SinkResult:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return SinkResult.fail("transient", retryable=True)
            return SinkResult.ok()

        result, attempts = await handler.execute_with_retry(operation)
        assert result.success
        assert attempts == 3

    @pytest.mark.asyncio
    async def test_no_retry_on_permanent_error(self) -> None:
        config = RetryConfig(max_retries=3, base_delay_ms=10)
        handler = RetryHandler(config, sink_name="test")
        call_count = 0

        async def operation() -> SinkResult:
            nonlocal call_count
            call_count += 1
            return SinkResult.fail("permanent", retryable=False)

        result, attempts = await handler.execute_with_retry(operation)
        assert not result.success
        assert attempts == 1

    @pytest.mark.asyncio
    async def test_exhausts_all_retries(self) -> None:
        config = RetryConfig(max_retries=2, base_delay_ms=10, max_delay_ms=20)
        handler = RetryHandler(config, sink_name="test")

        async def operation() -> SinkResult:
            return SinkResult.fail("always fails", retryable=True)

        result, attempts = await handler.execute_with_retry(operation)
        assert not result.success
        assert attempts == 3  # 1 initial + 2 retries
