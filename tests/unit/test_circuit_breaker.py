"""Unit tests for circuit breaker."""

import pytest

from durable_engine.config.models import CircuitBreakerConfig
from durable_engine.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


class TestCircuitBreaker:
    @pytest.mark.asyncio
    async def test_starts_closed(self) -> None:
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config, sink_name="test")
        assert cb.state == CircuitState.CLOSED
        await cb.check()  # Should not raise

    @pytest.mark.asyncio
    async def test_opens_after_threshold(self) -> None:
        config = CircuitBreakerConfig(failure_threshold=3, recovery_timeout_seconds=10)
        cb = CircuitBreaker(config, sink_name="test")

        for _ in range(3):
            await cb.record_failure()

        assert cb.state == CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            await cb.check()

    @pytest.mark.asyncio
    async def test_half_open_after_recovery_timeout(self) -> None:
        config = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout_seconds=0,  # Immediate recovery for testing
            half_open_max_calls=2,
        )
        cb = CircuitBreaker(config, sink_name="test")

        await cb.record_failure()
        await cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # With 0 recovery timeout, should transition to half-open
        await cb.check()
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_closes_after_half_open_success(self) -> None:
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout_seconds=0,
            half_open_max_calls=2,
        )
        cb = CircuitBreaker(config, sink_name="test")

        await cb.record_failure()
        await cb.check()  # Transitions to half-open

        await cb.record_success()
        await cb.record_success()
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_reopens_on_half_open_failure(self) -> None:
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout_seconds=0,
            half_open_max_calls=3,
        )
        cb = CircuitBreaker(config, sink_name="test")

        await cb.record_failure()
        await cb.check()
        assert cb.state == CircuitState.HALF_OPEN

        await cb.record_failure()
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_reset(self) -> None:
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config, sink_name="test")

        await cb.record_failure()
        assert cb.state == CircuitState.OPEN

        await cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
