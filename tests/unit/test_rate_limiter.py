"""Unit tests for token bucket rate limiter."""

import asyncio
import time

import pytest

from durable_engine.resilience.rate_limiter import TokenBucketRateLimiter


class TestTokenBucketRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_within_burst(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        for _ in range(10):
            await limiter.acquire()

    @pytest.mark.asyncio
    async def test_try_acquire_returns_false_when_empty(self) -> None:
        limiter = TokenBucketRateLimiter(rate=10, burst_size=2)
        assert await limiter.try_acquire()
        assert await limiter.try_acquire()
        assert not await limiter.try_acquire()

    @pytest.mark.asyncio
    async def test_rate_limiting_delays(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=1)
        await limiter.acquire()  # Use the burst token
        start = time.monotonic()
        await limiter.acquire()  # Should wait ~10ms
        elapsed = time.monotonic() - start
        assert elapsed >= 0.005  # At least some delay

    def test_adapt_on_failure_reduces_rate(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        limiter.adapt_on_failure(factor=0.5)
        assert limiter.current_rate == 50.0

    def test_adapt_on_success_increases_rate(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        limiter.adapt_on_success(factor=1.5)
        assert limiter.current_rate == 150.0

    def test_rate_does_not_exceed_max(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        for _ in range(100):
            limiter.adapt_on_success(factor=1.5)
        assert limiter.current_rate <= 200.0  # max_rate = rate * 2.0

    def test_rate_does_not_go_below_min(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        for _ in range(100):
            limiter.adapt_on_failure(factor=0.5)
        assert limiter.current_rate >= 10.0  # min_rate = rate * 0.1

    def test_reset_rate(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100, burst_size=10)
        limiter.adapt_on_failure()
        limiter.reset_rate()
        assert limiter.current_rate == 100.0
