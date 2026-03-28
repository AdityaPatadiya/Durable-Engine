"""Token bucket rate limiter with adaptive rate adjustment."""

import asyncio
import time


class TokenBucketRateLimiter:
    """Token bucket rate limiter with burst support and adaptive rate adjustment.

    Tokens are added at a constant rate. Each request consumes one token.
    If no tokens are available, the caller waits until one is replenished.
    """

    def __init__(self, rate: float, burst_size: int) -> None:
        self._rate = rate
        self._burst_size = burst_size
        self._tokens = float(burst_size)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._base_rate = rate
        self._min_rate = rate * 0.1
        self._max_rate = rate * 2.0

    @property
    def current_rate(self) -> float:
        return self._rate

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst_size, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self) -> None:
        """Acquire a token, waiting if necessary."""
        async with self._lock:
            self._refill()
            while self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait_time)
                self._refill()
            self._tokens -= 1.0

    async def try_acquire(self) -> bool:
        """Try to acquire a token without waiting. Returns True if successful."""
        async with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def adapt_on_failure(self, factor: float = 0.8) -> None:
        """Reduce rate on failure (adaptive throttling)."""
        self._rate = max(self._min_rate, self._rate * factor)

    def adapt_on_success(self, factor: float = 1.05) -> None:
        """Increase rate on success (recovery)."""
        self._rate = min(self._max_rate, self._rate * factor)

    def reset_rate(self) -> None:
        """Reset to the base configured rate."""
        self._rate = self._base_rate
