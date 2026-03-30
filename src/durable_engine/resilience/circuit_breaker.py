"""Circuit breaker pattern implementation."""

import asyncio
import time
from enum import Enum

import structlog

from durable_engine.config.models import CircuitBreakerConfig

logger = structlog.get_logger()


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is open and rejecting calls."""

    def __init__(self, sink_name: str, recovery_time: float) -> None:
        self.sink_name = sink_name
        self.recovery_time = recovery_time
        super().__init__(
            f"Circuit breaker OPEN for sink '{sink_name}'. Recovery in {recovery_time:.1f}s"
        )


class CircuitBreaker:
    """Per-sink circuit breaker with closed/open/half-open states."""

    def __init__(self, config: CircuitBreakerConfig, sink_name: str = "") -> None:
        self._failure_threshold = config.failure_threshold
        self._recovery_timeout = config.recovery_timeout_seconds
        self._half_open_max_calls = config.half_open_max_calls
        self._sink_name = sink_name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._last_failure_time: float = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    async def check(self) -> None:
        """Check if the circuit allows a request. Raises CircuitOpenError if not."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return

            if self._state == CircuitState.OPEN:
                elapsed = time.monotonic() - self._last_failure_time
                if elapsed >= self._recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info("circuit_half_open", sink=self._sink_name)
                    return
                else:
                    remaining = self._recovery_timeout - elapsed
                    raise CircuitOpenError(self._sink_name, remaining)

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self._half_open_max_calls:
                    raise CircuitOpenError(self._sink_name, 0)
                self._half_open_calls += 1

    async def record_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info("circuit_closed", sink=self._sink_name)
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    async def record_failure(self) -> None:
        """Record a failed call."""
        async with self._lock:
            self._failure_count += 1

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._last_failure_time = time.monotonic()
                logger.warning("circuit_reopened", sink=self._sink_name)
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._state = CircuitState.OPEN
                    self._last_failure_time = time.monotonic()
                    logger.warning(
                        "circuit_opened",
                        sink=self._sink_name,
                        failures=self._failure_count,
                    )

    async def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
