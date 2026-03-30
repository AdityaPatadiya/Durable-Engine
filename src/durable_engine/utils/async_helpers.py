"""Async utilities and graceful shutdown helpers."""

import asyncio
import signal
from collections.abc import Coroutine
from typing import Any

import structlog

logger = structlog.get_logger()


async def gather_with_concurrency(
    limit: int,
    *coros: Coroutine[Any, Any, Any],
) -> list[Any]:
    """Run coroutines with a concurrency limit."""
    semaphore = asyncio.Semaphore(limit)

    async def _limited(coro: Coroutine[Any, Any, Any]) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*[_limited(c) for c in coros])


async def run_with_timeout(
    coro: Coroutine[Any, Any, Any],
    deadline: float,
    description: str = "operation",
) -> Any:
    """Run a coroutine with a timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=deadline)
    except TimeoutError:
        logger.warning("operation_timed_out", operation=description, timeout_s=deadline)
        raise


class GracefulShutdown:
    """Helper to manage graceful shutdown on SIGINT/SIGTERM."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    @property
    def is_requested(self) -> bool:
        return self._event.is_set()

    @property
    def event(self) -> asyncio.Event:
        return self._event

    def request(self) -> None:
        self._event.set()

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request)
