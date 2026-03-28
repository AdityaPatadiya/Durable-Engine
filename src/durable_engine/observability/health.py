"""Health check HTTP endpoint."""

import asyncio
import json

from aiohttp import web

import structlog

from durable_engine.config.models import HealthConfig
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.sinks.base import BaseSink

logger = structlog.get_logger()


class HealthCheckServer:
    """Lightweight HTTP health check endpoint."""

    def __init__(
        self,
        config: HealthConfig,
        metrics: MetricsCollector,
        sinks: dict[str, BaseSink],
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._sinks = sinks
        self._app = web.Application()
        self._app.router.add_get(config.path, self._health_handler)
        self._runner: web.AppRunner | None = None

    async def _health_handler(self, request: web.Request) -> web.Response:
        sink_status = {}
        for name, sink in self._sinks.items():
            m = self._metrics.get_sink_metrics(name)
            sink_status[name] = {
                "connected": sink.is_connected,
                "success": m.success if m else 0,
                "failure": m.failure if m else 0,
            }

        health_data = {
            "status": "healthy",
            "uptime_seconds": round(self._metrics.elapsed_seconds, 1),
            "total_processed": self._metrics.get_total_processed(),
            "throughput_rps": round(self._metrics.get_throughput(), 1),
            "sinks": sink_status,
        }

        return web.json_response(health_data)

    async def start(self) -> None:
        if not self._config.enabled:
            return
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._config.port)
        await site.start()
        logger.info("health_server_started", port=self._config.port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
