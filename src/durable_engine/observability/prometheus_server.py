"""Prometheus metrics HTTP endpoint."""

import structlog

from durable_engine.config.models import MetricsExportConfig

logger = structlog.get_logger()


class PrometheusServer:
    """Starts a Prometheus metrics HTTP server on a configurable port."""

    def __init__(self, config: MetricsExportConfig) -> None:
        self._config = config
        self._server = None

    async def start(self) -> None:
        if not self._config.enabled:
            return

        try:
            from prometheus_client import start_http_server

            start_http_server(self._config.port)
            logger.info(
                "prometheus_server_started",
                port=self._config.port,
                path=self._config.path,
            )
        except ImportError:
            logger.warning("prometheus_client_not_installed_skipping_metrics_server")
        except OSError as e:
            logger.warning("prometheus_server_failed", error=str(e))

    async def stop(self) -> None:
        pass
