"""REST API sink — mock simulation and live HTTP/2 client."""

import asyncio
import random
import ssl

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()

# HTTP status codes that are safe to retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class RestApiSink(BaseSink):
    """Sends records as HTTP POST/PUT requests to a REST API.

    Supports two modes:
    - mock: Simulates HTTP calls with configurable latency/errors
    - live: Makes real HTTP/2 requests using httpx.AsyncClient
    """

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._mode = config.mode
        self._client = None

    async def _do_connect(self) -> None:
        if self._mode == "live":
            import httpx

            # Build SSL context if TLS is configured
            verify: bool | ssl.SSLContext = self.config.tls.verify
            if self.config.tls.enabled and self.config.tls.ca_path:
                ssl_ctx = ssl.create_default_context(cafile=self.config.tls.ca_path)
                if self.config.tls.cert_path and self.config.tls.key_path:
                    ssl_ctx.load_cert_chain(
                        self.config.tls.cert_path,
                        self.config.tls.key_path,
                    )
                verify = ssl_ctx

            # Build default headers
            headers = dict(self.config.headers)
            auth_cfg = self.config.auth
            if auth_cfg.type == "bearer":
                headers["Authorization"] = f"Bearer {auth_cfg.token}"
            elif auth_cfg.type == "api_key":
                headers[auth_cfg.api_key_header] = auth_cfg.api_key

            # Build auth tuple for basic auth
            auth = None
            if auth_cfg.type == "basic":
                auth = httpx.BasicAuth(auth_cfg.username, auth_cfg.password)

            self._client = httpx.AsyncClient(
                http2=True,
                verify=verify,
                headers=headers,
                auth=auth,
                timeout=httpx.Timeout(self.config.timeout_seconds),
                limits=httpx.Limits(
                    max_connections=self.config.concurrency,
                    max_keepalive_connections=self.config.concurrency,
                ),
            )
            logger.info("rest_sink_live_connected", endpoint=self.config.endpoint)
        else:
            await asyncio.sleep(0.01)
            logger.debug("rest_sink_mock_connected", endpoint=self.config.endpoint)

    async def _do_send(self, data: bytes) -> SinkResult:
        if self._mode == "live":
            return await self._send_live(data)
        return await self._send_mock(data)

    async def _send_live(self, data: bytes) -> SinkResult:
        """Send a real HTTP request."""
        import httpx

        url = self.config.endpoint
        if self.config.http_path:
            url = f"{url.rstrip('/')}/{self.config.http_path.lstrip('/')}"

        content_type = self.config.headers.get("Content-Type", "application/json")
        method = self.config.http_method.upper()

        try:
            response = await self._client.request(
                method=method,
                url=url,
                content=data,
                headers={"Content-Type": content_type},
            )

            if response.is_success:
                return SinkResult.ok()

            retryable = response.status_code in _RETRYABLE_STATUS_CODES
            return SinkResult.fail(
                f"HTTP {response.status_code}: {response.reason_phrase}",
                retryable=retryable,
            )

        except httpx.TimeoutException as e:
            return SinkResult.fail(f"HTTP timeout: {e}", retryable=True)
        except httpx.ConnectError as e:
            return SinkResult.fail(f"HTTP connection error: {e}", retryable=True)
        except httpx.HTTPError as e:
            return SinkResult.fail(f"HTTP error: {e}", retryable=True)

    async def _send_mock(self, data: bytes) -> SinkResult:
        """Simulate an HTTP request with configurable latency/errors."""
        latency = max(
            0.001,
            (
                self._sim.latency_ms
                + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms)
            )
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                status_code = random.choice([429, 500, 502, 503])
                return SinkResult.fail(f"HTTP {status_code}: Transient error", retryable=True)
            else:
                status_code = random.choice([400, 401, 403, 422])
                return SinkResult.fail(f"HTTP {status_code}: Permanent error", retryable=False)

        return SinkResult.ok()

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        """Send batch concurrently."""
        tasks = [self.send(data) for data in batch]
        return list(await asyncio.gather(*tasks))

    async def _do_close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.debug("rest_sink_client_closed", sink=self.name)
