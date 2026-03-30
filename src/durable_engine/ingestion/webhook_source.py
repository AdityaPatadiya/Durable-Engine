"""HTTP Webhook source — receives real-time records via POST requests."""

import asyncio
import json
from collections.abc import AsyncIterator

import structlog
from aiohttp import web

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record

logger = structlog.get_logger()


class WebhookSource(RecordSource):
    """Receives records via HTTP POST webhook endpoint.

    Starts an HTTP server that accepts POST requests with JSON payloads.
    Each request body becomes one or more records fed into the pipeline.

    Use case: Receive events from GitHub webhooks, Stripe, Slack,
    or any system that can send HTTP POST notifications.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8082,
        path: str = "/ingest",
        auth_token: str = "",
        max_queue_size: int = 50000,
    ) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._path = path
        self._auth_token = auth_token
        self._queue: asyncio.Queue[Record | None] = asyncio.Queue(maxsize=max_queue_size)
        self._runner: web.AppRunner | None = None
        self._total_requests = 0

    @property
    def source_name(self) -> str:
        return f"webhook://:{self._port}{self._path}"

    @property
    def is_streaming(self) -> bool:
        return True

    async def _handle_ingest(self, request: web.Request) -> web.Response:
        """Handle incoming POST requests."""
        # Auth check
        if self._auth_token:
            auth_header = request.headers.get("Authorization", "")
            if auth_header != f"Bearer {self._auth_token}":
                return web.json_response({"error": "Unauthorized"}, status=401)

        content_type = request.content_type
        self._total_requests += 1

        try:
            if content_type == "application/json":
                body = await request.json()

                # Support single object or array of objects
                items = body if isinstance(body, list) else [body]

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    record = Record.from_dict(
                        data=item,
                        source_file=f"webhook:{self._path}",
                        line_number=self._total_requests,
                    )
                    await self._queue.put(record)
                    self._records_read += 1

                return web.json_response(
                    {"status": "accepted", "records": len(items)},
                    status=202,
                )

            elif content_type == "text/plain":
                text = await request.text()
                # Try to parse each line as JSON
                count = 0
                for line in text.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if not isinstance(data, dict):
                            data = {"value": data}
                    except json.JSONDecodeError:
                        data = {"raw": line}

                    record = Record.from_dict(
                        data=data,
                        source_file=f"webhook:{self._path}",
                        line_number=self._total_requests,
                    )
                    await self._queue.put(record)
                    self._records_read += 1
                    count += 1

                return web.json_response({"status": "accepted", "records": count}, status=202)

            else:
                return web.json_response(
                    {"error": f"Unsupported content type: {content_type}"},
                    status=415,
                )

        except Exception as e:
            logger.error("webhook_parse_error", error=str(e))
            return web.json_response({"error": str(e)}, status=400)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "healthy",
                "records_received": self._records_read,
                "queue_size": self._queue.qsize(),
            }
        )

    async def setup(self) -> None:
        app = web.Application()
        app.router.add_post(self._path, self._handle_ingest)
        app.router.add_get("/health", self._handle_health)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

        logger.info(
            "webhook_source_started",
            host=self._host,
            port=self._port,
            path=self._path,
        )

    async def read_records_async(self) -> AsyncIterator[Record]:
        """Yield records received from webhook POSTs."""
        while True:
            record = await self._queue.get()
            if record is None:
                break
            yield record

    async def teardown(self) -> None:
        await self._queue.put(None)
        if self._runner:
            await self._runner.cleanup()
        logger.info("webhook_source_stopped")
