"""WebSocket source — receives real-time records from a WebSocket stream."""

import asyncio
import json
from collections.abc import AsyncIterator

import structlog

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record

logger = structlog.get_logger()


class WebSocketSource(RecordSource):
    """Connects to a WebSocket endpoint and consumes messages as records.

    Use case: Consume real-time events from WebSocket APIs —
    stock prices, IoT sensors, chat messages, live notifications, etc.
    """

    def __init__(
        self,
        url: str,
        auth_token: str = "",
        headers: dict[str, str] | None = None,
        reconnect_delay: float = 5.0,
        ping_interval: float = 30.0,
        subscribe_message: str = "",
    ) -> None:
        super().__init__()
        self._url = url
        self._auth_token = auth_token
        self._headers = headers or {}
        self._reconnect_delay = reconnect_delay
        self._ping_interval = ping_interval
        self._subscribe_message = subscribe_message

    @property
    def source_name(self) -> str:
        return f"ws://{self._url}"

    @property
    def is_streaming(self) -> bool:
        return True

    async def read_records_async(self) -> AsyncIterator[Record]:
        """Connect to WebSocket and yield records from messages."""
        import aiohttp

        extra_headers = dict(self._headers)
        if self._auth_token:
            extra_headers["Authorization"] = f"Bearer {self._auth_token}"

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        self._url,
                        headers=extra_headers,
                        heartbeat=self._ping_interval,
                    ) as ws:
                        logger.info("websocket_connected", url=self._url)

                        # Send subscription message if configured
                        if self._subscribe_message:
                            await ws.send_str(self._subscribe_message)
                            logger.debug("websocket_subscribed", message=self._subscribe_message)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    if not isinstance(data, dict):
                                        data = {"value": data}
                                except json.JSONDecodeError:
                                    data = {"raw": msg.data}

                                record = Record.from_dict(
                                    data=data,
                                    source_file=f"websocket://{self._url}",
                                    line_number=self._records_read + 1,
                                )
                                self._records_read += 1
                                yield record

                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                data = {"raw_hex": msg.data.hex(), "size": len(msg.data)}
                                record = Record.from_dict(
                                    data=data,
                                    source_file=f"websocket://{self._url}",
                                    line_number=self._records_read + 1,
                                )
                                self._records_read += 1
                                yield record

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning("websocket_closed", url=self._url)
                                break

            except Exception as e:
                logger.error("websocket_error", url=self._url, error=str(e))

            logger.info("websocket_reconnecting", delay=self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
