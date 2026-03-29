"""gRPC sink — mock simulation and live gRPC client."""

import asyncio
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()


class GrpcSink(BaseSink):
    """Sends records via gRPC unary or streaming calls.

    Supports two modes:
    - mock: Simulates gRPC calls with configurable latency/errors
    - live: Makes real gRPC calls using grpcio async channel
    """

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._mode = config.mode
        self._streaming_mode = config.streaming_mode
        self._stream_buffer: list[bytes] = []
        # Live mode state
        self._channel = None
        self._stub = None

    async def _do_connect(self) -> None:
        if self._mode == "live":
            import grpc

            # Build channel credentials if TLS is enabled
            if self.config.tls.enabled:
                root_certs = None
                private_key = None
                cert_chain = None

                if self.config.tls.ca_path:
                    with open(self.config.tls.ca_path, "rb") as f:
                        root_certs = f.read()
                if self.config.tls.cert_path and self.config.tls.key_path:
                    with open(self.config.tls.key_path, "rb") as f:
                        private_key = f.read()
                    with open(self.config.tls.cert_path, "rb") as f:
                        cert_chain = f.read()

                credentials = grpc.ssl_channel_credentials(
                    root_certificates=root_certs,
                    private_key=private_key,
                    certificate_chain=cert_chain,
                )

                # Add auth token as call credentials if configured
                if self.config.auth.type == "bearer":
                    token = self.config.auth.token
                    call_creds = grpc.access_token_call_credentials(token)
                    credentials = grpc.composite_channel_credentials(credentials, call_creds)

                self._channel = grpc.aio.secure_channel(
                    self.config.endpoint, credentials,
                    options=[
                        ("grpc.max_send_message_length", 50 * 1024 * 1024),
                        ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                        ("grpc.keepalive_time_ms", 30000),
                    ],
                )
            else:
                self._channel = grpc.aio.insecure_channel(
                    self.config.endpoint,
                    options=[
                        ("grpc.max_send_message_length", 50 * 1024 * 1024),
                        ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                    ],
                )

            logger.info(
                "grpc_sink_live_connected",
                endpoint=self.config.endpoint,
                tls=self.config.tls.enabled,
                mode=self._streaming_mode,
            )
        else:
            await asyncio.sleep(0.005)
            logger.debug("grpc_sink_mock_connected", endpoint=self.config.endpoint)

    async def _do_send(self, data: bytes) -> SinkResult:
        if self._mode == "live":
            return await self._send_live(data)
        return await self._send_mock(data)

    async def _send_live(self, data: bytes) -> SinkResult:
        """Send a real gRPC unary call with raw bytes."""
        import grpc

        if self._channel is None:
            return SinkResult.fail("gRPC channel not initialized", retryable=True)

        try:
            # Use generic unary-unary call with raw bytes
            # The service/method path is configurable
            service = self.config.grpc_service or "durable_engine.RecordService"
            method = self.config.grpc_method or "SendRecord"
            full_method = f"/{service}/{method}"

            response = await self._channel.unary_unary(
                full_method,
                request_serializer=lambda x: x,  # data is already serialized protobuf
                response_deserializer=lambda x: x,  # return raw bytes
            )(data, timeout=self.config.timeout_seconds)

            return SinkResult.ok()

        except grpc.aio.AioRpcError as e:
            code = e.code()
            retryable = code in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.ABORTED,
            )
            return SinkResult.fail(
                f"gRPC {code.name}: {e.details()}",
                retryable=retryable,
            )
        except Exception as e:
            return SinkResult.fail(f"gRPC error: {e}", retryable=True)

    async def _send_mock(self, data: bytes) -> SinkResult:
        """Simulate a gRPC call."""
        if self._streaming_mode == "bidirectional":
            self._stream_buffer.append(data)
            if len(self._stream_buffer) >= self.config.batch_size:
                return await self._flush_stream_mock()
            return SinkResult.ok()

        latency = max(
            0.001,
            (self._sim.latency_ms + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms))
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                code = random.choice(["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"])
                return SinkResult.fail(f"gRPC {code}", retryable=True)
            else:
                code = random.choice(["INVALID_ARGUMENT", "PERMISSION_DENIED", "NOT_FOUND"])
                return SinkResult.fail(f"gRPC {code}", retryable=False)

        return SinkResult.ok()

    async def _flush_stream_mock(self) -> SinkResult:
        """Flush the mock streaming buffer."""
        count = len(self._stream_buffer)
        latency = max(0.001, (self._sim.latency_ms * 0.5) / 1000.0)
        await asyncio.sleep(latency)
        self._stream_buffer.clear()

        if random.random() < self._sim.error_rate:
            return SinkResult.fail("gRPC UNAVAILABLE: Stream flush failed", retryable=True)

        logger.debug("grpc_stream_flushed", records=count)
        return SinkResult.ok()

    async def _do_flush(self) -> None:
        if self._mode == "mock" and self._stream_buffer:
            await self._flush_stream_mock()

    async def _do_close(self) -> None:
        if self._mode == "mock" and self._stream_buffer:
            await self._flush_stream_mock()

        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            logger.debug("grpc_channel_closed", sink=self.name)
