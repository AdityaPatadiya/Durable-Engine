"""Wide-Column DB sink — mock simulation and live Cassandra/ScyllaDB/DynamoDB clients."""

import asyncio
import json
import random

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.sinks.base import BaseSink, SinkResult

logger = structlog.get_logger()

CONSISTENCY_LATENCY_FACTOR = {
    "ONE": 0.5,
    "QUORUM": 1.0,
    "LOCAL_QUORUM": 0.8,
    "EACH_QUORUM": 1.5,
    "ALL": 2.0,
}


class WideColumnDbSink(BaseSink):
    """Performs async UPSERT operations to a wide-column database.

    Supports two modes:
    - mock: Simulates UPSERT with configurable latency/errors
    - live: Writes to real Cassandra/ScyllaDB or DynamoDB
    """

    def __init__(self, name: str, config: SinkConfig) -> None:
        super().__init__(name, config)
        self._sim = config.simulation
        self._mode = config.mode
        self._keyspace = config.keyspace
        self._table = config.table
        self._consistency = config.consistency_level
        self._db_type = config.db_type
        self._latency_factor = CONSISTENCY_LATENCY_FACTOR.get(self._consistency, 1.0)
        # Live mode state
        self._cluster = None
        self._session = None
        self._prepared_stmt = None
        self._dynamodb_table = None

    async def _do_connect(self) -> None:
        if self._mode == "live":
            if self._db_type in ("cassandra", "scylladb"):
                await self._connect_cassandra()
            elif self._db_type == "dynamodb":
                await self._connect_dynamodb()
            else:
                raise ValueError(f"Unsupported DB type: {self._db_type}")
        else:
            await asyncio.sleep(0.02)
            logger.debug("widecolumn_mock_connected", keyspace=self._keyspace, table=self._table)

    async def _connect_cassandra(self) -> None:
        """Connect to a real Cassandra/ScyllaDB cluster."""
        from cassandra import ConsistencyLevel
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy

        loop = asyncio.get_event_loop()

        # Parse contact points from endpoint (comma-separated hosts)
        contact_points = [h.strip() for h in self.config.endpoint.split(",")]
        # Separate host and port
        hosts = []
        port = 9042
        for cp in contact_points:
            if ":" in cp:
                host, port_str = cp.rsplit(":", 1)
                hosts.append(host)
                port = int(port_str)
            else:
                hosts.append(cp)

        # Auth provider
        auth_provider = None
        if self.config.auth.type == "basic":
            auth_provider = PlainTextAuthProvider(
                username=self.config.auth.username,
                password=self.config.auth.password,
            )

        # SSL options
        ssl_options = None
        if self.config.tls.enabled:
            import ssl

            ssl_context = ssl.create_default_context(cafile=self.config.tls.ca_path or None)
            if self.config.tls.cert_path and self.config.tls.key_path:
                ssl_context.load_cert_chain(
                    self.config.tls.cert_path,
                    self.config.tls.key_path,
                )
            if not self.config.tls.verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            ssl_options = {"ssl_context": ssl_context}

        def _connect():
            cluster = Cluster(
                contact_points=hosts,
                port=port,
                auth_provider=auth_provider,
                ssl_options=ssl_options,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                protocol_version=4,
            )
            session = cluster.connect(self._keyspace)

            # Map consistency level string to cassandra enum
            cl_map = {
                "ONE": ConsistencyLevel.ONE,
                "QUORUM": ConsistencyLevel.QUORUM,
                "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
                "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
                "ALL": ConsistencyLevel.ALL,
            }
            session.default_consistency_level = cl_map.get(
                self._consistency, ConsistencyLevel.LOCAL_QUORUM
            )

            # Prepare the UPSERT statement
            cols = "record_id, data, source_file, line_number, created_at"
            prepared = session.prepare(
                f"INSERT INTO {self._table} ({cols}) VALUES (?, ?, ?, ?, toTimestamp(now()))"
            )
            return cluster, session, prepared

        self._cluster, self._session, self._prepared_stmt = await loop.run_in_executor(
            None, _connect
        )

        logger.info(
            "cassandra_connected",
            hosts=hosts,
            keyspace=self._keyspace,
            table=self._table,
            consistency=self._consistency,
        )

    async def _connect_dynamodb(self) -> None:
        """Connect to AWS DynamoDB."""
        import aiobotocore.session

        session = aiobotocore.session.get_session()

        # Auth from config or environment (AWS_ACCESS_KEY_ID, etc.)
        kwargs = {}
        if self.config.auth.type == "basic":
            kwargs["aws_access_key_id"] = self.config.auth.username
            kwargs["aws_secret_access_key"] = self.config.auth.password
        if self.config.endpoint and not self.config.endpoint.startswith("dynamodb"):
            kwargs["endpoint_url"] = self.config.endpoint

        self._dynamodb_client_ctx = session.create_client("dynamodb", **kwargs)
        self._dynamodb_client = await self._dynamodb_client_ctx.__aenter__()

        logger.info(
            "dynamodb_connected",
            table=self._table,
            endpoint=self.config.endpoint or "AWS default",
        )

    async def _do_send(self, data: bytes) -> SinkResult:
        if self._mode == "live":
            if self._db_type in ("cassandra", "scylladb"):
                return await self._send_cassandra(data)
            else:
                return await self._send_dynamodb(data)
        return await self._send_mock(data)

    async def _send_cassandra(self, data: bytes) -> SinkResult:
        """Execute a real CQL UPSERT."""
        from cassandra import OperationTimedOut, Unavailable, WriteTimeout

        loop = asyncio.get_event_loop()

        try:
            # Deserialize the Avro/JSON data to extract fields
            record_data = json.loads(data) if data[0:1] == b"{" else {"raw": data.hex()}
            record_id = record_data.get("id", record_data.get("__record_id", ""))
            fields_json = json.dumps(record_data.get("fields", record_data), default=str)
            source_file = record_data.get("source_file", record_data.get("__source_file", ""))
            line_number = int(record_data.get("line_number", record_data.get("__line_number", 0)))

            def _execute():
                return self._session.execute(
                    self._prepared_stmt,
                    [record_id, fields_json, source_file, line_number],
                )

            await loop.run_in_executor(None, _execute)
            return SinkResult.ok()

        except (WriteTimeout, OperationTimedOut) as e:
            return SinkResult.fail(f"CQL WriteTimeout: {e}", retryable=True)
        except Unavailable as e:
            return SinkResult.fail(f"CQL Unavailable: {e}", retryable=True)
        except Exception as e:
            error_name = type(e).__name__
            retryable = "Timeout" in error_name or "Unavailable" in error_name
            return SinkResult.fail(f"CQL {error_name}: {e}", retryable=retryable)

    async def _send_dynamodb(self, data: bytes) -> SinkResult:
        """Execute a real DynamoDB PutItem."""
        from botocore.exceptions import ClientError

        try:
            record_data = json.loads(data) if data[0:1] == b"{" else {"raw": data.hex()}
            record_id = record_data.get("id", record_data.get("__record_id", ""))

            # Build DynamoDB item — all values as strings for simplicity
            item = {"record_id": {"S": str(record_id)}}
            fields = record_data.get("fields", record_data)
            if isinstance(fields, dict):
                for k, v in fields.items():
                    if k.startswith("__"):
                        continue
                    item[k] = {"S": str(v)}

            await self._dynamodb_client.put_item(
                TableName=self._table,
                Item=item,
            )
            return SinkResult.ok()

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            retryable = error_code in (
                "ProvisionedThroughputExceededException",
                "ThrottlingException",
                "InternalServerError",
                "ServiceUnavailable",
            )
            return SinkResult.fail(f"DynamoDB {error_code}: {e}", retryable=retryable)
        except Exception as e:
            return SinkResult.fail(f"DynamoDB error: {e}", retryable=True)

    async def _send_mock(self, data: bytes) -> SinkResult:
        """Simulate a DB UPSERT."""
        latency = max(
            0.001,
            (
                self._sim.latency_ms * self._latency_factor
                + random.uniform(-self._sim.latency_jitter_ms, self._sim.latency_jitter_ms)
            )
            / 1000.0,
        )
        await asyncio.sleep(latency)

        if random.random() < self._sim.error_rate:
            is_transient = random.random() < self._sim.transient_error_rate
            if is_transient:
                error = random.choice(
                    ["WriteTimeoutException", "UnavailableException", "OverloadedException"]
                )
                return SinkResult.fail(
                    f"CQL {error} (consistency={self._consistency})", retryable=True
                )
            else:
                error = random.choice(["InvalidQueryException", "UnauthorizedException"])
                return SinkResult.fail(f"CQL {error} (keyspace={self._keyspace})", retryable=False)

        return SinkResult.ok()

    async def send_batch(self, batch: list[bytes]) -> list[SinkResult]:
        if self._mode == "mock":
            # Simulated batch UPSERT
            latency_per_record = max(
                0.0005, (self._sim.latency_ms * self._latency_factor * 0.3) / 1000.0
            )
            batch_latency = latency_per_record * len(batch)
            await asyncio.sleep(min(batch_latency, 1.0))

            results: list[SinkResult] = []
            for _ in batch:
                if random.random() < self._sim.error_rate:
                    results.append(SinkResult.fail("CQL WriteTimeoutException", retryable=True))
                    self._records_failed += 1
                else:
                    results.append(SinkResult.ok())
                    self._records_sent += 1
            return results
        else:
            # Live mode — send concurrently
            tasks = [self.send(data) for data in batch]
            return list(await asyncio.gather(*tasks))

    async def _do_close(self) -> None:
        if self._cluster is not None:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._cluster.shutdown)
            self._cluster = None
            self._session = None
            self._prepared_stmt = None
            logger.debug("cassandra_cluster_closed", sink=self.name)

        if hasattr(self, "_dynamodb_client_ctx") and self._dynamodb_client_ctx is not None:
            await self._dynamodb_client_ctx.__aexit__(None, None, None)
            self._dynamodb_client = None
            logger.debug("dynamodb_client_closed", sink=self.name)
