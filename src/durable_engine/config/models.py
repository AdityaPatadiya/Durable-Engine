"""Pydantic configuration models with validation."""

from pydantic import BaseModel, Field


class CsvConfig(BaseModel):
    delimiter: str = ","
    quotechar: str = '"'
    has_header: bool = True


class FixedWidthConfig(BaseModel):
    column_widths: list[int] = Field(default_factory=list)
    column_names: list[str] = Field(default_factory=list)


class CheckpointConfig(BaseModel):
    enabled: bool = True
    file: str = "output/checkpoint.json"
    interval_seconds: int = 10


class KafkaSourceConfig(BaseModel):
    """Config for Kafka consumer source."""

    brokers: str = "localhost:9092"
    topic: str = ""
    group_id: str = "durable-engine"
    data_format: str = "json"  # json | raw
    auto_offset_reset: str = "earliest"  # earliest | latest
    auth_username: str = ""
    auth_password: str = ""
    tls_enabled: bool = False
    tls_ca_path: str = ""


class WebhookSourceConfig(BaseModel):
    """Config for HTTP webhook source."""

    host: str = "0.0.0.0"
    port: int = 8082
    path: str = "/ingest"
    auth_token: str = ""
    max_queue_size: int = 50000


class PostgresCdcConfig(BaseModel):
    """Config for PostgreSQL CDC source."""

    dsn: str = ""  # e.g. "postgresql://user:pass@localhost:5432/mydb"
    publication: str = "durable_engine_pub"
    slot_name: str = "durable_engine_slot"
    tables: list[str] = Field(default_factory=list)


class WebSocketSourceConfig(BaseModel):
    """Config for WebSocket source."""

    url: str = ""  # e.g. "wss://stream.example.com/events"
    auth_token: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    reconnect_delay: float = 5.0
    ping_interval: float = 30.0
    subscribe_message: str = ""


class IngestionConfig(BaseModel):
    enabled: bool = True
    source_type: str = "file"  # file | kafka | webhook | postgres_cdc | websocket
    # File source settings
    file_path: str = ""
    file_format: str = "auto"
    encoding: str = "utf-8"
    csv: CsvConfig = Field(default_factory=CsvConfig)
    fixed_width: FixedWidthConfig = Field(default_factory=FixedWidthConfig)
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)
    # Live source settings
    kafka: KafkaSourceConfig = Field(default_factory=KafkaSourceConfig)
    webhook: WebhookSourceConfig = Field(default_factory=WebhookSourceConfig)
    postgres_cdc: PostgresCdcConfig = Field(default_factory=PostgresCdcConfig)
    websocket: WebSocketSourceConfig = Field(default_factory=WebSocketSourceConfig)


class RateLimitConfig(BaseModel):
    requests_per_second: float = 100.0
    burst_size: int = 200


class RetryConfig(BaseModel):
    max_retries: int = 3
    base_delay_ms: int = 100
    max_delay_ms: int = 5000
    backoff_multiplier: float = 2.0


class CircuitBreakerConfig(BaseModel):
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 30
    half_open_max_calls: int = 3


class SimulationConfig(BaseModel):
    latency_ms: int = 50
    latency_jitter_ms: int = 20
    error_rate: float = 0.02
    transient_error_rate: float = 0.8


class AuthConfig(BaseModel):
    """Authentication configuration for live sinks."""

    type: str = "none"  # none | bearer | basic | api_key | mtls
    token: str = ""
    username: str = ""
    password: str = ""
    api_key: str = ""
    api_key_header: str = "X-API-Key"
    cert_path: str = ""
    key_path: str = ""
    ca_path: str = ""


class TlsConfig(BaseModel):
    """TLS/SSL configuration for live sinks."""

    enabled: bool = False
    cert_path: str = ""
    key_path: str = ""
    ca_path: str = ""
    verify: bool = True


class SinkConfig(BaseModel):
    enabled: bool = True
    mode: str = "mock"  # mock | live
    type: str
    endpoint: str = ""
    transformer: str
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    concurrency: int = 10
    timeout_seconds: int = 30
    batch_size: int = 50
    simulation: SimulationConfig = Field(default_factory=SimulationConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    tls: TlsConfig = Field(default_factory=TlsConfig)
    # REST-specific
    http_method: str = "POST"
    http_path: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    # MQ-specific
    topic: str = ""
    partitions: int = 1
    mq_type: str = "kafka"  # kafka | rabbitmq
    rabbitmq_exchange: str = ""
    rabbitmq_routing_key: str = ""
    # gRPC-specific
    streaming_mode: str = "unary"
    grpc_service: str = ""
    grpc_method: str = ""
    # Wide-column DB specific
    keyspace: str = ""
    table: str = ""
    consistency_level: str = "LOCAL_QUORUM"
    db_type: str = "cassandra"  # cassandra | scylladb | dynamodb


class CoreEngineConfig(BaseModel):
    name: str = "durable-engine"
    batch_size: int = 500
    max_queue_size: int = 10000
    worker_count: int = 4
    shutdown_timeout_seconds: int = 30


class DlqConfig(BaseModel):
    output_dir: str = "dlq"
    max_file_size_mb: int = 100
    include_error_metadata: bool = True


class ReporterConfig(BaseModel):
    interval_seconds: int = 5
    format: str = "rich"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"
    file: str = "output/engine.log"


class HealthConfig(BaseModel):
    enabled: bool = True
    port: int = 8080
    path: str = "/health"


class MetricsExportConfig(BaseModel):
    enabled: bool = True
    port: int = 9090
    path: str = "/metrics"


class ObservabilityConfig(BaseModel):
    reporter: ReporterConfig = Field(default_factory=ReporterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    health: HealthConfig = Field(default_factory=HealthConfig)
    metrics: MetricsExportConfig = Field(default_factory=MetricsExportConfig)


class EngineConfig(BaseModel):
    engine: CoreEngineConfig = Field(default_factory=CoreEngineConfig)
    ingestion: IngestionConfig
    sinks: dict[str, SinkConfig] = Field(default_factory=dict)
    dlq: DlqConfig = Field(default_factory=DlqConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
