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


class IngestionConfig(BaseModel):
    file_path: str
    file_format: str = "auto"
    encoding: str = "utf-8"
    csv: CsvConfig = Field(default_factory=CsvConfig)
    fixed_width: FixedWidthConfig = Field(default_factory=FixedWidthConfig)
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)


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


class SinkConfig(BaseModel):
    enabled: bool = True
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
    # Sink-specific fields
    topic: str = ""
    partitions: int = 1
    keyspace: str = ""
    table: str = ""
    consistency_level: str = "LOCAL_QUORUM"
    streaming_mode: str = "unary"


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
