"""Shared test fixtures."""

import os
import tempfile
from pathlib import Path

import pytest

from durable_engine.config.models import (
    CircuitBreakerConfig,
    CoreEngineConfig,
    CsvConfig,
    DlqConfig,
    EngineConfig,
    FixedWidthConfig,
    IngestionConfig,
    ObservabilityConfig,
    RateLimitConfig,
    RetryConfig,
    SimulationConfig,
    SinkConfig,
)
from durable_engine.ingestion.record import Record

SAMPLES_DIR = Path(__file__).parent.parent / "samples"


@pytest.fixture
def sample_csv_path() -> Path:
    return SAMPLES_DIR / "sample.csv"


@pytest.fixture
def sample_jsonl_path() -> Path:
    return SAMPLES_DIR / "sample.jsonl"


@pytest.fixture
def sample_fixed_width_path() -> Path:
    return SAMPLES_DIR / "sample.fixed_width.txt"


@pytest.fixture
def sample_record() -> Record:
    return Record.from_dict(
        data={
            "id": "1",
            "name": "Alice Johnson",
            "age": "32",
            "email": "alice.johnson@example.com",
            "status": "active",
        },
        source_file="test.csv",
        line_number=1,
    )


@pytest.fixture
def sample_records() -> list[Record]:
    return [
        Record.from_dict(
            data={"id": str(i), "name": f"User {i}", "email": f"user{i}@test.com"},
            source_file="test.csv",
            line_number=i,
        )
        for i in range(1, 11)
    ]


@pytest.fixture
def sink_config() -> SinkConfig:
    return SinkConfig(
        type="rest",
        transformer="json",
        endpoint="http://localhost:8081/api",
        rate_limit=RateLimitConfig(requests_per_second=1000, burst_size=2000),
        retry=RetryConfig(max_retries=3, base_delay_ms=10, max_delay_ms=100),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=5, recovery_timeout_seconds=1),
        simulation=SimulationConfig(latency_ms=1, latency_jitter_ms=0, error_rate=0.0),
        concurrency=10,
        batch_size=50,
    )


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def dlq_config(tmp_dir) -> DlqConfig:
    return DlqConfig(output_dir=str(tmp_dir / "dlq"))
