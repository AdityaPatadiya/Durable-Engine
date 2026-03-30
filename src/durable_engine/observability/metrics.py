"""Metrics collector with in-memory counters and optional Prometheus export."""

import threading
import time
from typing import Any

import structlog

logger = structlog.get_logger()


class SinkMetrics:
    """Thread-safe metrics for a single sink."""

    __slots__ = ("_success", "_failure", "_lock")

    def __init__(self) -> None:
        self._success = 0
        self._failure = 0
        self._lock = threading.Lock()

    @property
    def success(self) -> int:
        return self._success

    @property
    def failure(self) -> int:
        return self._failure

    @property
    def total(self) -> int:
        return self._success + self._failure

    def record_success(self) -> None:
        with self._lock:
            self._success += 1

    def record_failure(self) -> None:
        with self._lock:
            self._failure += 1


class MetricsCollector:
    """Central metrics collector with optional Prometheus export."""

    def __init__(self) -> None:
        self._sink_metrics: dict[str, SinkMetrics] = {}
        self._start_time = time.monotonic()
        self._ingested_count = 0
        self._lock = threading.Lock()
        # Prometheus gauges/counters (initialized lazily)
        self._prom_success: dict[str, Any] = {}
        self._prom_failure: dict[str, Any] = {}
        self._prom_ingested: Any = None
        self._prom_enabled = False

    def enable_prometheus(self) -> None:
        """Initialize Prometheus counters."""
        try:
            from prometheus_client import Gauge

            self._prom_ingested = Gauge(
                "durable_engine_records_ingested_total",
                "Total records ingested from source",
            )
            self._prom_throughput = Gauge(
                "durable_engine_throughput_rps",
                "Current throughput in records per second",
            )
            self._prom_enabled = True
            logger.info("prometheus_metrics_enabled")
        except ImportError:
            logger.warning("prometheus_client_not_installed")

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def ingested_count(self) -> int:
        return self._ingested_count

    def set_ingested_count(self, count: int) -> None:
        self._ingested_count = count
        if self._prom_enabled and self._prom_ingested:
            self._prom_ingested.set(count)

    def register_sink(self, sink_name: str) -> None:
        with self._lock:
            self._sink_metrics[sink_name] = SinkMetrics()

        if self._prom_enabled:
            from prometheus_client import Counter

            self._prom_success[sink_name] = Counter(
                "durable_engine_sink_success_total",
                "Successful records sent to sink",
                ["sink"],
            ).labels(sink=sink_name)
            self._prom_failure[sink_name] = Counter(
                "durable_engine_sink_failure_total",
                "Failed records for sink",
                ["sink"],
            ).labels(sink=sink_name)

    def record_success(self, sink_name: str) -> None:
        metrics = self._sink_metrics.get(sink_name)
        if metrics:
            metrics.record_success()
        prom = self._prom_success.get(sink_name)
        if prom:
            prom.inc()

    def record_failure(self, sink_name: str) -> None:
        metrics = self._sink_metrics.get(sink_name)
        if metrics:
            metrics.record_failure()
        prom = self._prom_failure.get(sink_name)
        if prom:
            prom.inc()

    def get_sink_metrics(self, sink_name: str) -> SinkMetrics | None:
        return self._sink_metrics.get(sink_name)

    def get_all_sink_metrics(self) -> dict[str, SinkMetrics]:
        return dict(self._sink_metrics)

    def get_total_processed(self) -> int:
        return sum(m.total for m in self._sink_metrics.values())

    def get_total_success(self) -> int:
        return sum(m.success for m in self._sink_metrics.values())

    def get_total_failure(self) -> int:
        return sum(m.failure for m in self._sink_metrics.values())

    def get_throughput(self) -> float:
        """Records processed per second (across all sinks)."""
        elapsed = self.elapsed_seconds
        if elapsed <= 0:
            return 0.0
        return self.get_total_processed() / elapsed

    def update_prometheus_gauges(self) -> None:
        """Update Prometheus gauges (called periodically by reporter)."""
        if self._prom_enabled:
            self._prom_throughput.set(self.get_throughput())
            if self._prom_ingested:
                self._prom_ingested.set(self._ingested_count)
