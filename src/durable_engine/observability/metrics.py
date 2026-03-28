"""In-memory metrics collector for tracking throughput and success/failure counts."""

import threading
import time


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
    """Central metrics collector for the engine."""

    def __init__(self) -> None:
        self._sink_metrics: dict[str, SinkMetrics] = {}
        self._start_time = time.monotonic()
        self._ingested_count = 0
        self._lock = threading.Lock()

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def ingested_count(self) -> int:
        return self._ingested_count

    def set_ingested_count(self, count: int) -> None:
        self._ingested_count = count

    def register_sink(self, sink_name: str) -> None:
        with self._lock:
            self._sink_metrics[sink_name] = SinkMetrics()

    def record_success(self, sink_name: str) -> None:
        metrics = self._sink_metrics.get(sink_name)
        if metrics:
            metrics.record_success()

    def record_failure(self, sink_name: str) -> None:
        metrics = self._sink_metrics.get(sink_name)
        if metrics:
            metrics.record_failure()

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
