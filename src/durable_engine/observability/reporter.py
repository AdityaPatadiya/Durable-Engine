"""Periodic metrics reporter — prints status updates every N seconds."""

import asyncio
import time

import structlog

from durable_engine.config.models import ReporterConfig
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.sinks.base import BaseSink

logger = structlog.get_logger()


class MetricsReporter:
    """Reports engine metrics at configurable intervals."""

    def __init__(
        self,
        metrics: MetricsCollector,
        config: ReporterConfig,
        sinks: dict[str, BaseSink],
    ) -> None:
        self._metrics = metrics
        self._interval = config.interval_seconds
        self._format = config.format
        self._sinks = sinks
        self._start_time = time.monotonic()

    async def start(self) -> None:
        """Start the periodic reporting loop."""
        while True:
            await asyncio.sleep(self._interval)
            self._print_status()

    def _print_status(self) -> None:
        elapsed = self._metrics.elapsed_seconds
        total = self._metrics.get_total_processed()
        throughput = self._metrics.get_throughput()

        if self._format == "json":
            self._print_json_status(elapsed, total, throughput)
        elif self._format == "rich":
            self._print_rich_status(elapsed, total, throughput)
        else:
            self._print_plain_status(elapsed, total, throughput)

    def _print_plain_status(self, elapsed: float, total: int, throughput: float) -> None:
        lines = [
            f"\n{'='*60}",
            f"  Status Report  |  Elapsed: {elapsed:.1f}s",
            f"  Records Processed: {total}  |  Throughput: {throughput:.1f} rec/s",
            f"{'─'*60}",
        ]

        for sink_name, sink_metrics in self._metrics.get_all_sink_metrics().items():
            lines.append(
                f"  {sink_name:20s}  |  OK: {sink_metrics.success:>8d}  |  "
                f"FAIL: {sink_metrics.failure:>6d}  |  Total: {sink_metrics.total:>8d}"
            )

        lines.append(f"{'='*60}\n")
        print("\n".join(lines))

    def _print_rich_status(self, elapsed: float, total: int, throughput: float) -> None:
        try:
            from rich.console import Console
            from rich.table import Table

            console = Console()
            table = Table(title=f"Engine Status  |  {elapsed:.1f}s elapsed  |  {throughput:.1f} rec/s")
            table.add_column("Sink", style="cyan")
            table.add_column("Success", style="green", justify="right")
            table.add_column("Failure", style="red", justify="right")
            table.add_column("Total", justify="right")

            for sink_name, sink_metrics in self._metrics.get_all_sink_metrics().items():
                table.add_row(
                    sink_name,
                    str(sink_metrics.success),
                    str(sink_metrics.failure),
                    str(sink_metrics.total),
                )

            console.print(table)
        except ImportError:
            self._print_plain_status(elapsed, total, throughput)

    def _print_json_status(self, elapsed: float, total: int, throughput: float) -> None:
        logger.info(
            "status_report",
            elapsed_s=round(elapsed, 1),
            total_processed=total,
            throughput_rps=round(throughput, 1),
            sinks={
                name: {"success": m.success, "failure": m.failure, "total": m.total}
                for name, m in self._metrics.get_all_sink_metrics().items()
            },
        )

    def print_final_summary(self) -> None:
        elapsed = self._metrics.elapsed_seconds
        total = self._metrics.get_total_processed()
        throughput = self._metrics.get_throughput()
        ingested = self._metrics.ingested_count

        print(f"\n{'='*60}")
        print(f"  FINAL SUMMARY")
        print(f"{'='*60}")
        print(f"  Duration:           {elapsed:.2f}s")
        print(f"  Records Ingested:   {ingested}")
        print(f"  Total Dispatched:   {total}")
        print(f"  Total Success:      {self._metrics.get_total_success()}")
        print(f"  Total Failures:     {self._metrics.get_total_failure()}")
        print(f"  Avg Throughput:     {throughput:.1f} rec/s")
        print(f"{'─'*60}")

        for sink_name, m in self._metrics.get_all_sink_metrics().items():
            print(f"  {sink_name:20s}  OK: {m.success:>8d}  FAIL: {m.failure:>6d}")

        print(f"{'='*60}\n")
