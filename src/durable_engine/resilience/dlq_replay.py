"""DLQ Replay tool — re-ingest failed records from Dead Letter Queue files.

Usage:
    # Replay all DLQ files through the engine
    python -m durable_engine.resilience.dlq_replay --dlq-dir dlq/ --config config/default.yaml

    # Replay only a specific sink's DLQ
    python -m durable_engine.resilience.dlq_replay \
        --dlq-dir dlq/ --config config/default.yaml --sink rest_api

    # Dry-run (show what would be replayed)
    python -m durable_engine.resilience.dlq_replay --dlq-dir dlq/ --dry-run
"""

import asyncio
import json
import sys
from pathlib import Path

import click
import structlog

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record

logger = structlog.get_logger()


class DlqFileReader(RecordSource):
    """Reads records from DLQ JSONL files for replay."""

    def __init__(self, dlq_files: list[Path]) -> None:
        super().__init__()
        self._dlq_files = dlq_files
        self._total_records = 0

    @property
    def source_name(self) -> str:
        return f"dlq-replay ({len(self._dlq_files)} files)"

    async def read_records_async(self):
        for dlq_file in self._dlq_files:
            logger.info("replaying_dlq_file", file=str(dlq_file))
            content = await asyncio.to_thread(dlq_file.read_text, encoding="utf-8")
            for line_number, line in enumerate(content.splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("invalid_dlq_line", file=str(dlq_file), line=line_number)
                    continue

                record = Record.from_dict(
                    data=entry.get("data", {}),
                    source_file=entry.get("source_file", f"dlq:{dlq_file.name}"),
                    line_number=entry.get("line_number", line_number),
                )
                # Preserve original record_id if available
                if "record_id" in entry:
                    record.record_id = entry["record_id"]

                self._records_read += 1
                yield record


def find_dlq_files(dlq_dir: str, sink_filter: str | None = None) -> list[Path]:
    """Find DLQ JSONL files in the given directory."""
    dlq_path = Path(dlq_dir)
    if not dlq_path.exists():
        return []

    files = list(dlq_path.glob("*.jsonl"))
    if sink_filter:
        files = [f for f in files if f.stem == sink_filter or f.stem.startswith(f"{sink_filter}.")]

    return sorted(files)


def count_dlq_records(dlq_files: list[Path]) -> dict[str, int]:
    """Count records per DLQ file."""
    counts: dict[str, int] = {}
    for f in dlq_files:
        with open(f) as fh:
            count = sum(1 for line in fh if line.strip())
        counts[f.stem] = count
    return counts


@click.command("dlq-replay")
@click.option("--dlq-dir", type=click.Path(exists=True), default="dlq", help="DLQ directory")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True),
    default="config/default.yaml",
    help="Engine config",
)
@click.option("--sink", "sink_filter", default=None, help="Replay only this sink's DLQ")
@click.option("--dry-run", is_flag=True, help="Show what would be replayed without running")
@click.option("--clear-after", is_flag=True, help="Delete DLQ files after successful replay")
def replay_dlq(
    dlq_dir: str, config_path: str, sink_filter: str | None, dry_run: bool, clear_after: bool
) -> None:
    """Replay failed records from the Dead Letter Queue."""
    dlq_files = find_dlq_files(dlq_dir, sink_filter)

    if not dlq_files:
        click.echo(
            f"No DLQ files found in {dlq_dir}"
            + (f" for sink '{sink_filter}'" if sink_filter else "")
        )
        sys.exit(0)

    counts = count_dlq_records(dlq_files)
    total = sum(counts.values())

    click.echo("\nDLQ Replay Summary")
    click.echo(f"{'=' * 50}")
    for name, count in counts.items():
        click.echo(f"  {name:30s}  {count:>6d} records")
    click.echo(f"{'─' * 50}")
    click.echo(f"  {'Total':30s}  {total:>6d} records")
    click.echo(f"{'=' * 50}\n")

    if dry_run:
        click.echo("Dry run — no records will be replayed.")
        sys.exit(0)

    if total == 0:
        click.echo("No records to replay.")
        sys.exit(0)

    # Run the engine with DLQ files as input
    from durable_engine.config.loader import load_config
    from durable_engine.observability.structured_log import setup_logging

    config = load_config(Path(config_path))
    config.ingestion.source_type = "file"  # Will be overridden
    setup_logging(config.observability.logging)

    async def _run_replay() -> int:
        from durable_engine.observability.metrics import MetricsCollector
        from durable_engine.observability.reporter import MetricsReporter
        from durable_engine.orchestrator.engine import FanOutEngine
        from durable_engine.resilience.dlq import DeadLetterQueue
        from durable_engine.sinks.sink_factory import SinkFactory
        from durable_engine.transformation.transformer_registry import TransformerRegistry

        metrics = MetricsCollector()
        sinks = SinkFactory.create_all(config.sinks, metrics)
        dlq = DeadLetterQueue(config.dlq)
        source = DlqFileReader(dlq_files)

        engine = FanOutEngine(
            config=config.engine,
            reader=source,
            transformer_registry=TransformerRegistry(),
            sinks=sinks,
            metrics=metrics,
            dlq=dlq,
            shutdown_event=asyncio.Event(),
        )

        reporter = MetricsReporter(
            metrics=metrics, config=config.observability.reporter, sinks=sinks
        )

        await engine.start()
        reporter.print_final_summary()

        # Clear DLQ files if requested and no new failures
        if clear_after and dlq.get_total_count() == 0:
            for f in dlq_files:
                f.unlink()
                click.echo(f"  Deleted: {f}")

        return 0

    exit_code = asyncio.run(_run_replay())
    sys.exit(exit_code)


if __name__ == "__main__":
    replay_dlq()
