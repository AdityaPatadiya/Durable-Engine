"""Dead Letter Queue for persisting failed records."""

import json
import time
from pathlib import Path

import structlog

from durable_engine.config.models import DlqConfig
from durable_engine.ingestion.record import Record

logger = structlog.get_logger()


class DeadLetterQueue:
    """Persists records that failed all retry attempts to disk."""

    def __init__(self, config: DlqConfig) -> None:
        self._output_dir = Path(config.output_dir)
        self._max_file_size = config.max_file_size_mb * 1024 * 1024
        self._include_metadata = config.include_error_metadata
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._file_handles: dict[str, object] = {}
        self._counts: dict[str, int] = {}

    def _get_dlq_path(self, sink_name: str) -> Path:
        return self._output_dir / f"{sink_name}.jsonl"

    def write(
        self,
        record: Record,
        sink_name: str,
        error: str,
        attempts: int,
    ) -> None:
        """Write a failed record to the DLQ file for the given sink."""
        dlq_path = self._get_dlq_path(sink_name)

        entry: dict[str, object] = {
            "record_id": record.record_id,
            "data": record.data,
            "source_file": record.source_file,
            "line_number": record.line_number,
        }

        if self._include_metadata:
            entry["error_metadata"] = {
                "sink": sink_name,
                "error": error,
                "attempts": attempts,
                "failed_at": time.time(),
            }

        if dlq_path.exists() and dlq_path.stat().st_size >= self._max_file_size:
            rotated = dlq_path.with_suffix(f".{int(time.time())}.jsonl")
            dlq_path.rename(rotated)
            logger.info("dlq_file_rotated", sink=sink_name, rotated_to=str(rotated))

        with open(dlq_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")

        self._counts[sink_name] = self._counts.get(sink_name, 0) + 1
        logger.debug(
            "record_sent_to_dlq",
            sink=sink_name,
            record_id=record.record_id,
            error=error,
        )

    def get_counts(self) -> dict[str, int]:
        """Return DLQ record counts per sink."""
        return dict(self._counts)

    def get_total_count(self) -> int:
        return sum(self._counts.values())
