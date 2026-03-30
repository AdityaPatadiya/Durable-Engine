"""Checkpoint manager — tracks processing progress for crash recovery.

Periodically saves the number of records processed to a JSON file.
On restart, the engine can skip already-processed records.
"""

import asyncio
import contextlib
import json
import time
from pathlib import Path

import structlog

from durable_engine.config.models import CheckpointConfig

logger = structlog.get_logger()


class CheckpointManager:
    """Tracks and persists ingestion progress for resumable processing."""

    def __init__(self, config: CheckpointConfig) -> None:
        self._enabled = config.enabled
        self._file = Path(config.file)
        self._interval = config.interval_seconds
        self._records_processed = 0
        self._last_save_time = 0.0
        self._source_file = ""
        self._task: asyncio.Task[None] | None = None

    @property
    def records_processed(self) -> int:
        return self._records_processed

    def load(self, source_file: str) -> int:
        """Load checkpoint for the given source file. Returns records to skip."""
        self._source_file = source_file

        if not self._enabled:
            return 0

        if not self._file.exists():
            return 0

        try:
            data = json.loads(self._file.read_text(encoding="utf-8"))
            if data.get("source_file") == source_file:
                skip: int = data.get("records_processed", 0)
                logger.info(
                    "checkpoint_loaded",
                    source=source_file,
                    skip_records=skip,
                    saved_at=data.get("saved_at"),
                )
                self._records_processed = skip
                return skip
            else:
                logger.info(
                    "checkpoint_source_mismatch",
                    checkpoint_source=data.get("source_file"),
                    current_source=source_file,
                )
                return 0
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("checkpoint_load_failed", error=str(e))
            return 0

    def advance(self, count: int = 1) -> None:
        """Advance the checkpoint by count records."""
        self._records_processed += count

    def save(self) -> None:
        """Save current checkpoint to disk."""
        if not self._enabled:
            return

        self._file.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "source_file": self._source_file,
            "records_processed": self._records_processed,
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "timestamp": time.time(),
        }

        # Write atomically via temp file
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self._file)

        self._last_save_time = time.monotonic()
        logger.debug(
            "checkpoint_saved",
            records=self._records_processed,
            file=str(self._file),
        )

    async def start_periodic_save(self, shutdown_event: asyncio.Event) -> None:
        """Periodically save checkpoint in the background."""
        if not self._enabled:
            return

        while not shutdown_event.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shutdown_event.wait(), timeout=self._interval)
            self.save()

    def clear(self) -> None:
        """Clear the checkpoint file (call after successful completion)."""
        if self._file.exists():
            self._file.unlink()
            logger.info("checkpoint_cleared", file=str(self._file))
