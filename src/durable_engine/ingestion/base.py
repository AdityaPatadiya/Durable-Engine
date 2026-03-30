"""Abstract base classes for data sources."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Generator
from pathlib import Path

from durable_engine.ingestion.record import Record


class RecordSource(ABC):
    """Abstract base for all record sources — both files and live streams.

    This is the core interface that the pipeline consumes from.
    Sources can be finite (files) or infinite (Kafka, webhooks, CDC).
    """

    def __init__(self) -> None:
        self._records_read = 0

    @property
    def records_read(self) -> int:
        return self._records_read

    @property
    def source_name(self) -> str:
        """Human-readable name for this source (for logging/metrics)."""
        return self.__class__.__name__

    @property
    def is_streaming(self) -> bool:
        """Whether this source produces records indefinitely (True) or finishes (False)."""
        return False

    async def setup(self) -> None:  # noqa: B027
        """Initialize connections, subscriptions, etc. Called once before reading."""

    @abstractmethod
    async def read_records_async(self) -> AsyncIterator[Record]:
        """Yield records asynchronously. This is the primary interface for the pipeline."""
        ...

    async def teardown(self) -> None:  # noqa: B027
        """Clean up connections. Called once after reading is done."""

    def estimate_total_records(self) -> int | None:
        """Estimate total number of records. Returns None for streaming sources."""
        return None

    async def __aenter__(self) -> "RecordSource":
        await self.setup()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.teardown()


class FileReader(RecordSource):
    """Abstract streaming file reader. Wraps sync generator into async interface."""

    def __init__(self, file_path: str, encoding: str = "utf-8") -> None:
        super().__init__()
        self._file_path = Path(file_path)
        self._encoding = encoding

        if not self._file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        if not self._file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")

    @property
    def file_path(self) -> Path:
        return self._file_path

    @property
    def source_name(self) -> str:
        return str(self._file_path)

    def read_records(self) -> Generator[Record, None, None]:
        """Yield records one at a time, tracking count."""
        for record in self._read_records_impl():
            self._records_read += 1
            yield record

    @abstractmethod
    def _read_records_impl(self) -> Generator[Record, None, None]:
        """Yield records one at a time from the source file. Must not load entire file."""
        ...

    async def read_records_async(self) -> AsyncIterator[Record]:
        """Wrap the sync generator into an async iterator."""
        for record in self._read_records_impl():
            self._records_read += 1
            yield record
