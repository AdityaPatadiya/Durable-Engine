"""Abstract base class for file readers."""

from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path

from durable_engine.ingestion.record import Record


class FileReader(ABC):
    """Abstract streaming file reader. All readers must yield records one at a time."""

    def __init__(self, file_path: str, encoding: str = "utf-8") -> None:
        self._file_path = Path(file_path)
        self._encoding = encoding
        self._records_read = 0

        if not self._file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        if not self._file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")

    @property
    def file_path(self) -> Path:
        return self._file_path

    @property
    def records_read(self) -> int:
        return self._records_read

    @abstractmethod
    def read_records(self) -> Generator[Record, None, None]:
        """Yield records one at a time from the source file. Must not load entire file."""
        ...

    def estimate_total_records(self) -> int | None:
        """Estimate total number of records. Returns None if unknown."""
        return None
