"""Streaming CSV file reader."""

import csv
from collections.abc import Generator

from durable_engine.config.models import CsvConfig
from durable_engine.ingestion.base import FileReader
from durable_engine.ingestion.record import Record


class CsvFileReader(FileReader):
    """Reads CSV files line-by-line using a streaming generator."""

    def __init__(
        self,
        file_path: str,
        encoding: str = "utf-8",
        csv_config: CsvConfig | None = None,
    ) -> None:
        super().__init__(file_path, encoding)
        self._csv_config = csv_config or CsvConfig()

    def _read_records_impl(self) -> Generator[Record, None, None]:
        with open(self._file_path, newline="", encoding=self._encoding) as f:
            if self._csv_config.has_header:
                reader = csv.DictReader(
                    f,
                    delimiter=self._csv_config.delimiter,
                    quotechar=self._csv_config.quotechar,
                )
                for line_number, row in enumerate(reader, start=2):
                    yield Record.from_dict(
                        data=dict(row),
                        source_file=str(self._file_path),
                        line_number=line_number,
                    )
            else:
                reader_raw = csv.reader(
                    f,
                    delimiter=self._csv_config.delimiter,
                    quotechar=self._csv_config.quotechar,
                )
                for line_number, raw_row in enumerate(reader_raw, start=1):
                    data: dict[str, object] = {f"col_{i}": val for i, val in enumerate(raw_row)}
                    yield Record.from_dict(
                        data=data,
                        source_file=str(self._file_path),
                        line_number=line_number,
                    )

    def estimate_total_records(self) -> int | None:
        count = 0
        with open(self._file_path, encoding=self._encoding) as f:
            for _ in f:
                count += 1
        if self._csv_config.has_header:
            count -= 1
        return max(count, 0)
