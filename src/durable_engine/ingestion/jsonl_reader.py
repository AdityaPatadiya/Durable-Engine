"""Streaming JSONL (JSON Lines) file reader."""

import json
from collections.abc import Generator

from durable_engine.ingestion.base import FileReader
from durable_engine.ingestion.record import Record


class JsonlFileReader(FileReader):
    """Reads JSONL files line-by-line using a streaming generator."""

    def _read_records_impl(self) -> Generator[Record, None, None]:
        with open(self._file_path, encoding=self._encoding) as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Invalid JSON at line {line_number} in {self._file_path}: {e}"
                    ) from e

                if not isinstance(data, dict):
                    raise ValueError(
                        f"Expected JSON object at line {line_number}, got {type(data).__name__}"
                    )

                yield Record.from_dict(
                    data=data,
                    source_file=str(self._file_path),
                    line_number=line_number,
                )
