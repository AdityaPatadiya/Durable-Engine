"""Streaming fixed-width file reader."""

from collections.abc import Generator

from durable_engine.config.models import FixedWidthConfig
from durable_engine.ingestion.base import FileReader
from durable_engine.ingestion.record import Record


class FixedWidthFileReader(FileReader):
    """Reads fixed-width format files line-by-line."""

    def __init__(
        self,
        file_path: str,
        encoding: str = "utf-8",
        fw_config: FixedWidthConfig | None = None,
    ) -> None:
        super().__init__(file_path, encoding)
        self._fw_config = fw_config or FixedWidthConfig()

        if not self._fw_config.column_widths:
            raise ValueError("Fixed-width reader requires column_widths in config")
        if not self._fw_config.column_names:
            self._fw_config.column_names = [
                f"col_{i}" for i in range(len(self._fw_config.column_widths))
            ]
        if len(self._fw_config.column_widths) != len(self._fw_config.column_names):
            raise ValueError("column_widths and column_names must have the same length")

    def _parse_line(self, line: str) -> dict[str, str]:
        fields: dict[str, str] = {}
        offset = 0
        for width, name in zip(
            self._fw_config.column_widths, self._fw_config.column_names, strict=True
        ):
            value = line[offset : offset + width].strip()
            fields[name] = value
            offset += width
        return fields

    def _read_records_impl(self) -> Generator[Record, None, None]:
        with open(self._file_path, encoding=self._encoding) as f:
            for line_number, line in enumerate(f, start=1):
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue

                data = self._parse_line(line)
                yield Record.from_dict(
                    data=data,
                    source_file=str(self._file_path),
                    line_number=line_number,
                )
