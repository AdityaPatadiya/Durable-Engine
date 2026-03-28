"""Factory for creating file readers based on config or file extension."""

from pathlib import Path

from durable_engine.config.models import IngestionConfig
from durable_engine.ingestion.base import FileReader
from durable_engine.ingestion.csv_reader import CsvFileReader
from durable_engine.ingestion.fixed_width import FixedWidthFileReader
from durable_engine.ingestion.jsonl_reader import JsonlFileReader

_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".jsonl": "jsonl",
    ".json": "jsonl",
    ".txt": "fixed_width",
    ".dat": "fixed_width",
    ".fw": "fixed_width",
}


class ReaderFactory:
    """Creates the appropriate FileReader based on configuration."""

    @staticmethod
    def create(config: IngestionConfig) -> FileReader:
        file_format = config.file_format

        if file_format == "auto":
            ext = Path(config.file_path).suffix.lower()
            file_format = _EXTENSION_MAP.get(ext)
            if file_format is None:
                raise ValueError(
                    f"Cannot auto-detect format for extension '{ext}'. "
                    f"Set file_format explicitly in config."
                )

        if file_format == "csv":
            return CsvFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
                csv_config=config.csv,
            )
        elif file_format == "jsonl":
            return JsonlFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
            )
        elif file_format == "fixed_width":
            return FixedWidthFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
                fw_config=config.fixed_width,
            )
        else:
            raise ValueError(f"Unsupported file format: '{file_format}'")
