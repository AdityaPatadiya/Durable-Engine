"""Unit tests for fixed-width file reader."""

from pathlib import Path

import pytest

from durable_engine.config.models import FixedWidthConfig
from durable_engine.ingestion.fixed_width import FixedWidthFileReader


class TestFixedWidthFileReader:
    def test_reads_all_records(self, sample_fixed_width_path: Path) -> None:
        config = FixedWidthConfig(
            column_widths=[10, 20, 15, 30, 10],
            column_names=["id", "name", "age", "email", "status"],
        )
        reader = FixedWidthFileReader(str(sample_fixed_width_path), fw_config=config)
        records = list(reader.read_records())
        assert len(records) == 10

    def test_fields_are_stripped(self, sample_fixed_width_path: Path) -> None:
        config = FixedWidthConfig(
            column_widths=[10, 20, 15, 30, 10],
            column_names=["id", "name", "age", "email", "status"],
        )
        reader = FixedWidthFileReader(str(sample_fixed_width_path), fw_config=config)
        records = list(reader.read_records())
        assert records[0].data["id"] == "1"
        assert records[0].data["name"] == "Alice Johnson"

    def test_missing_widths_raises(self, sample_fixed_width_path: Path) -> None:
        config = FixedWidthConfig(column_widths=[], column_names=[])
        with pytest.raises(ValueError, match="column_widths"):
            FixedWidthFileReader(str(sample_fixed_width_path), fw_config=config)

    def test_mismatched_widths_names_raises(self, sample_fixed_width_path: Path) -> None:
        config = FixedWidthConfig(column_widths=[10, 20], column_names=["a"])
        with pytest.raises(ValueError, match="same length"):
            FixedWidthFileReader(str(sample_fixed_width_path), fw_config=config)
