"""Unit tests for CSV file reader."""

from pathlib import Path

import pytest

from durable_engine.config.models import CsvConfig
from durable_engine.ingestion.csv_reader import CsvFileReader


class TestCsvFileReader:
    def test_reads_all_records(self, sample_csv_path: Path) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        records = list(reader.read_records())
        assert len(records) == 10

    def test_records_have_correct_fields(self, sample_csv_path: Path) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        records = list(reader.read_records())
        first = records[0]
        assert first.data["name"] == "Alice Johnson"
        assert first.data["email"] == "alice.johnson@example.com"

    def test_records_have_source_metadata(self, sample_csv_path: Path) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        records = list(reader.read_records())
        for r in records:
            assert r.source_file == str(sample_csv_path)
            assert r.line_number >= 2  # Header is line 1

    def test_records_read_count(self, sample_csv_path: Path) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        list(reader.read_records())
        assert reader.records_read == 10

    def test_file_not_found_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            CsvFileReader("/nonexistent/file.csv")

    def test_estimate_total_records(self, sample_csv_path: Path) -> None:
        reader = CsvFileReader(str(sample_csv_path))
        total = reader.estimate_total_records()
        assert total == 10

    def test_custom_delimiter(self, tmp_dir: Path) -> None:
        tsv = tmp_dir / "data.csv"
        tsv.write_text("name\tage\nAlice\t32\nBob\t28\n")
        config = CsvConfig(delimiter="\t")
        reader = CsvFileReader(str(tsv), csv_config=config)
        records = list(reader.read_records())
        assert len(records) == 2
        assert records[0].data["name"] == "Alice"
