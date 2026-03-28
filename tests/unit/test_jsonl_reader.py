"""Unit tests for JSONL file reader."""

from pathlib import Path

import pytest

from durable_engine.ingestion.jsonl_reader import JsonlFileReader


class TestJsonlFileReader:
    def test_reads_all_records(self, sample_jsonl_path: Path) -> None:
        reader = JsonlFileReader(str(sample_jsonl_path))
        records = list(reader.read_records())
        assert len(records) == 10

    def test_records_have_correct_data(self, sample_jsonl_path: Path) -> None:
        reader = JsonlFileReader(str(sample_jsonl_path))
        records = list(reader.read_records())
        assert records[0].data["name"] == "Alice Johnson"
        assert records[0].data["age"] == 32

    def test_records_read_count(self, sample_jsonl_path: Path) -> None:
        reader = JsonlFileReader(str(sample_jsonl_path))
        list(reader.read_records())
        assert reader.records_read == 10

    def test_invalid_json_raises(self, tmp_dir: Path) -> None:
        bad_file = tmp_dir / "bad.jsonl"
        bad_file.write_text('{"valid": true}\nnot json\n')
        reader = JsonlFileReader(str(bad_file))
        with pytest.raises(ValueError, match="Invalid JSON"):
            list(reader.read_records())

    def test_skips_blank_lines(self, tmp_dir: Path) -> None:
        file = tmp_dir / "sparse.jsonl"
        file.write_text('{"a": 1}\n\n{"b": 2}\n\n')
        reader = JsonlFileReader(str(file))
        records = list(reader.read_records())
        assert len(records) == 2
