"""Unit tests for JSON transformer."""

import json

from durable_engine.ingestion.record import Record
from durable_engine.transformation.json_transformer import JsonTransformer


class TestJsonTransformer:
    def test_transform_produces_valid_json(self, sample_record: Record) -> None:
        transformer = JsonTransformer()
        result = transformer.transform(sample_record)
        parsed = json.loads(result)
        assert parsed["id"] == sample_record.record_id
        assert parsed["data"]["name"] == "Alice Johnson"

    def test_transform_includes_metadata(self, sample_record: Record) -> None:
        transformer = JsonTransformer()
        result = transformer.transform(sample_record)
        parsed = json.loads(result)
        assert "metadata" in parsed
        assert parsed["metadata"]["source_file"] == "test.csv"
        assert parsed["metadata"]["line_number"] == 1

    def test_name_and_content_type(self) -> None:
        t = JsonTransformer()
        assert t.name == "json"
        assert t.content_type == "application/json"

    def test_transform_batch(self, sample_records: list[Record]) -> None:
        transformer = JsonTransformer()
        results = transformer.transform_batch(sample_records)
        assert len(results) == 10
        for r in results:
            json.loads(r)  # All must be valid JSON
