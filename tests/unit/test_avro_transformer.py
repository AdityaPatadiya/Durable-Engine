"""Unit tests for Avro transformer."""

import io

import fastavro

from durable_engine.ingestion.record import Record
from durable_engine.transformation.avro_transformer import (
    PARSED_SCHEMA,
    AvroTransformer,
    CqlMapTransformer,
)


class TestAvroTransformer:
    def test_transform_produces_bytes(self, sample_record: Record) -> None:
        transformer = AvroTransformer()
        result = transformer.transform(sample_record)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_transform_is_decodable(self, sample_record: Record) -> None:
        transformer = AvroTransformer()
        result = transformer.transform(sample_record)
        buf = io.BytesIO(result)
        decoded = fastavro.schemaless_reader(buf, PARSED_SCHEMA)
        assert decoded["id"] == sample_record.record_id
        assert decoded["fields"]["name"] == "Alice Johnson"

    def test_name_and_content_type(self) -> None:
        t = AvroTransformer()
        assert t.name == "avro"
        assert t.content_type == "application/avro"


class TestCqlMapTransformer:
    def test_transform_produces_json_map(self, sample_record: Record) -> None:
        import json

        transformer = CqlMapTransformer()
        result = transformer.transform(sample_record)
        parsed = json.loads(result)
        assert parsed["name"] == "Alice Johnson"
        assert "__record_id" in parsed

    def test_name(self) -> None:
        assert CqlMapTransformer().name == "cql_map"
