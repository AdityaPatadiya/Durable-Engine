"""Unit tests for Protobuf transformer."""

from durable_engine.ingestion.record import Record
from durable_engine.transformation.protobuf_transformer import ProtobufTransformer


class TestProtobufTransformer:
    def test_transform_produces_bytes(self, sample_record: Record) -> None:
        transformer = ProtobufTransformer()
        result = transformer.transform(sample_record)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_name_and_content_type(self) -> None:
        t = ProtobufTransformer()
        assert t.name == "protobuf"
        assert t.content_type == "application/x-protobuf"

    def test_transform_different_records_produce_different_bytes(self) -> None:
        t = ProtobufTransformer()
        r1 = Record.from_dict({"name": "Alice"}, "f1", 1)
        r2 = Record.from_dict({"name": "Bob"}, "f2", 2)
        assert t.transform(r1) != t.transform(r2)

    def test_transform_contains_field_data(self, sample_record: Record) -> None:
        t = ProtobufTransformer()
        result = t.transform(sample_record)
        # The field values should be present as UTF-8 in the binary
        assert b"Alice Johnson" in result
