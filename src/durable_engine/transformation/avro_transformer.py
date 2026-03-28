"""Avro / CQL Map transformer for wide-column DB sinks."""

import io
import json

import fastavro

from durable_engine.ingestion.record import Record
from durable_engine.transformation.base import Transformer

AVRO_SCHEMA = {
    "type": "record",
    "name": "DataRecord",
    "namespace": "com.durable_engine",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "fields", "type": {"type": "map", "values": "string"}},
        {"name": "timestamp", "type": "long"},
        {"name": "source_file", "type": "string"},
        {"name": "line_number", "type": "long"},
    ],
}

PARSED_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA)


class AvroTransformer(Transformer):
    """Transforms records into Avro binary format for wide-column DB sinks."""

    @property
    def name(self) -> str:
        return "avro"

    @property
    def content_type(self) -> str:
        return "application/avro"

    def transform(self, record: Record) -> bytes:
        avro_record = {
            "id": record.record_id,
            "fields": {str(k): str(v) for k, v in record.data.items()},
            "timestamp": int(record.timestamp),
            "source_file": record.source_file,
            "line_number": record.line_number,
        }

        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, PARSED_SCHEMA, avro_record)
        return buf.getvalue()

    def transform_batch(self, records: list[Record]) -> list[bytes]:
        return [self.transform(r) for r in records]


class CqlMapTransformer(Transformer):
    """Transforms records into CQL Map<string, string> format (JSON-encoded map)."""

    @property
    def name(self) -> str:
        return "cql_map"

    @property
    def content_type(self) -> str:
        return "application/json"

    def transform(self, record: Record) -> bytes:
        cql_map = {str(k): str(v) for k, v in record.data.items()}
        cql_map["__record_id"] = record.record_id
        cql_map["__source_file"] = record.source_file
        cql_map["__line_number"] = str(record.line_number)
        cql_map["__timestamp"] = str(int(record.timestamp))
        return json.dumps(cql_map, ensure_ascii=False).encode("utf-8")
