"""JSON transformer for REST API sinks."""

import json

from durable_engine.ingestion.record import Record
from durable_engine.transformation.base import Transformer


class JsonTransformer(Transformer):
    """Transforms records into JSON bytes for REST API consumption."""

    @property
    def name(self) -> str:
        return "json"

    @property
    def content_type(self) -> str:
        return "application/json"

    def transform(self, record: Record) -> bytes:
        payload = {
            "id": record.record_id,
            "data": record.data,
            "metadata": {
                "source_file": record.source_file,
                "line_number": record.line_number,
                "timestamp": record.timestamp,
            },
        }
        return json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")

    def transform_batch(self, records: list[Record]) -> list[bytes]:
        return [self.transform(r) for r in records]
