"""Protobuf transformer for gRPC sinks."""

import struct

from durable_engine.ingestion.record import Record
from durable_engine.transformation.base import Transformer


class ProtobufTransformer(Transformer):
    """Transforms records into Protobuf-compatible binary format.

    Uses a manual wire-format encoding to avoid requiring compiled .proto files
    at runtime. For production use with actual gRPC services, replace with
    generated protobuf message classes.
    """

    @property
    def name(self) -> str:
        return "protobuf"

    @property
    def content_type(self) -> str:
        return "application/x-protobuf"

    def _encode_varint(self, value: int) -> bytes:
        """Encode an integer as a protobuf varint."""
        parts = []
        while value > 0x7F:
            parts.append((value & 0x7F) | 0x80)
            value >>= 7
        parts.append(value & 0x7F)
        return bytes(parts)

    def _encode_string_field(self, field_number: int, value: str) -> bytes:
        """Encode a string as a length-delimited protobuf field."""
        encoded = value.encode("utf-8")
        tag = self._encode_varint((field_number << 3) | 2)
        length = self._encode_varint(len(encoded))
        return tag + length + encoded

    def _encode_int64_field(self, field_number: int, value: int) -> bytes:
        """Encode an int64 as a varint protobuf field."""
        tag = self._encode_varint((field_number << 3) | 0)
        return tag + self._encode_varint(value)

    def _encode_map_entry(self, key: str, value: str) -> bytes:
        """Encode a single map<string, string> entry."""
        key_bytes = self._encode_string_field(1, key)
        value_bytes = self._encode_string_field(2, value)
        entry = key_bytes + value_bytes
        return entry

    def transform(self, record: Record) -> bytes:
        """Encode record matching the proto schema:
        field 1: string id
        field 2: map<string, string> fields
        field 3: int64 timestamp
        field 4: string source_file
        field 5: int64 line_number
        """
        parts: list[bytes] = []

        # Field 1: id
        parts.append(self._encode_string_field(1, record.record_id))

        # Field 2: map entries (each is a length-delimited message on field 2)
        for key, value in record.data.items():
            entry = self._encode_map_entry(str(key), str(value))
            tag = self._encode_varint((2 << 3) | 2)
            length = self._encode_varint(len(entry))
            parts.append(tag + length + entry)

        # Field 3: timestamp
        parts.append(self._encode_int64_field(3, int(record.timestamp)))

        # Field 4: source_file
        parts.append(self._encode_string_field(4, record.source_file))

        # Field 5: line_number
        parts.append(self._encode_int64_field(5, record.line_number))

        return b"".join(parts)
