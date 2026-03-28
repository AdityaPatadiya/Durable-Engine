"""XML transformer for Message Queue sinks."""

from xml.etree.ElementTree import Element, SubElement, tostring

from durable_engine.ingestion.record import Record
from durable_engine.transformation.base import Transformer


class XmlTransformer(Transformer):
    """Transforms records into XML bytes for legacy message queue systems."""

    def __init__(self, root_tag: str = "record", encoding: str = "utf-8") -> None:
        self._root_tag = root_tag
        self._encoding = encoding

    @property
    def name(self) -> str:
        return "xml"

    @property
    def content_type(self) -> str:
        return "application/xml"

    def transform(self, record: Record) -> bytes:
        root = Element(self._root_tag)
        root.set("id", record.record_id)

        metadata = SubElement(root, "metadata")
        SubElement(metadata, "source_file").text = record.source_file
        SubElement(metadata, "line_number").text = str(record.line_number)
        SubElement(metadata, "timestamp").text = str(record.timestamp)

        data_elem = SubElement(root, "data")
        for key, value in record.data.items():
            field_elem = SubElement(data_elem, "field")
            field_elem.set("name", str(key))
            field_elem.text = str(value) if value is not None else ""

        return tostring(root, encoding="unicode").encode(self._encoding)
