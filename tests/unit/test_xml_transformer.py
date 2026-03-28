"""Unit tests for XML transformer."""

from xml.etree.ElementTree import fromstring

from durable_engine.ingestion.record import Record
from durable_engine.transformation.xml_transformer import XmlTransformer


class TestXmlTransformer:
    def test_transform_produces_valid_xml(self, sample_record: Record) -> None:
        transformer = XmlTransformer()
        result = transformer.transform(sample_record)
        root = fromstring(result)
        assert root.tag == "record"

    def test_transform_includes_data_fields(self, sample_record: Record) -> None:
        transformer = XmlTransformer()
        result = transformer.transform(sample_record)
        root = fromstring(result)
        data = root.find("data")
        assert data is not None
        fields = {f.get("name"): f.text for f in data.findall("field")}
        assert fields["name"] == "Alice Johnson"

    def test_transform_includes_metadata(self, sample_record: Record) -> None:
        transformer = XmlTransformer()
        result = transformer.transform(sample_record)
        root = fromstring(result)
        metadata = root.find("metadata")
        assert metadata is not None
        assert metadata.find("source_file").text == "test.csv"

    def test_name_and_content_type(self) -> None:
        t = XmlTransformer()
        assert t.name == "xml"
        assert t.content_type == "application/xml"

    def test_custom_root_tag(self, sample_record: Record) -> None:
        transformer = XmlTransformer(root_tag="entry")
        result = transformer.transform(sample_record)
        root = fromstring(result)
        assert root.tag == "entry"
