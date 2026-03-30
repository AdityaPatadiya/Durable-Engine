"""Registry pattern for managing transformers by name."""

from durable_engine.transformation.avro_transformer import AvroTransformer
from durable_engine.transformation.base import Transformer
from durable_engine.transformation.json_transformer import JsonTransformer
from durable_engine.transformation.protobuf_transformer import ProtobufTransformer
from durable_engine.transformation.xml_transformer import XmlTransformer


class TransformerRegistry:
    """Central registry mapping transformer names to instances.

    Pre-registers all built-in transformers. Extensible via register().
    """

    def __init__(self) -> None:
        self._transformers: dict[str, Transformer] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register(JsonTransformer())
        self.register(ProtobufTransformer())
        self.register(XmlTransformer())
        self.register(AvroTransformer())

    def register(self, transformer: Transformer) -> None:
        self._transformers[transformer.name] = transformer

    def get(self, name: str) -> Transformer:
        if name not in self._transformers:
            available = ", ".join(sorted(self._transformers.keys()))
            raise KeyError(f"Unknown transformer '{name}'. Available: {available}")
        return self._transformers[name]

    def list_transformers(self) -> list[str]:
        return sorted(self._transformers.keys())
