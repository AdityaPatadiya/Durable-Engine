"""Abstract base class for transformers (Strategy Pattern interface)."""

from abc import ABC, abstractmethod

from durable_engine.ingestion.record import Record


class Transformer(ABC):
    """Strategy interface for record transformation.

    Each sink requires data in a different format. Transformers convert
    the canonical Record into the target format (JSON, Protobuf, XML, Avro).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this transformer (e.g., 'json', 'protobuf')."""
        ...

    @property
    @abstractmethod
    def content_type(self) -> str:
        """MIME type of the output format."""
        ...

    @abstractmethod
    def transform(self, record: Record) -> bytes:
        """Transform a single record into the target format bytes."""
        ...

    def transform_batch(self, records: list[Record]) -> list[bytes]:
        """Transform a batch of records. Override for batch-optimized implementations."""
        return [self.transform(r) for r in records]
