"""Internal canonical Record representation."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field


@dataclass(slots=True)
class Record:
    """Canonical internal representation of a single data record."""

    record_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: dict[str, object] = field(default_factory=dict)
    source_file: str = ""
    line_number: int = 0
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_dict(
        cls, data: dict[str, object], source_file: str = "", line_number: int = 0
    ) -> Record:
        return cls(
            data=data,
            source_file=source_file,
            line_number=line_number,
        )

    def get(self, key: str, default: object = None) -> object:
        return self.data.get(key, default)
