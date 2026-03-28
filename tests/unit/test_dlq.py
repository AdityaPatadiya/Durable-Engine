"""Unit tests for Dead Letter Queue."""

import json
from pathlib import Path

from durable_engine.config.models import DlqConfig
from durable_engine.ingestion.record import Record
from durable_engine.resilience.dlq import DeadLetterQueue


class TestDeadLetterQueue:
    def test_write_creates_file(self, tmp_dir: Path) -> None:
        config = DlqConfig(output_dir=str(tmp_dir))
        dlq = DeadLetterQueue(config)
        record = Record.from_dict({"key": "value"}, "test.csv", 1)

        dlq.write(record, "rest_api", "HTTP 500", attempts=3)

        dlq_file = tmp_dir / "rest_api.jsonl"
        assert dlq_file.exists()

    def test_write_produces_valid_jsonl(self, tmp_dir: Path) -> None:
        config = DlqConfig(output_dir=str(tmp_dir))
        dlq = DeadLetterQueue(config)
        record = Record.from_dict({"name": "Alice"}, "test.csv", 5)

        dlq.write(record, "grpc", "UNAVAILABLE", attempts=3)

        dlq_file = tmp_dir / "grpc.jsonl"
        line = dlq_file.read_text().strip()
        entry = json.loads(line)
        assert entry["data"]["name"] == "Alice"
        assert entry["error_metadata"]["error"] == "UNAVAILABLE"
        assert entry["error_metadata"]["attempts"] == 3

    def test_counts_per_sink(self, tmp_dir: Path) -> None:
        config = DlqConfig(output_dir=str(tmp_dir))
        dlq = DeadLetterQueue(config)

        for i in range(5):
            dlq.write(Record.from_dict({}, "f", i), "rest", "err", 3)
        for i in range(3):
            dlq.write(Record.from_dict({}, "f", i), "grpc", "err", 3)

        counts = dlq.get_counts()
        assert counts["rest"] == 5
        assert counts["grpc"] == 3
        assert dlq.get_total_count() == 8

    def test_multiple_writes_append(self, tmp_dir: Path) -> None:
        config = DlqConfig(output_dir=str(tmp_dir))
        dlq = DeadLetterQueue(config)

        dlq.write(Record.from_dict({"a": 1}, "f", 1), "sink1", "err1", 1)
        dlq.write(Record.from_dict({"b": 2}, "f", 2), "sink1", "err2", 2)

        dlq_file = tmp_dir / "sink1.jsonl"
        lines = dlq_file.read_text().strip().split("\n")
        assert len(lines) == 2
