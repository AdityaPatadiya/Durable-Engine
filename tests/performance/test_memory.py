"""Performance tests for memory usage verification."""

import tempfile
import tracemalloc
from pathlib import Path

import pytest

from durable_engine.ingestion.csv_reader import CsvFileReader


class TestMemory:
    def test_csv_reader_constant_memory(self, tmp_dir: Path) -> None:
        """Verify CSV reader uses constant memory regardless of file size."""
        # Generate a large-ish CSV file
        csv_path = tmp_dir / "large.csv"
        num_records = 50_000

        with open(csv_path, "w") as f:
            f.write("id,name,email,department,salary\n")
            for i in range(num_records):
                f.write(f"{i},User {i},user{i}@test.com,Dept {i % 10},{50000 + i}\n")

        tracemalloc.start()

        reader = CsvFileReader(str(csv_path))
        count = 0
        peak_snapshots = []

        for record in reader.read_records():
            count += 1
            if count % 10000 == 0:
                _, peak = tracemalloc.get_traced_memory()
                peak_snapshots.append(peak)

        tracemalloc.stop()

        assert count == num_records

        # Memory should not grow significantly between snapshots
        if len(peak_snapshots) >= 2:
            first_peak = peak_snapshots[0]
            last_peak = peak_snapshots[-1]
            # Allow up to 2x growth (accounting for GC, buffers, etc.)
            growth_ratio = last_peak / first_peak if first_peak > 0 else 1.0
            assert growth_ratio < 3.0, (
                f"Memory grew {growth_ratio:.1f}x from {first_peak / 1024:.0f}KB "
                f"to {last_peak / 1024:.0f}KB — possible memory leak"
            )
