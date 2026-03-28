"""File detection and utility functions."""

import os
from pathlib import Path


def get_file_size_mb(file_path: str) -> float:
    """Get file size in megabytes."""
    return os.path.getsize(file_path) / (1024 * 1024)


def detect_encoding(file_path: str, sample_size: int = 8192) -> str:
    """Simple encoding detection by reading a sample of the file."""
    with open(file_path, "rb") as f:
        sample = f.read(sample_size)

    # Check for BOM
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if sample.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if sample.startswith(b"\xfe\xff"):
        return "utf-16-be"

    # Try UTF-8
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # Fallback to latin-1 (always succeeds)
    return "latin-1"


def estimate_line_count(file_path: str, sample_lines: int = 1000) -> int | None:
    """Estimate total lines in a file by sampling."""
    path = Path(file_path)
    if not path.exists():
        return None

    file_size = path.stat().st_size
    if file_size == 0:
        return 0

    sample_bytes = 0
    line_count = 0

    with open(file_path, "rb") as f:
        for line in f:
            sample_bytes += len(line)
            line_count += 1
            if line_count >= sample_lines:
                break

    if line_count == 0:
        return 0

    avg_line_size = sample_bytes / line_count
    estimated_total = int(file_size / avg_line_size)
    return estimated_total


def validate_file(file_path: str) -> list[str]:
    """Validate that a file exists and is readable. Returns list of errors."""
    errors: list[str] = []
    path = Path(file_path)

    if not path.exists():
        errors.append(f"File not found: {file_path}")
    elif not path.is_file():
        errors.append(f"Not a regular file: {file_path}")
    elif not os.access(file_path, os.R_OK):
        errors.append(f"File not readable: {file_path}")

    return errors
