"""Record validation — validates and sanitizes records before transformation.

Catches malformed data early, before it reaches transformers or sinks.
Invalid records are sent to the DLQ instead of crashing the pipeline.
"""

import re
from dataclasses import dataclass

import structlog

from durable_engine.ingestion.record import Record

logger = structlog.get_logger()

# Max field value length (prevents huge payloads)
MAX_FIELD_VALUE_LENGTH = 1_000_000  # 1MB per field

# Pattern for detecting potential injection attempts
_SCRIPT_PATTERN = re.compile(r"<script[^>]*>", re.IGNORECASE)
_SQL_INJECTION_PATTERN = re.compile(
    r"(\b(union|select|insert|update|delete|drop|alter|exec|execute)\b.*\b(from|into|table|where)\b)",
    re.IGNORECASE,
)


@dataclass
class ValidationResult:
    """Result of validating a record."""
    valid: bool
    errors: list[str]
    sanitized_record: Record | None = None


class RecordValidator:
    """Validates and optionally sanitizes records."""

    def __init__(
        self,
        required_fields: list[str] | None = None,
        max_field_length: int = MAX_FIELD_VALUE_LENGTH,
        strip_whitespace: bool = True,
        reject_empty_records: bool = True,
        sanitize_html: bool = True,
        check_sql_injection: bool = False,
    ) -> None:
        self._required_fields = required_fields or []
        self._max_field_length = max_field_length
        self._strip_whitespace = strip_whitespace
        self._reject_empty = reject_empty_records
        self._sanitize_html = sanitize_html
        self._check_sql_injection = check_sql_injection

    def validate(self, record: Record) -> ValidationResult:
        """Validate a single record. Returns ValidationResult."""
        errors: list[str] = []

        # Check for empty data
        if self._reject_empty and not record.data:
            errors.append("Record has no data fields")
            return ValidationResult(valid=False, errors=errors)

        # Check required fields
        for field in self._required_fields:
            if field not in record.data:
                errors.append(f"Missing required field: '{field}'")
            elif record.data[field] is None or str(record.data[field]).strip() == "":
                errors.append(f"Required field '{field}' is empty")

        # Validate and sanitize field values
        sanitized_data: dict[str, object] = {}
        for key, value in record.data.items():
            str_val = str(value) if value is not None else ""

            # Check field value length
            if len(str_val) > self._max_field_length:
                errors.append(
                    f"Field '{key}' exceeds max length ({len(str_val)} > {self._max_field_length})"
                )
                continue

            # Strip whitespace
            if self._strip_whitespace and isinstance(value, str):
                str_val = str_val.strip()

            # Check for HTML/script injection
            if self._sanitize_html and isinstance(value, str):
                if _SCRIPT_PATTERN.search(str_val):
                    errors.append(f"Field '{key}' contains potential script injection")
                    str_val = _SCRIPT_PATTERN.sub("[REMOVED]", str_val)

            # Check for SQL injection patterns
            if self._check_sql_injection and isinstance(value, str):
                if _SQL_INJECTION_PATTERN.search(str_val):
                    logger.warning(
                        "potential_sql_injection",
                        field=key,
                        record_id=record.record_id,
                    )

            sanitized_data[key] = str_val if isinstance(value, str) else value

        if errors:
            return ValidationResult(valid=False, errors=errors)

        # Create sanitized record
        sanitized = Record.from_dict(
            data=sanitized_data,
            source_file=record.source_file,
            line_number=record.line_number,
        )
        sanitized.record_id = record.record_id
        sanitized.timestamp = record.timestamp

        return ValidationResult(valid=True, errors=[], sanitized_record=sanitized)

    def validate_batch(self, records: list[Record]) -> tuple[list[Record], list[tuple[Record, list[str]]]]:
        """Validate a batch. Returns (valid_records, [(invalid_record, errors)])."""
        valid: list[Record] = []
        invalid: list[tuple[Record, list[str]]] = []

        for record in records:
            result = self.validate(record)
            if result.valid and result.sanitized_record:
                valid.append(result.sanitized_record)
            else:
                invalid.append((record, result.errors))

        return valid, invalid
