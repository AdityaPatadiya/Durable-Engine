"""PostgreSQL CDC (Change Data Capture) source — real-time DB change streaming."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from typing import Any

import structlog

from durable_engine.ingestion.base import RecordSource
from durable_engine.ingestion.record import Record

logger = structlog.get_logger()

# Pattern to parse test_decoding output like:
# table public.users: INSERT: id[integer]:1 name[text]:'Alice' email[text]:'alice@example.com'
_CHANGE_PATTERN = re.compile(
    r"table (?P<schema>\w+)\.(?P<table>\w+): (?P<operation>\w+): (?P<columns>.*)"
)
_COLUMN_PATTERN = re.compile(r"(?P<name>\w+)\[(?P<type>\w+)\]:(?P<value>'[^']*'|\S+)")


def _parse_test_decoding_output(data: str) -> dict[str, object]:
    """Parse the text output from test_decoding plugin into a dict."""
    match = _CHANGE_PATTERN.match(data)
    if not match:
        # BEGIN/COMMIT or unrecognized lines
        return {
            "__cdc_raw": data,
            "__cdc_operation": data.split(":")[0].strip() if ":" in data else "unknown",
        }

    result: dict[str, object] = {
        "__cdc_schema": match.group("schema"),
        "__cdc_table": match.group("table"),
        "__cdc_operation": match.group("operation"),
    }

    columns_str = match.group("columns")
    for col_match in _COLUMN_PATTERN.finditer(columns_str):
        name = col_match.group("name")
        value = col_match.group("value")
        # Strip surrounding quotes from string values
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        result[name] = value

    return result


class PostgresCdcSource(RecordSource):
    """Streams real-time changes from PostgreSQL using logical replication.

    Requires: pip install psycopg[binary] (psycopg3)

    Uses the built-in `test_decoding` output plugin (no extra extensions needed).

    Prerequisites on PostgreSQL:
    1. postgresql.conf: wal_level = logical
    2. Create a publication: CREATE PUBLICATION my_pub FOR TABLE my_table;
    3. The replication slot is created automatically on first run.

    Use case: Capture INSERT/UPDATE/DELETE changes from a PostgreSQL table
    and fan-out to all downstream sinks in real-time.
    """

    def __init__(
        self,
        dsn: str,
        publication: str = "durable_engine_pub",
        slot_name: str = "durable_engine_slot",
        tables: list[str] | None = None,
        poll_interval: float = 0.5,
    ) -> None:
        super().__init__()
        self._dsn = dsn
        self._publication = publication
        self._slot_name = slot_name
        self._tables = tables or []
        self._poll_interval = poll_interval
        self._conn: Any = None

    @property
    def source_name(self) -> str:
        return f"postgres-cdc://{self._publication}"

    @property
    def is_streaming(self) -> bool:
        return True

    async def setup(self) -> None:
        import psycopg

        self._conn = await psycopg.AsyncConnection.connect(
            self._dsn,
            autocommit=True,
        )

        # Create replication slot with test_decoding plugin if it doesn't exist
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    [self._slot_name],
                )
                if await cur.fetchone() is None:
                    await cur.execute(
                        "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')",
                        [self._slot_name],
                    )
                    logger.info(
                        "cdc_replication_slot_created", slot=self._slot_name, plugin="test_decoding"
                    )
                else:
                    logger.info("cdc_replication_slot_exists", slot=self._slot_name)
        except Exception as e:
            logger.warning("cdc_slot_setup_note", msg=str(e))

        logger.info(
            "postgres_cdc_connected",
            publication=self._publication,
            slot=self._slot_name,
        )

    async def read_records_async(self) -> AsyncIterator[Record]:
        """Poll for changes from the logical replication slot."""
        while True:
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(
                        "SELECT lsn, xid, data FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                        [self._slot_name],
                    )

                    rows = await cur.fetchall()
                    if not rows:
                        await asyncio.sleep(self._poll_interval)
                        continue

                    for lsn, xid, data in rows:
                        # Skip BEGIN/COMMIT transaction markers
                        if isinstance(data, str) and data.startswith(("BEGIN", "COMMIT")):
                            continue

                        # Filter by table if tables list is configured
                        change_data = _parse_test_decoding_output(data)
                        table_name = change_data.get("__cdc_table")

                        if self._tables and table_name not in self._tables:
                            continue

                        change_data["__cdc_lsn"] = str(lsn)
                        change_data["__cdc_xid"] = str(xid)

                        table = table_name or "unknown"
                        record = Record.from_dict(
                            data=change_data,
                            source_file=f"postgres-cdc://{self._publication}/{table}",
                            line_number=self._records_read + 1,
                        )
                        self._records_read += 1
                        yield record

            except Exception as e:
                logger.error("cdc_read_error", error=str(e))
                await asyncio.sleep(2)

    async def teardown(self) -> None:
        if self._conn is not None:
            await self._conn.close()
        logger.info("postgres_cdc_disconnected")
