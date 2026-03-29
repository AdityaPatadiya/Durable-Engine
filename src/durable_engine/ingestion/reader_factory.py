"""Factory for creating record sources based on config."""

from pathlib import Path

from durable_engine.config.models import IngestionConfig
from durable_engine.ingestion.base import FileReader, RecordSource
from durable_engine.ingestion.csv_reader import CsvFileReader
from durable_engine.ingestion.fixed_width import FixedWidthFileReader
from durable_engine.ingestion.jsonl_reader import JsonlFileReader

_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".jsonl": "jsonl",
    ".json": "jsonl",
    ".txt": "fixed_width",
    ".dat": "fixed_width",
    ".fw": "fixed_width",
}


class ReaderFactory:
    """Creates the appropriate RecordSource based on configuration."""

    @staticmethod
    def create(config: IngestionConfig) -> RecordSource:
        source_type = config.source_type

        if source_type == "file":
            return ReaderFactory._create_file_reader(config)
        elif source_type == "kafka":
            return ReaderFactory._create_kafka_source(config)
        elif source_type == "webhook":
            return ReaderFactory._create_webhook_source(config)
        elif source_type == "postgres_cdc":
            return ReaderFactory._create_postgres_cdc_source(config)
        elif source_type == "websocket":
            return ReaderFactory._create_websocket_source(config)
        else:
            raise ValueError(
                f"Unsupported source type: '{source_type}'. "
                f"Available: file, kafka, webhook, postgres_cdc, websocket"
            )

    @staticmethod
    def _create_file_reader(config: IngestionConfig) -> FileReader:
        file_format = config.file_format

        if file_format == "auto":
            ext = Path(config.file_path).suffix.lower()
            file_format = _EXTENSION_MAP.get(ext)
            if file_format is None:
                raise ValueError(
                    f"Cannot auto-detect format for extension '{ext}'. "
                    f"Set file_format explicitly in config."
                )

        if file_format == "csv":
            return CsvFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
                csv_config=config.csv,
            )
        elif file_format == "jsonl":
            return JsonlFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
            )
        elif file_format == "fixed_width":
            return FixedWidthFileReader(
                file_path=config.file_path,
                encoding=config.encoding,
                fw_config=config.fixed_width,
            )
        else:
            raise ValueError(f"Unsupported file format: '{file_format}'")

    @staticmethod
    def _create_kafka_source(config: IngestionConfig) -> RecordSource:
        from durable_engine.ingestion.kafka_source import KafkaSource

        kc = config.kafka
        return KafkaSource(
            brokers=kc.brokers,
            topic=kc.topic,
            group_id=kc.group_id,
            data_format=kc.data_format,
            auto_offset_reset=kc.auto_offset_reset,
            auth_username=kc.auth_username,
            auth_password=kc.auth_password,
            tls_enabled=kc.tls_enabled,
            tls_ca_path=kc.tls_ca_path,
        )

    @staticmethod
    def _create_webhook_source(config: IngestionConfig) -> RecordSource:
        from durable_engine.ingestion.webhook_source import WebhookSource

        wc = config.webhook
        return WebhookSource(
            host=wc.host,
            port=wc.port,
            path=wc.path,
            auth_token=wc.auth_token,
            max_queue_size=wc.max_queue_size,
        )

    @staticmethod
    def _create_postgres_cdc_source(config: IngestionConfig) -> RecordSource:
        from durable_engine.ingestion.postgres_cdc_source import PostgresCdcSource

        pc = config.postgres_cdc
        return PostgresCdcSource(
            dsn=pc.dsn,
            publication=pc.publication,
            slot_name=pc.slot_name,
            tables=pc.tables,
        )

    @staticmethod
    def _create_websocket_source(config: IngestionConfig) -> RecordSource:
        from durable_engine.ingestion.websocket_source import WebSocketSource

        ws = config.websocket
        return WebSocketSource(
            url=ws.url,
            auth_token=ws.auth_token,
            headers=ws.headers,
            reconnect_delay=ws.reconnect_delay,
            ping_interval=ws.ping_interval,
            subscribe_message=ws.subscribe_message,
        )
