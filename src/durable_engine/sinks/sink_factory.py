"""Factory for creating sinks from configuration."""

import structlog

from durable_engine.config.models import SinkConfig
from durable_engine.observability.metrics import MetricsCollector
from durable_engine.sinks.base import BaseSink
from durable_engine.sinks.grpc_sink import GrpcSink
from durable_engine.sinks.mq_sink import MessageQueueSink
from durable_engine.sinks.rest_sink import RestApiSink
from durable_engine.sinks.widecolumn_sink import WideColumnDbSink

logger = structlog.get_logger()

_SINK_TYPE_MAP: dict[str, type[BaseSink]] = {
    "rest": RestApiSink,
    "grpc": GrpcSink,
    "mq": MessageQueueSink,
    "widecolumn": WideColumnDbSink,
}


class SinkFactory:
    """Creates sink instances from configuration."""

    @staticmethod
    def create(name: str, config: SinkConfig) -> BaseSink:
        sink_class = _SINK_TYPE_MAP.get(config.type)
        if sink_class is None:
            available = ", ".join(sorted(_SINK_TYPE_MAP.keys()))
            raise ValueError(f"Unknown sink type '{config.type}'. Available: {available}")

        return sink_class(name=name, config=config)

    @staticmethod
    def create_all(
        sinks_config: dict[str, SinkConfig],
        metrics: MetricsCollector,
    ) -> dict[str, BaseSink]:
        sinks: dict[str, BaseSink] = {}

        for name, config in sinks_config.items():
            if not config.enabled:
                logger.info("sink_disabled", sink=name)
                continue

            sink = SinkFactory.create(name, config)
            sinks[name] = sink
            metrics.register_sink(name)
            logger.info("sink_created", sink=name, type=config.type)

        return sinks

    @staticmethod
    def register_type(type_name: str, sink_class: type[BaseSink]) -> None:
        """Register a custom sink type for extensibility."""
        _SINK_TYPE_MAP[type_name] = sink_class
