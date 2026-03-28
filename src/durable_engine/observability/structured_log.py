"""Structured JSON logging setup using structlog."""

import logging
import sys
from pathlib import Path

import structlog

from durable_engine.config.models import LoggingConfig


def setup_logging(config: LoggingConfig) -> None:
    """Configure structured logging for the application."""
    log_level = getattr(logging, config.level.upper(), logging.INFO)

    # Ensure log output directory exists
    log_file = Path(config.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Configure standard logging
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if config.file:
        file_handler = logging.FileHandler(config.file, encoding="utf-8")
        handlers.append(file_handler)

    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        handlers=handlers,
        force=True,
    )

    # Configure structlog
    if config.format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
