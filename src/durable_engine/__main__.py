"""CLI entrypoint for the Durable Engine."""

import asyncio
import signal
import sys
from pathlib import Path

import click
import structlog

from durable_engine.app import DurableEngine
from durable_engine.config.loader import load_config
from durable_engine.observability.structured_log import setup_logging

logger = structlog.get_logger()


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    default="config/default.yaml",
    help="Path to the configuration YAML file.",
)
@click.option(
    "--input",
    "input_file",
    type=click.Path(path_type=Path),
    default=None,
    help="Override the input file path from config.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default=None,
    help="Override log level.",
)
@click.version_option(version="1.0.0", prog_name="durable-engine")
def main(config_path: Path, input_file: Path | None, log_level: str | None) -> None:
    """Durable Engine - High-Throughput Data Fan-Out & Transformation Engine."""
    config = load_config(config_path)

    if input_file:
        config.ingestion.file_path = str(input_file)
    if log_level:
        config.observability.logging.level = log_level.upper()

    setup_logging(config.observability.logging)

    logger.info(
        "starting_engine",
        config_file=str(config_path),
        input_file=config.ingestion.file_path,
    )

    engine = DurableEngine(config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal(sig: int, _frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signal.Signals(sig).name)
        loop.call_soon_threadsafe(engine.request_shutdown)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        exit_code = loop.run_until_complete(engine.run())
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
        loop.run_until_complete(engine.shutdown())
        exit_code = 1
    finally:
        loop.close()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
