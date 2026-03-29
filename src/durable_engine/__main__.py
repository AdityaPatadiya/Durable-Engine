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


@click.group(invoke_without_command=True)
@click.pass_context
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
def main(ctx: click.Context, config_path: Path, input_file: Path | None, log_level: str | None) -> None:
    """Durable Engine - High-Throughput Data Fan-Out & Transformation Engine."""
    if ctx.invoked_subcommand is not None:
        return

    config = load_config(config_path)

    if input_file:
        config.ingestion.file_path = str(input_file)
    if log_level:
        config.observability.logging.level = log_level.upper()

    setup_logging(config.observability.logging)

    logger.info(
        "starting_engine",
        config_file=str(config_path),
        source_type=config.ingestion.source_type,
    )

    engine = DurableEngine(config)

    async def _run() -> int:
        loop = asyncio.get_event_loop()

        def _handle_signal() -> None:
            logger.info("shutdown_signal_received")
            engine.request_shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        return await engine.run()

    try:
        exit_code = asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt")
        exit_code = 1

    sys.exit(exit_code)


# Register DLQ replay as a subcommand
from durable_engine.resilience.dlq_replay import replay_dlq

main.add_command(replay_dlq, name="dlq-replay")

if __name__ == "__main__":
    main()
