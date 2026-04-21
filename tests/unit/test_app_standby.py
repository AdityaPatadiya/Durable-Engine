"""Unit tests for standby mode behavior in DurableEngine."""

import asyncio
import unittest
from unittest.mock import patch

from durable_engine.app import DurableEngine
from durable_engine.config.models import EngineConfig


class TestDurableEngineStandby(unittest.IsolatedAsyncioTestCase):
    async def test_standby_mode_skips_source_startup(self) -> None:
        config = EngineConfig.model_validate(
            {
                "engine": {
                    "name": "standby-test",
                    "batch_size": 100,
                    "max_queue_size": 1000,
                    "worker_count": 1,
                    "shutdown_timeout_seconds": 5,
                },
                "ingestion": {
                    "enabled": False,
                    "file_path": "samples/sample.csv",
                },
                "sinks": {},
                "observability": {
                    "reporter": {
                        "interval_seconds": 60,
                        "format": "json",
                    },
                    "logging": {
                        "level": "INFO",
                        "format": "json",
                        "file": "output/engine.log",
                    },
                    "health": {
                        "enabled": False,
                    },
                    "metrics": {
                        "enabled": False,
                    },
                },
            }
        )

        def _fail_if_called(*args, **kwargs):
            raise AssertionError("ReaderFactory.create should not be called in standby mode")

        with patch("durable_engine.app.ReaderFactory.create", side_effect=_fail_if_called):
            engine = DurableEngine(config)
            run_task = asyncio.create_task(engine.run())

            # Let the run loop enter standby waiting state, then request shutdown.
            await asyncio.sleep(0)
            engine.request_shutdown()

            exit_code = await asyncio.wait_for(run_task, timeout=2)

        self.assertEqual(exit_code, 0)
