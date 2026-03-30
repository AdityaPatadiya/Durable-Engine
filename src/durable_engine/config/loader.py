"""YAML config loader with environment variable interpolation."""

import os
import re
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from durable_engine.config.models import EngineConfig

ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")


def _interpolate_env_vars(value: str) -> str:
    """Replace ${VAR} or ${VAR:default} with environment variable values."""

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group(2)
        env_value = os.environ.get(var_name, default)
        if env_value is None:
            raise ValueError(f"Environment variable '{var_name}' is not set and has no default")
        return env_value

    return ENV_VAR_PATTERN.sub(_replace, value)


def _walk_and_interpolate(obj: object) -> object:
    """Recursively walk a config dict and interpolate env vars in string values."""
    if isinstance(obj, str):
        return _interpolate_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _walk_and_interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_interpolate(item) for item in obj]
    return obj


def load_config(config_path: Path) -> EngineConfig:
    """Load and validate configuration from a YAML file."""
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    interpolated = _walk_and_interpolate(raw)

    return EngineConfig.model_validate(interpolated)


def merge_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge override config into base config."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    return merged
