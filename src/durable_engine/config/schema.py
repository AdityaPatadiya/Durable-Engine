"""Runtime config validation against JSON Schema."""

import json
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent.parent.parent.parent / "config" / "schema.json"


def load_schema() -> dict:
    """Load the JSON Schema for config validation."""
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def validate_config_against_schema(config_dict: dict) -> list[str]:
    """Validate a config dictionary against the JSON Schema. Returns list of errors."""
    errors: list[str] = []
    schema = load_schema()

    required_top_level = schema.get("required", [])
    for field in required_top_level:
        if field not in config_dict:
            errors.append(f"Missing required top-level field: '{field}'")

    engine = config_dict.get("engine", {})
    if "batch_size" in engine:
        bs = engine["batch_size"]
        if not isinstance(bs, int) or bs < 1 or bs > 10000:
            errors.append(f"engine.batch_size must be integer between 1 and 10000, got {bs}")

    if "max_queue_size" in engine:
        qs = engine["max_queue_size"]
        if not isinstance(qs, int) or qs < 100:
            errors.append(f"engine.max_queue_size must be integer >= 100, got {qs}")

    sinks = config_dict.get("sinks", {})
    valid_types = {"rest", "grpc", "mq", "widecolumn"}
    valid_transformers = {"json", "protobuf", "xml", "avro"}
    for sink_name, sink_cfg in sinks.items():
        if not isinstance(sink_cfg, dict):
            errors.append(f"Sink '{sink_name}' must be a mapping")
            continue
        sink_type = sink_cfg.get("type")
        if sink_type not in valid_types:
            errors.append(f"Sink '{sink_name}' has invalid type '{sink_type}'")
        transformer = sink_cfg.get("transformer")
        if transformer not in valid_transformers:
            errors.append(f"Sink '{sink_name}' has invalid transformer '{transformer}'")

    return errors
