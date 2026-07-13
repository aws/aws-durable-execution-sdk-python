"""Tests for SAM template generation."""

import importlib.util
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "generate_sam_template.py"
)
SPEC = importlib.util.spec_from_file_location("generate_sam_template", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
generate_sam_template = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generate_sam_template)


def test_build_template_adds_default_json_logging_to_durable_functions():
    template = generate_sam_template.build_template(
        [
            {
                "handler": "hello_world.handler",
                "description": "A durable example",
                "durableConfig": {
                    "RetentionPeriodInDays": 7,
                    "ExecutionTimeout": 300,
                },
            },
            {
                "handler": "logger_example.handler",
                "description": "A durable example with custom logging",
                "durableConfig": {
                    "RetentionPeriodInDays": 7,
                    "ExecutionTimeout": 300,
                },
                "loggingConfig": {"ApplicationLogLevel": "INFO"},
            },
            {
                "handler": "plain_lambda.handler",
                "description": "A non-durable example",
            },
        ]
    )

    resources = template["Resources"]
    assert resources["HelloWorld"]["Properties"]["LoggingConfig"] == {
        "LogFormat": "JSON"
    }
    assert resources["LoggerExample"]["Properties"]["LoggingConfig"] == {
        "LogFormat": "JSON",
        "ApplicationLogLevel": "INFO",
    }
    assert "LoggingConfig" not in resources["PlainLambda"]["Properties"]
