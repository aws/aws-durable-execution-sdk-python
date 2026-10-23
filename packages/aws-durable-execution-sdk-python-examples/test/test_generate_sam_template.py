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
        "LogFormat": "JSON",
        "LogGroup": {"Ref": "HelloWorldLogGroup"},
    }
    assert resources["LoggerExample"]["Properties"]["LoggingConfig"] == {
        "LogFormat": "JSON",
        "ApplicationLogLevel": "INFO",
        "LogGroup": {"Ref": "LoggerExampleLogGroup"},
    }
    assert resources["PlainLambda"]["Properties"]["LoggingConfig"] == {
        "LogGroup": {"Ref": "PlainLambdaLogGroup"},
    }


MICROVM_EXAMPLE = {
    "name": "MicroVM Example",
    "handler": "microvm_example.handler",
    "description": "A MicroVM example",
    "usesMicrovm": True,
    "durableConfig": {"RetentionPeriodInDays": 7, "ExecutionTimeout": 900},
}
PLAIN_EXAMPLE = {
    "name": "Hello World",
    "handler": "hello_world.handler",
    "description": "A durable example",
    "durableConfig": {"RetentionPeriodInDays": 7, "ExecutionTimeout": 300},
}


def test_build_template_declares_microvm_parameters_with_empty_defaults():
    template = generate_sam_template.build_template([PLAIN_EXAMPLE])

    parameters = template["Parameters"]
    assert parameters["MicrovmImageArn"]["Default"] == ""
    assert parameters["MicrovmImageArn"]["Type"] == "String"
    assert parameters["MicrovmExecutionRoleArn"]["Default"] == ""
    assert parameters["MicrovmExecutionRoleArn"]["Type"] == "String"


def test_build_template_omits_condition_without_microvm_example():
    template = generate_sam_template.build_template([PLAIN_EXAMPLE])

    assert "Conditions" not in template


def test_build_template_requires_image_and_role_for_microvm_condition():
    template = generate_sam_template.build_template([PLAIN_EXAMPLE, MICROVM_EXAMPLE])

    assert template["Conditions"] == {
        "HasMicrovmImage": {
            "Fn::And": [
                {"Fn::Not": [{"Fn::Equals": [{"Ref": "MicrovmImageArn"}, ""]}]},
                {"Fn::Not": [{"Fn::Equals": [{"Ref": "MicrovmExecutionRoleArn"}, ""]}]},
            ]
        }
    }


def test_build_template_sets_condition_on_microvm_function_and_log_group():
    template = generate_sam_template.build_template([MICROVM_EXAMPLE])

    resources = template["Resources"]
    assert set(resources) == {"MicrovmExample", "MicrovmExampleLogGroup"}
    assert resources["MicrovmExample"]["Condition"] == "HasMicrovmImage"
    assert resources["MicrovmExampleLogGroup"]["Condition"] == "HasMicrovmImage"


def test_build_template_sets_microvm_environment_variables():
    template = generate_sam_template.build_template(
        [{**MICROVM_EXAMPLE, "environment": {"EXTRA": "value"}}]
    )

    assert template["Resources"]["MicrovmExample"]["Properties"]["Environment"] == {
        "Variables": {
            "EXTRA": "value",
            "MICROVM_IMAGE_ARN": {"Ref": "MicrovmImageArn"},
            "MICROVM_EXECUTION_ROLE_ARN": {"Ref": "MicrovmExecutionRoleArn"},
        }
    }


def test_build_template_leaves_example_without_flag_unconditional():
    template = generate_sam_template.build_template(
        [{**PLAIN_EXAMPLE, "usesMicrovm": False}]
    )

    resources = template["Resources"]
    assert "Condition" not in resources["HelloWorld"]
    assert "Condition" not in resources["HelloWorldLogGroup"]
    assert "Environment" not in resources["HelloWorld"]["Properties"]
    assert "Timeout" not in resources["HelloWorld"]["Properties"]
    assert "Conditions" not in template


def test_build_template_sets_timeout_from_catalog():
    template = generate_sam_template.build_template([{**PLAIN_EXAMPLE, "timeout": 120}])

    assert template["Resources"]["HelloWorld"]["Properties"]["Timeout"] == 120
    assert template["Globals"]["Function"]["Timeout"] == 60
