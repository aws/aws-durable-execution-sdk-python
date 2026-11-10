# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Python Durable Execution Telemetry conformance handlers.

The conformance runner reads requirement mappings from
``TestingMetadata.TestDescription``. SAM builds each function through
``src/Makefile``. A renamed handler or a missing build target fails only when a
cloud deployment runs. So these tests pin that wiring locally.
The module also runs each handler through ``DurableFunctionTestRunner`` and checks
the exported record.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType
from typing import Any

import boto3
import pytest
import yaml
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


PKG_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = PKG_DIR / "src"
TEMPLATE_PATH = PKG_DIR / "template.yaml"

SDK_REPOSITORY_URL = "git+https://github.com/aws/aws-durable-execution-sdk-python.git"
EXPECTED_MODULES: frozenset[str] = frozenset(
    {
        "telemetry_1_success",
        "telemetry_2_failure",
        "telemetry_3_result_select",
        "telemetry_4_full_tree_map",
    }
)
# Logical ID -> requirement ID. The function name suffix must equal the
# requirement ID, so the runner can match the deployed function to its test.
EXPECTED_FUNCTIONS: dict[str, str] = {
    "Telemetry1Success": "telemetry-1",
    "Telemetry2Failure": "telemetry-2",
    "Telemetry3ResultSelect": "telemetry-3",
    "Telemetry4FullTreeMap": "telemetry-4",
}


class CfnLoader(yaml.SafeLoader):
    """SafeLoader that keeps CloudFormation short-form tags as (tag, value)."""


def _construct_cfn_tag(
    loader: CfnLoader, _suffix: str, node: yaml.Node
) -> tuple[str, Any]:
    value: Any
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return (node.tag, value)


CfnLoader.add_multi_constructor("!", _construct_cfn_tag)


def load_template(path: Path) -> dict[str, Any]:
    """Load a template like yaml.safe_load, keeping short-form tags."""
    with path.open(encoding="utf-8") as stream:
        loader = CfnLoader(stream)
        try:
            template: dict[str, Any] = loader.get_single_data()
        finally:
            loader.dispose()
    return template


def functions(template: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        logical_id: resource
        for logical_id, resource in template["Resources"].items()
        if resource["Type"] == "AWS::Serverless::Function"
    }


@pytest.fixture(scope="module")
def template() -> dict[str, Any]:
    return load_template(TEMPLATE_PATH)


@pytest.fixture(scope="module")
def makefile() -> str:
    return (SRC_DIR / "Makefile").read_text(encoding="utf-8")


def test_every_function_points_at_an_existing_handler_module(
    template: dict[str, Any],
) -> None:
    for resource in functions(template).values():
        module, _, attribute = resource["Properties"]["Handler"].partition(".")
        source = SRC_DIR / f"{module}.py"

        assert source.is_file()
        assert attribute == "handler"
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        assert any(
            isinstance(node, ast.FunctionDef) and node.name == "handler"
            for node in tree.body
        )


def test_every_function_builds_from_src_with_the_makefile(
    template: dict[str, Any],
) -> None:
    for resource in functions(template).values():
        assert resource["Metadata"]["BuildMethod"] == "makefile"
        assert resource["Properties"]["CodeUri"] == "src/"


def test_makefile_has_a_build_target_per_function(
    template: dict[str, Any], makefile: str
) -> None:
    for logical_id in functions(template):
        assert re.search(rf"^build-{logical_id}:", makefile, re.MULTILINE)
        assert re.search(rf"^\.PHONY:.*\bbuild-{logical_id}\b", makefile, re.MULTILINE)


def test_handler_modules_are_exactly_the_expected_set() -> None:
    assert {path.stem for path in SRC_DIR.glob("*.py")} == EXPECTED_MODULES


def test_every_handler_module_is_deployed_by_exactly_one_function(
    template: dict[str, Any],
) -> None:
    """Two functions on one module would leave one requirement untested."""
    deployed_modules = sorted(
        resource["Properties"]["Handler"].partition(".")[0]
        for resource in functions(template).values()
    )

    assert deployed_modules == sorted(EXPECTED_MODULES)


def test_requirements_install_both_packages_from_one_commit() -> None:
    requirements: str = (SRC_DIR / "requirements.txt").read_text(encoding="utf-8")

    for package in (
        "aws-durable-execution-sdk-python",
        "aws-durable-execution-sdk-python-telemetry-exporter",
    ):
        assert (
            f"{package} @ {SDK_REPOSITORY_URL}@${{PYTHON_SDK_REF}}"
            f"#subdirectory=packages/{package}"
        ) in requirements


def test_log_group_parameter_is_declared(template: dict[str, Any]) -> None:
    assert template["Parameters"]["TelemetryLogGroupName"]["Type"] == "String"
    assert template["Parameters"]["LambdaExecutionRoleArn"]["Type"] == "String"


def test_log_group_is_created_from_the_parameter(template: dict[str, Any]) -> None:
    group: dict[str, Any] = template["Resources"]["TelemetryLogGroup"]

    assert group["Type"] == "AWS::Logs::LogGroup"
    assert group["Properties"]["LogGroupName"] == ("!Ref", "TelemetryLogGroupName")
    assert group["Properties"]["RetentionInDays"] == 1


def test_every_function_depends_on_the_log_group(template: dict[str, Any]) -> None:
    for resource in functions(template).values():
        assert resource["DependsOn"] == "TelemetryLogGroup"


def test_log_group_name_reaches_the_handler_environment(
    template: dict[str, Any],
) -> None:
    variables: dict[str, Any] = template["Globals"]["Function"]["Environment"][
        "Variables"
    ]

    assert variables["TELEMETRY_LOG_GROUP_NAME"] == ("!Ref", "TelemetryLogGroupName")


def test_template_functions_are_exactly_the_expected_set(
    template: dict[str, Any],
) -> None:
    assert {
        logical_id: resource["TestingMetadata"]["TestDescription"]
        for logical_id, resource in functions(template).items()
    } == {
        logical_id: [requirement_id]
        for logical_id, requirement_id in EXPECTED_FUNCTIONS.items()
    }


@pytest.mark.parametrize(
    ("logical_id", "requirement_id"), sorted(EXPECTED_FUNCTIONS.items())
)
def test_function_settings_match_the_contract(
    template: dict[str, Any], logical_id: str, requirement_id: str
) -> None:
    function: dict[str, Any] = functions(template)[logical_id]

    assert function["Properties"]["FunctionName"] == (
        "!Sub",
        f"${{AWS::StackName}}-{requirement_id}",
    )
    assert function["Properties"]["Role"] == ("!Ref", "LambdaExecutionRoleArn")


def test_global_function_settings_match_the_contract(
    template: dict[str, Any],
) -> None:
    assert template["Globals"]["Function"]["Runtime"] == "python3.13"
    assert template["Globals"]["Function"]["DurableConfig"] == {
        "ExecutionTimeout": 300,
        "RetentionPeriodInDays": 1,
    }


def load_handler_module(module: str) -> ModuleType:
    """Execute ``src/<module>.py`` from its file, as the Lambda runtime does.

    ``src/`` is not a package, so the module is loaded by path. Executing the
    module runs the import-time environment guard and constructs the plugin.
    """
    spec = importlib.util.spec_from_file_location(module, SRC_DIR / f"{module}.py")
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


LOG_GROUP_NAME = "/aws/lambda/telemetry-test"


class FakeLogsClient:
    """Stands in for the boto3 ``logs`` client and records each PutLogEvents call."""

    def __init__(self) -> None:
        self.put_log_events_calls: list[dict[str, Any]] = []

    def create_log_stream(self, **_kwargs: Any) -> None:
        return None

    def put_log_events(self, **kwargs: Any) -> None:
        self.put_log_events_calls.append(kwargs)

    def records(self) -> list[dict[str, Any]]:
        """Return the JSON record of every log event, in send order."""
        return [
            json.loads(event["message"])
            for call in self.put_log_events_calls
            for event in call["logEvents"]
        ]


@pytest.fixture
def logs_client(monkeypatch: pytest.MonkeyPatch) -> FakeLogsClient:
    """Give the handler's exporter a fake ``logs`` client.

    The handler constructs ``CloudWatchLogsExporter`` at import, and the
    exporter calls ``boto3.client("logs")`` in its constructor. botocore
    resolves credentials when it creates a client, so a real client would
    touch the credential chain and IMDS. The fake keeps the test on the real
    ``export`` and ``render`` path and stops at the AWS API boundary.
    """
    client = FakeLogsClient()
    monkeypatch.setenv("TELEMETRY_LOG_GROUP_NAME", LOG_GROUP_NAME)
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: client)
    return client


@pytest.mark.parametrize("module", sorted(EXPECTED_MODULES))
def test_handler_module_fails_fast_without_log_group(
    monkeypatch: pytest.MonkeyPatch, module: str
) -> None:
    monkeypatch.delenv("TELEMETRY_LOG_GROUP_NAME", raising=False)

    with pytest.raises(RuntimeError, match="TELEMETRY_LOG_GROUP_NAME must be set"):
        load_handler_module(module)


def test_telemetry_1_handler_succeeds_and_records_the_step(
    logs_client: FakeLogsClient,
) -> None:
    module = load_handler_module("telemetry_1_success")

    with DurableFunctionTestRunner(
        handler=module.handler, execution_timeout=15
    ) as runner:
        result = runner.run(input='{"value": 1}')

    assert result.status is InvocationStatus.SUCCEEDED
    assert len(logs_client.put_log_events_calls) == 1
    assert logs_client.put_log_events_calls[0]["logGroupName"] == LOG_GROUP_NAME
    record = logs_client.records()[0]
    assert record["status"] == "SUCCEEDED"
    increment = record["operationsByName"]["increment"]
    assert increment["type"] == "STEP"
    assert increment["status"] == "SUCCEEDED"
    assert increment["count"] == 1


def test_telemetry_2_handler_fails_with_payment_failed_after_one_attempt(
    logs_client: FakeLogsClient,
) -> None:
    """telemetry-2 pins both messages through ExpectedResult and ExpectedExecutionHistory."""
    module = load_handler_module("telemetry_2_failure")

    with DurableFunctionTestRunner(
        handler=module.handler, execution_timeout=15
    ) as runner:
        result = runner.run(input="{}")

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.message == "payment failed"
    assert len(logs_client.put_log_events_calls) == 1
    assert logs_client.put_log_events_calls[0]["logGroupName"] == LOG_GROUP_NAME
    record = logs_client.records()[0]
    assert record["status"] == "FAILED"
    assert "output" not in record
    assert record["error"]["message"] == "payment failed"
    charge = record["operationsByName"]["charge"]
    assert charge["type"] == "STEP"
    assert charge["status"] == "FAILED"
    assert charge["count"] == 1
    assert charge["failedCount"] == 1
    assert charge["maxAttempt"] == 1
    assert charge["error"]["message"] == "card declined"


def test_telemetry_3_handler_exports_only_the_selected_result_key(
    logs_client: FakeLogsClient,
) -> None:
    """telemetry-3 pins the three-key execution result and the one-key record result."""
    module = load_handler_module("telemetry_3_result_select")

    with DurableFunctionTestRunner(
        handler=module.handler, execution_timeout=15
    ) as runner:
        result = runner.run(input='{"userId": "u-123"}')

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert json.loads(result.result) == {
        "userId": "u-123",
        "email": "test@example.com",
        "token": "tok-456",
    }
    assert len(logs_client.put_log_events_calls) == 1
    assert logs_client.put_log_events_calls[0]["logGroupName"] == LOG_GROUP_NAME
    record = logs_client.records()[0]
    assert record["status"] == "SUCCEEDED"
    assert "input" not in record
    assert "output" not in record
    fetch_user = record["operationsByName"]["fetch-user"]
    assert fetch_user["type"] == "STEP"
    assert fetch_user["status"] == "SUCCEEDED"
    assert fetch_user["count"] == 1
    assert fetch_user["result"] == {"userId": "u-123"}


def test_telemetry_4_handler_records_the_full_operation_tree(
    logs_client: FakeLogsClient,
) -> None:
    """telemetry-4 pins five operationsByName keys under full-tree detail."""
    module = load_handler_module("telemetry_4_full_tree_map")

    with DurableFunctionTestRunner(
        handler=module.handler, execution_timeout=15
    ) as runner:
        result = runner.run(input="[1, 2]")

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert json.loads(result.result) == ["Hello, 1!", "Hello, 2!"]
    assert len(logs_client.put_log_events_calls) == 1
    assert logs_client.put_log_events_calls[0]["logGroupName"] == LOG_GROUP_NAME
    record = logs_client.records()[0]
    assert record["status"] == "SUCCEEDED"
    assert record["input"] == [1, 2]
    assert record["output"] == ["Hello, 1!", "Hello, 2!"]
    operations = record["operationsByName"]
    assert set(operations) == {
        "process-items",
        "item-1",
        "item-2",
        "greet-1",
        "greet-2",
    }
    expected_kinds = {
        "process-items": ("CONTEXT", "Map"),
        "item-1": ("CONTEXT", "MapIteration"),
        "item-2": ("CONTEXT", "MapIteration"),
        "greet-1": ("STEP", "Step"),
        "greet-2": ("STEP", "Step"),
    }
    for name, (operation_type, sub_type) in expected_kinds.items():
        operation = operations[name]
        assert operation["type"] == operation_type
        assert operation["subType"] == sub_type
        assert operation["status"] == "SUCCEEDED"
        assert operation["count"] == 1
        assert operation["failedCount"] == 0
        assert "result" not in operation
        assert "error" not in operation
        if operation_type == "CONTEXT":
            assert "maxAttempt" not in operation
        else:
            assert operation["maxAttempt"] == 1
