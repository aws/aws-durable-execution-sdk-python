# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Python Durable Execution Telemetry conformance handlers.

The conformance runner reads requirement mappings from
``TestingMetadata.TestDescription``. SAM builds each function through
``src/Makefile``. A renamed handler or a missing build target fails only when a
cloud deployment runs. So these tests pin that wiring locally.
"""

from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_telemetry_exporter import CloudWatchLogsExporter
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


PKG_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = PKG_DIR / "src"
TEMPLATE_PATH = PKG_DIR / "template.yaml"

SDK_REPOSITORY_URL = "git+https://github.com/aws/aws-durable-execution-sdk-python.git"
EXPECTED_MODULES: frozenset[str] = frozenset(
    {"telemetry_1_success", "telemetry_2_failure"}
)
# Logical ID -> requirement ID. The function name suffix must equal the
# requirement ID, so the runner can match the deployed function to its test.
EXPECTED_FUNCTIONS: dict[str, str] = {
    "Telemetry1Success": "telemetry-1",
    "Telemetry2Failure": "telemetry-2",
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


@pytest.fixture
def exported_records(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture what the handler's exporter would write to CloudWatch Logs.

    The handler constructs ``CloudWatchLogsExporter`` itself, so the capture
    replaces ``export`` on the class. The captured value is the rendered
    record with ``operationsByName``, which is the shape the suite asserts.
    """
    records: list[dict[str, Any]] = []

    def capture(self: CloudWatchLogsExporter, record: dict[str, Any]) -> None:
        records.append(self.render(record))

    monkeypatch.setenv("TELEMETRY_LOG_GROUP_NAME", "/aws/lambda/telemetry-test")
    # The exporter builds a boto3 client at construction. Client construction
    # needs a region and no credentials.
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setattr(CloudWatchLogsExporter, "export", capture)
    return records


@pytest.mark.parametrize("module", sorted(EXPECTED_MODULES))
def test_handler_module_fails_fast_without_log_group(
    monkeypatch: pytest.MonkeyPatch, module: str
) -> None:
    monkeypatch.delenv("TELEMETRY_LOG_GROUP_NAME", raising=False)

    with pytest.raises(RuntimeError, match="TELEMETRY_LOG_GROUP_NAME must be set"):
        load_handler_module(module)


def test_telemetry_1_handler_succeeds_and_records_the_step(
    exported_records: list[dict[str, Any]],
) -> None:
    module = load_handler_module("telemetry_1_success")

    with DurableFunctionTestRunner(
        handler=module.handler, execution_timeout=15
    ) as runner:
        result = runner.run(input='{"value": 1}')

    assert result.status is InvocationStatus.SUCCEEDED
    assert len(exported_records) == 1
    record = exported_records[0]
    assert record["status"] == "SUCCEEDED"
    increment = record["operationsByName"]["increment"]
    assert increment["type"] == "STEP"
    assert increment["status"] == "SUCCEEDED"
    assert increment["count"] == 1


def test_telemetry_2_handler_fails_with_payment_failed_after_one_attempt(
    exported_records: list[dict[str, Any]],
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
    assert len(exported_records) == 1
    record = exported_records[0]
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
