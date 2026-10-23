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
import re
from pathlib import Path
from typing import Any

import pytest
import yaml


PKG_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = PKG_DIR / "src"
TEMPLATE_PATH = PKG_DIR / "template.yaml"

SDK_REPOSITORY_URL = "git+https://github.com/aws/aws-durable-execution-sdk-python.git"
EXPECTED_COVERAGE: list[str] = ["telemetry-1"]
EXPECTED_MODULES: frozenset[str] = frozenset({"telemetry_1_success"})


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


def test_coverage_is_exactly_telemetry_1(template: dict[str, Any]) -> None:
    coverage: list[str] = [
        description
        for resource in functions(template).values()
        for description in resource["TestingMetadata"]["TestDescription"]
    ]

    assert coverage == EXPECTED_COVERAGE


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


def test_function_settings_match_the_contract(template: dict[str, Any]) -> None:
    function: dict[str, Any] = functions(template)["Telemetry1Success"]

    assert function["Properties"]["FunctionName"] == (
        "!Sub",
        "${AWS::StackName}-telemetry-1",
    )
    assert function["Properties"]["Role"] == ("!Ref", "LambdaExecutionRoleArn")
    assert template["Globals"]["Function"]["Runtime"] == "python3.13"
    assert template["Globals"]["Function"]["DurableConfig"] == {
        "ExecutionTimeout": 300,
        "RetentionPeriodInDays": 1,
    }


def test_handler_reads_the_log_group_from_the_environment() -> None:
    source: str = (SRC_DIR / "telemetry_1_success.py").read_text(encoding="utf-8")

    assert 'os.environ.get("TELEMETRY_LOG_GROUP_NAME")' in source
    assert "raise RuntimeError" in source
    assert "CloudWatchLogsExporter(log_group_name=log_group_name)" in source
