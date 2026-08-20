# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Python OTel conformance handlers and templates.

The conformance runner reads requirement mappings from
``TestingMetadata.TestDescription``, and SAM builds each function through
``src/Makefile``. A handler that is renamed, unregistered, or missing a build
target therefore only fails once a deployment runs in the cloud, so these tests
pin that wiring locally.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml


PKG_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = PKG_DIR / "src"
TEMPLATE_PATH = PKG_DIR / "template.yaml"
LONG_RUNNING_TEMPLATE_PATH = PKG_DIR / "template-long-running.yaml"

SDK_REPOSITORY_URL = "git+https://github.com/aws/aws-durable-execution-sdk-python.git"
COLLECTOR_CONFIG_URI = "/opt/collector-config/config-s3.yaml"

EXPECTED_MAPPINGS: list[tuple[str, str]] = [
    ("Otel1Success", "otel-invocation-1"),
    ("Otel2WaitResume", "otel-invocation-2"),
    ("Otel3Retry", "otel-invocation-3"),
    ("Otel4TerminalFailure", "otel-invocation-4"),
    ("Otel5ChildContext", "otel-invocation-5"),
    ("Otel6Parallel", "otel-invocation-6"),
    ("Otel7Map", "otel-invocation-7"),
    ("Otel8HandledFailure", "otel-invocation-8"),
    ("Otel9WaitForCondition", "otel-invocation-9"),
    ("Otel10WaitForCallback", "otel-invocation-10"),
    ("Otel11ChainedInvoke", "otel-invocation-11"),
    ("Otel12ChildContextFailure", "otel-invocation-12"),
    ("Otel13ParallelFailure", "otel-invocation-13"),
    ("Otel14MapFailure", "otel-invocation-14"),
    ("Otel15WaitInterrupted", "otel-invocation-15"),
    ("Otel16WaitForConditionFailure", "otel-invocation-16"),
    ("Otel17WaitForCallbackFailure", "otel-invocation-17"),
    ("Otel18ChainedInvokeFailure", "otel-invocation-18"),
    ("Otel19ExecutionFailure", "otel-invocation-19"),
    ("Otel20VirtualContext", "otel-invocation-20"),
    ("OtelExecution1Success", "otel-execution-1"),
    ("OtelExecution2WaitResume", "otel-execution-2"),
    ("OtelExecution3Retry", "otel-execution-3"),
    ("OtelExecution4TerminalFailure", "otel-execution-4"),
    ("OtelExecution5ChildContext", "otel-execution-5"),
    ("OtelExecution6Parallel", "otel-execution-6"),
    ("OtelExecution7Map", "otel-execution-7"),
    ("OtelExecution8HandledFailure", "otel-execution-8"),
    ("OtelExecution9WaitForCondition", "otel-execution-9"),
    ("OtelExecution10WaitForCallback", "otel-execution-10"),
    ("OtelExecution11ChainedInvoke", "otel-execution-11"),
    ("OtelExecution12ChildContextFailure", "otel-execution-12"),
    ("OtelExecution13ParallelFailure", "otel-execution-13"),
    ("OtelExecution14MapFailure", "otel-execution-14"),
    ("OtelExecution15WaitInterrupted", "otel-execution-15"),
    ("OtelExecution16WaitForConditionFailure", "otel-execution-16"),
    ("OtelExecution17WaitForCallbackFailure", "otel-execution-17"),
    ("OtelExecution18ChainedInvokeFailure", "otel-execution-18"),
    ("OtelExecution19ExecutionFailure", "otel-execution-19"),
    ("OtelExecution20VirtualContext", "otel-execution-20"),
]
EXPECTED_LONG_RUNNING_MAPPINGS: list[tuple[str, str]] = [
    ("OtelLongRunning1Wait", "otel-long-running-1"),
    ("OtelLongRunning2Retry", "otel-long-running-2"),
    ("OtelLongRunning3Callback", "otel-long-running-3"),
    ("OtelLongRunning4ChainedInvoke", "otel-long-running-4"),
]
# Invoke targets carry no requirement of their own; the chained-invoke cases
# assert on the telemetry the target produces.
EXPECTED_TARGETS: dict[str, str] = {
    "Otel11InvokeTarget": "otel-invocation-11-target",
    "Otel18InvokeTarget": "otel-invocation-18-target",
    "OtelExecution11InvokeTarget": "otel-execution-11-target",
    "OtelExecution18InvokeTarget": "otel-execution-18-target",
}
EXPECTED_LONG_RUNNING_TARGETS: dict[str, str] = {
    "OtelLongRunning4InvokeTarget": "otel-long-running-4-target",
}
CHAINED_INVOKE_PAIRS: list[tuple[Path, str, str]] = [
    (TEMPLATE_PATH, "Otel11ChainedInvoke", "Otel11InvokeTarget"),
    (TEMPLATE_PATH, "Otel18ChainedInvokeFailure", "Otel18InvokeTarget"),
    (TEMPLATE_PATH, "OtelExecution11ChainedInvoke", "OtelExecution11InvokeTarget"),
    (
        TEMPLATE_PATH,
        "OtelExecution18ChainedInvokeFailure",
        "OtelExecution18InvokeTarget",
    ),
    (
        LONG_RUNNING_TEMPLATE_PATH,
        "OtelLongRunning4ChainedInvoke",
        "OtelLongRunning4InvokeTarget",
    ),
]
REQUIRED_PARAMETERS: frozenset[str] = frozenset(
    {
        "LambdaExecutionRoleArn",
        "OtelCollectorBucket",
        "OtelCollectorLayerArn",
        "OtelCollectorPrefix",
        "OtelExecWrapper",
        "OtelExporterEndpoint",
        "OtelExporterHeaders",
        "OtelLayerArn",
        "OtelSecretEnvironmentNames",
        "OtelServiceName",
        "OtelSuite",
        "OtelTracesExporter",
    }
)
REQUIRED_LONG_RUNNING_PARAMETERS: frozenset[str] = frozenset(
    {
        "LambdaExecutionRoleArn",
        "OtelExecWrapper",
        "OtelLayerArn",
        "OtelServiceName",
        "OtelTracesExporter",
        "OtelView",
    }
)
EXPECTED_MODULES: frozenset[str] = frozenset(
    {
        "common",
        "otel_1_success",
        "otel_2_wait_resume",
        "otel_3_retry",
        "otel_4_terminal_failure",
        "otel_5_child_context",
        "otel_6_parallel",
        "otel_7_map",
        "otel_8_handled_failure",
        "otel_9_wait_for_condition",
        "otel_10_wait_for_callback",
        "otel_11_chained_invoke",
        "otel_12_child_context_failure",
        "otel_13_parallel_failure",
        "otel_14_map_failure",
        "otel_15_wait_interrupted",
        "otel_16_wait_for_condition_failure",
        "otel_17_wait_for_callback_failure",
        "otel_18_chained_invoke_failure",
        "otel_19_execution_failure",
        "otel_20_virtual_context",
        "otel_long_running_1_wait",
        "otel_long_running_2_retry",
        "otel_long_running_3_callback",
        "otel_long_running_4_chained_invoke",
    }
)


@dataclass(frozen=True)
class CfnTag:
    """A CloudFormation short-form intrinsic, such as ``!Ref OtelLayerArn``."""

    tag: str
    value: Any


def ref(name: str) -> CfnTag:
    return CfnTag("!Ref", name)


def sub(value: str) -> CfnTag:
    return CfnTag("!Sub", value)


class CfnLoader(yaml.SafeLoader):
    """SafeLoader that wraps CloudFormation short-form tags instead of failing."""


def _construct_cfn_tag(loader: CfnLoader, _suffix: str, node: yaml.Node) -> CfnTag:
    value: Any
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return CfnTag(node.tag, value)


CfnLoader.add_multi_constructor("!", _construct_cfn_tag)


def _safe_load_cfn(stream: object) -> Any:
    """Safely load a CloudFormation template with short-form tags.

    Equivalent to yaml.safe_load (CfnLoader extends yaml.SafeLoader) while
    additionally preserving CloudFormation short-form tags.
    """
    loader = CfnLoader(stream)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def load_template(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        template: dict[str, Any] = _safe_load_cfn(stream)
    return template


def requirement_mappings(template: dict[str, Any]) -> list[tuple[str, str]]:
    """Return the (logical id, requirement id) pairs the runner discovers."""
    mappings: list[tuple[str, str]] = []
    for logical_id, resource in template["Resources"].items():
        descriptions: list[str] = resource.get("TestingMetadata", {}).get(
            "TestDescription", []
        )
        mappings.extend((logical_id, description) for description in descriptions)
    return mappings


def environment_variables(resource: dict[str, Any]) -> dict[str, Any]:
    environment: dict[str, Any] = resource["Properties"].get("Environment", {})
    variables: dict[str, Any] = environment.get("Variables", {})
    return variables


@pytest.fixture(scope="module")
def template() -> dict[str, Any]:
    return load_template(TEMPLATE_PATH)


@pytest.fixture(scope="module")
def long_running_template() -> dict[str, Any]:
    return load_template(LONG_RUNNING_TEMPLATE_PATH)


@pytest.fixture(scope="module")
def makefile() -> str:
    return (SRC_DIR / "Makefile").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("template_path", "expected_mappings"),
    [
        (TEMPLATE_PATH, EXPECTED_MAPPINGS),
        (LONG_RUNNING_TEMPLATE_PATH, EXPECTED_LONG_RUNNING_MAPPINGS),
    ],
    ids=["template", "template-long-running"],
)
def test_templates_map_every_otel_requirement(
    template_path: Path, expected_mappings: list[tuple[str, str]]
) -> None:
    assert requirement_mappings(load_template(template_path)) == expected_mappings


@pytest.mark.parametrize(
    ("template_path", "expected_mappings", "expected_targets"),
    [
        (TEMPLATE_PATH, EXPECTED_MAPPINGS, EXPECTED_TARGETS),
        (
            LONG_RUNNING_TEMPLATE_PATH,
            EXPECTED_LONG_RUNNING_MAPPINGS,
            EXPECTED_LONG_RUNNING_TARGETS,
        ),
    ],
    ids=["template", "template-long-running"],
)
def test_templates_declare_only_the_expected_functions(
    template_path: Path,
    expected_mappings: list[tuple[str, str]],
    expected_targets: dict[str, str],
) -> None:
    resources: dict[str, Any] = load_template(template_path)["Resources"]
    expected_ids: set[str] = {logical_id for logical_id, _ in expected_mappings} | set(
        expected_targets
    )

    assert set(resources) == expected_ids
    for resource in resources.values():
        assert resource["Type"] == "AWS::Serverless::Function"
        assert resource["Metadata"]["BuildMethod"] == "makefile"
        assert resource["Properties"]["CodeUri"] == "src/"
        assert resource["Properties"]["Role"] == ref("LambdaExecutionRoleArn")


@pytest.mark.parametrize(
    ("template_path", "expected_mappings", "expected_targets"),
    [
        (TEMPLATE_PATH, EXPECTED_MAPPINGS, EXPECTED_TARGETS),
        (
            LONG_RUNNING_TEMPLATE_PATH,
            EXPECTED_LONG_RUNNING_MAPPINGS,
            EXPECTED_LONG_RUNNING_TARGETS,
        ),
    ],
    ids=["template", "template-long-running"],
)
def test_function_names_identify_the_requirement(
    template_path: Path,
    expected_mappings: list[tuple[str, str]],
    expected_targets: dict[str, str],
) -> None:
    resources: dict[str, Any] = load_template(template_path)["Resources"]
    expected_names: dict[str, str] = {
        **dict(expected_mappings),
        **expected_targets,
    }

    for logical_id, suffix in expected_names.items():
        assert resources[logical_id]["Properties"]["FunctionName"] == sub(
            f"${{AWS::StackName}}-{suffix}"
        )


@pytest.mark.parametrize(
    ("template_path", "expected_mappings", "expected_targets"),
    [
        (TEMPLATE_PATH, EXPECTED_MAPPINGS, EXPECTED_TARGETS),
        (
            LONG_RUNNING_TEMPLATE_PATH,
            EXPECTED_LONG_RUNNING_MAPPINGS,
            EXPECTED_LONG_RUNNING_TARGETS,
        ),
    ],
    ids=["template", "template-long-running"],
)
def test_makefile_builds_every_function(
    makefile: str,
    template_path: Path,
    expected_mappings: list[tuple[str, str]],
    expected_targets: dict[str, str],
) -> None:
    logical_ids: list[str] = [logical_id for logical_id, _ in expected_mappings] + list(
        expected_targets
    )

    for logical_id in logical_ids:
        assert f"build-{logical_id}" in makefile


@pytest.mark.parametrize(
    ("template_path", "required_parameters"),
    [
        (TEMPLATE_PATH, REQUIRED_PARAMETERS),
        (LONG_RUNNING_TEMPLATE_PATH, REQUIRED_LONG_RUNNING_PARAMETERS),
    ],
    ids=["template", "template-long-running"],
)
def test_templates_accept_the_runner_parameters(
    template_path: Path, required_parameters: frozenset[str]
) -> None:
    parameters: dict[str, Any] = load_template(template_path)["Parameters"]

    assert required_parameters <= set(parameters)


@pytest.mark.parametrize(
    ("template_path", "expected_mappings"),
    [
        (TEMPLATE_PATH, EXPECTED_MAPPINGS),
        (LONG_RUNNING_TEMPLATE_PATH, EXPECTED_LONG_RUNNING_MAPPINGS),
    ],
    ids=["template", "template-long-running"],
)
def test_every_registered_handler_exists(
    template_path: Path,
    expected_mappings: list[tuple[str, str]],
) -> None:
    resources: dict[str, Any] = load_template(template_path)["Resources"]

    assert expected_mappings  # every template registers at least one requirement
    for logical_id, resource in resources.items():
        handler: str = resource["Properties"]["Handler"]
        module_name, _, function_name = handler.rpartition(".")
        module_path: Path = SRC_DIR / f"{module_name}.py"
        assert module_path.is_file(), f"{logical_id} references missing {module_path}"
        tree: ast.Module = ast.parse(
            module_path.read_text(encoding="utf-8"), filename=str(module_path)
        )
        exported: set[str] = {
            node.name for node in tree.body if isinstance(node, ast.FunctionDef)
        }
        assert function_name in exported, (
            f"{module_name} does not define {function_name}"
        )


def test_template_deploys_only_the_selected_view(template: dict[str, Any]) -> None:
    assert template["Parameters"]["OtelSuite"]["AllowedValues"] == [
        "all",
        "otel-invocation",
        "otel-execution",
    ]
    assert {"DeployInvocationView", "DeployExecutionView"} <= set(
        template["Conditions"]
    )
    for logical_id, resource in template["Resources"].items():
        expected_condition: str = (
            "DeployExecutionView"
            if logical_id.startswith("OtelExecution")
            else "DeployInvocationView"
        )
        assert resource["Condition"] == expected_condition


def test_execution_view_functions_override_the_plugin_mode(
    template: dict[str, Any],
) -> None:
    globals_variables: dict[str, Any] = template["Globals"]["Function"]["Environment"][
        "Variables"
    ]

    assert globals_variables["OTEL_PLUGIN_MODE"] == "invocation"
    for logical_id, resource in template["Resources"].items():
        variables: dict[str, Any] = environment_variables(resource)
        if logical_id.startswith("OtelExecution"):
            assert variables["OTEL_PLUGIN_MODE"] == "execution"
        else:
            assert "OTEL_PLUGIN_MODE" not in variables


def test_long_running_functions_take_the_plugin_mode_from_a_parameter(
    long_running_template: dict[str, Any],
) -> None:
    globals_variables: dict[str, Any] = long_running_template["Globals"]["Function"][
        "Environment"
    ]["Variables"]

    assert globals_variables["OTEL_PLUGIN_MODE"] == ref("OtelView")
    assert long_running_template["Parameters"]["OtelView"]["AllowedValues"] == [
        "invocation",
        "execution",
    ]


def test_interrupted_wait_functions_time_out_before_the_wait_ends(
    template: dict[str, Any],
) -> None:
    for logical_id in ("Otel15WaitInterrupted", "OtelExecution15WaitInterrupted"):
        assert template["Resources"][logical_id]["Properties"]["DurableConfig"] == {
            "ExecutionTimeout": 15,
            "RetentionPeriodInDays": 1,
        }


@pytest.mark.parametrize(
    ("template_path", "source_id", "target_id"),
    CHAINED_INVOKE_PAIRS,
    ids=[source_id for _, source_id, _ in CHAINED_INVOKE_PAIRS],
)
def test_chained_invoke_functions_point_at_their_target(
    template_path: Path, source_id: str, target_id: str
) -> None:
    resources: dict[str, Any] = load_template(template_path)["Resources"]
    variables: dict[str, Any] = environment_variables(resources[source_id])

    assert variables["OTEL_INVOKE_TARGET_FUNCTION_NAME"] == sub(
        f"${{{target_id}.Arn}}:$LATEST"
    )


def test_exporter_headers_are_hidden(template: dict[str, Any]) -> None:
    assert template["Parameters"]["OtelExporterHeaders"]["NoEcho"] is True


def test_the_test_collector_is_optional(template: dict[str, Any]) -> None:
    globals_variables: dict[str, Any] = template["Globals"]["Function"]["Environment"][
        "Variables"
    ]

    assert template["Conditions"]["HasOtelCollectorLayer"] == CfnTag(
        "!Not", [CfnTag("!Equals", [ref("OtelCollectorLayerArn"), ""])]
    )
    assert globals_variables["OTEL_S3_BUCKET"] == ref("OtelCollectorBucket")
    assert globals_variables["OTEL_S3_PREFIX"] == ref("OtelCollectorPrefix")
    assert globals_variables["OPENTELEMETRY_COLLECTOR_CONFIG_URI"] == CfnTag(
        "!If",
        [
            "HasOtelCollectorLayer",
            COLLECTOR_CONFIG_URI,
            ref("AWS::NoValue"),
        ],
    )


def test_handler_modules_are_exactly_the_expected_set() -> None:
    modules: set[str] = {path.stem for path in SRC_DIR.glob("*.py")}

    assert modules == EXPECTED_MODULES


@pytest.mark.parametrize(
    "handler_path", sorted(SRC_DIR.glob("*.py")), ids=lambda path: path.stem
)
def test_handler_modules_are_valid_python(handler_path: Path) -> None:
    ast.parse(handler_path.read_text(encoding="utf-8"), filename=str(handler_path))


def test_requirements_install_both_sdk_packages_from_one_commit() -> None:
    requirements: str = (SRC_DIR / "requirements.txt").read_text(encoding="utf-8")

    for package in (
        "aws-durable-execution-sdk-python",
        "aws-durable-execution-sdk-python-otel",
    ):
        assert (
            f"{package} @ {SDK_REPOSITORY_URL}@${{PYTHON_SDK_REF}}"
            f"#subdirectory=packages/{package}"
        ) in requirements


def test_common_selects_the_plugin_from_the_deployed_view() -> None:
    common: str = (SRC_DIR / "common.py").read_text(encoding="utf-8")

    assert 'os.environ.get("OTEL_PLUGIN_MODE") == "execution"' in common
    assert "ExecutionOtelPlugin(OtelPluginConfig())" in common
    assert "InvocationOtelPlugin(OtelPluginConfig())" in common
