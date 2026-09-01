# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Config normalization tests for the Workflow Insight plugin.

Covers the ``StrEnum``-backed ``EmitMode`` / ``OperationDetail`` inputs (comment 5):
enum members and JS-style strings both normalize to enum members, defaults resolve
to the documented behavior, and an invalid dynamic string raises ``ValueError``.
Also carries a smoke test for the exact documented call shape (comment 4).
"""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python_insight import (
    EmitMode,
    OperationDetail,
    WorkflowInsightConfig,
    workflow_insight,
)
from aws_durable_execution_sdk_python_insight.exporters import S3Exporter


# -- enum values match the JS-style wire strings -----------------------------


def test_emit_mode_values():
    assert EmitMode.ON_COMPLETE == "on-complete"
    assert EmitMode.ON_FAILURE == "on-failure"
    assert EmitMode.ON_CHANGE == "on-change"


def test_operation_detail_values():
    assert OperationDetail.TOP_LEVEL == "top-level"
    assert OperationDetail.FULL_TREE == "full-tree"


# -- string inputs normalize to enum members --------------------------------


@pytest.mark.parametrize(
    ("text", "member"),
    [
        ("on-complete", EmitMode.ON_COMPLETE),
        ("on-failure", EmitMode.ON_FAILURE),
        ("on-change", EmitMode.ON_CHANGE),
    ],
)
def test_emit_mode_string_input_normalizes_to_enum(text, member):
    config = WorkflowInsightConfig(emit_mode=text)
    assert config.emit_mode is member
    assert isinstance(config.emit_mode, EmitMode)


@pytest.mark.parametrize(
    ("text", "member"),
    [
        ("top-level", OperationDetail.TOP_LEVEL),
        ("full-tree", OperationDetail.FULL_TREE),
    ],
)
def test_operation_detail_string_input_normalizes_to_enum(text, member):
    config = WorkflowInsightConfig(operation_detail=text)
    assert config.operation_detail is member
    assert isinstance(config.operation_detail, OperationDetail)


# -- enum-member inputs pass through unchanged -------------------------------


def test_enum_member_inputs_pass_through():
    config = WorkflowInsightConfig(
        emit_mode=EmitMode.ON_CHANGE, operation_detail=OperationDetail.FULL_TREE
    )
    assert config.emit_mode is EmitMode.ON_CHANGE
    assert config.operation_detail is OperationDetail.FULL_TREE


# -- defaults ----------------------------------------------------------------


def test_defaults_are_none_and_plugin_resolves_them():
    config = WorkflowInsightConfig()
    assert config.emit_mode is None
    assert config.operation_detail is None
    plugin = workflow_insight(config)
    # Default emit mode is on-complete; default detail is top-level.
    assert plugin._emit_mode is EmitMode.ON_COMPLETE
    assert plugin._top_level_only is True


def test_full_tree_input_disables_top_level_only():
    plugin = workflow_insight(WorkflowInsightConfig(operation_detail="full-tree"))
    assert plugin._top_level_only is False


# -- invalid dynamic strings raise -------------------------------------------


def test_invalid_emit_mode_string_raises_value_error():
    with pytest.raises(ValueError):
        WorkflowInsightConfig(emit_mode="on-compleat")  # typo, dynamic value


def test_invalid_operation_detail_string_raises_value_error():
    with pytest.raises(ValueError):
        WorkflowInsightConfig(operation_detail="whole-tree")  # invalid dynamic value


# -- README smoke test: the documented call shape must construct (comment 4) --


def test_readme_usage_call_shape_constructs_plugin():
    # Mirrors the README example: workflow_insight(WorkflowInsightConfig(exporters=[...])).
    # A stub S3 client avoids any boto3/network dependency while exercising the
    # exact documented call.
    exporter = S3Exporter(
        bucket="my-bucket", prefix="workflow-insight/", client=object()
    )
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    assert plugin._exporters == [exporter]


# -- export_timeout_seconds validation ---------------------------------------


def test_export_timeout_defaults_to_five_seconds():
    config = WorkflowInsightConfig()
    assert config.export_timeout_seconds == 5.0
    assert workflow_insight(config)._export_timeout == 5.0


@pytest.mark.parametrize("value", [0.1, 1, 2.5, 30])
def test_export_timeout_accepts_finite_positive_numbers(value):
    config = WorkflowInsightConfig(export_timeout_seconds=value)
    assert config.export_timeout_seconds == value
    assert workflow_insight(config)._export_timeout == float(value)


@pytest.mark.parametrize(
    "value",
    [
        0,
        0.0,
        -1,
        -0.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        True,  # bool is an int subtype but must be rejected explicitly
        False,
        "5",  # non-numeric
        None,
    ],
)
def test_export_timeout_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        WorkflowInsightConfig(export_timeout_seconds=value)
