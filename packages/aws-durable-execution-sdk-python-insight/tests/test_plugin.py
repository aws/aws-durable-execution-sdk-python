# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for WorkflowInsightPlugin record building.

Drives the plugin with the SDK's real hook dataclasses and a capturing exporter
(a test double only at the destination boundary — the plugin logic under test is
exercised end to end, nothing about SDK behavior is mocked).
"""

from __future__ import annotations

import datetime
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    InvocationStatus,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
)

from aws_durable_execution_sdk_python_insight import (
    ContentConfig,
    ContentOperations,
    OperationOverride,
    WorkflowInsightConfig,
    workflow_insight,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
T0 = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
T1 = datetime.datetime(2026, 1, 1, 0, 0, 1, tzinfo=datetime.UTC)


class CaptureExporter:
    def __init__(self, max_record_size_bytes: int | None = None, render=None) -> None:
        self.max_record_size_bytes = max_record_size_bytes
        self._render = render or (lambda r: r)
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> Any:
        return self._render(record)

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return None


def _step(
    name,
    status=OperationStatus.SUCCEEDED,
    attempt=1,
    result=None,
    error=None,
    parent_id=None,
    op_id=None,
):
    return OperationEndInfo(
        operation_id=op_id or name,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=name,
        parent_id=parent_id,
        start_time=T0,
        is_replayed=False,
        status=status,
        end_time=T1,
        result=result,
        error=error,
        attempt=attempt,
    )


def _run(
    plugin,
    *,
    ops,
    status=InvocationStatus.SUCCEEDED,
    result='"Hello, World!"',
    error=None,
    input_value="World",
):
    plugin.on_invocation_start(
        InvocationStartInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=T0,
            execution_input=input_value,
        )
    )
    for op in ops:
        plugin.on_operation_end(op)
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=T0,
            status=status,
            error=error,
            execution_result=result,
        )
    )


def test_basic_success_record():
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    _run(plugin, ops=[_step("greet")])
    assert len(exporter.records) == 1
    rec = exporter.records[0]
    assert rec["recordType"] == "WorkflowInsight"
    assert rec["schemaVersion"] == "1.0"
    assert rec["executionArn"] == ARN
    assert rec["executionName"] == "exec-1"
    assert rec["functionName"] == "my-fn"
    assert rec["status"] == "SUCCEEDED"
    assert rec["input"] == "World"
    assert rec["output"] == "Hello, World!"
    assert "error" not in rec
    assert [op["name"] for op in rec["operations"]] == ["greet"]
    op = rec["operations"][0]
    assert (
        op["type"] == "STEP" and op["subType"] == "Step" and op["status"] == "SUCCEEDED"
    )
    assert op["attempt"] == 1
    assert "result" not in op  # results omitted by default


def test_on_failure_success_emits_nothing():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-failure")
    )
    _run(plugin, ops=[_step("greet")], status=InvocationStatus.SUCCEEDED)
    assert exporter.records == []


def test_sampling_zero_emits_nothing():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=0)
    )
    _run(plugin, ops=[_step("greet")])
    assert exporter.records == []


def test_content_omit_input_output_without_drop_flags():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter], content=ContentConfig(input=False, output=False)
        )
    )
    _run(plugin, ops=[_step("greet")])
    rec = exporter.records[0]
    assert "input" not in rec and "output" not in rec
    assert "droppedInput" not in rec and "droppedOutput" not in rec


def test_result_opt_in():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            content=ContentConfig(
                operations=ContentOperations(
                    overrides=[OperationOverride("compute", result=lambda r: r)]
                )
            ),
        )
    )
    _run(plugin, ops=[_step("compute", result="42")], result="42")
    op = exporter.records[0]["operations"][0]
    assert op["result"] == 42  # checkpointed JSON string parsed


def test_include_errors_false_drops_op_error_keeps_record_error():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            content=ContentConfig(operations=ContentOperations(include_errors=False)),
        )
    )
    err = ErrorObject(message="boom", type="StepError", data=None, stack_trace=None)
    op_err = ErrorObject(
        message="boom", type="InsightTestError", data=None, stack_trace=None
    )
    _run(
        plugin,
        ops=[_step("failing-step", status=OperationStatus.FAILED, error=op_err)],
        status=InvocationStatus.FAILED,
        result=None,
        error=err,
    )
    rec = exporter.records[0]
    assert rec["error"]["name"] == "StepError"
    assert "error" not in rec["operations"][0]


def test_top_level_only_drops_children():
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    parent = OperationEndInfo(
        operation_id="p",
        operation_type=OperationType.CONTEXT,
        sub_type=OperationSubType.PARALLEL,
        name="parallel-work",
        parent_id=None,
        start_time=T0,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=T1,
    )
    child = _step("branch-a-step", parent_id="p", op_id="c")
    _run(plugin, ops=[parent, child])
    names = [op["name"] for op in exporter.records[0]["operations"]]
    assert names == ["parallel-work"]


def test_full_tree_includes_children_with_parent_id():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], operation_detail="full-tree")
    )
    parent = OperationEndInfo(
        operation_id="p",
        operation_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        name="parent-context",
        parent_id=None,
        start_time=T0,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=T1,
    )
    child = _step("child-step", parent_id="p", op_id="c")
    _run(plugin, ops=[parent, child])
    ops = {op["name"]: op for op in exporter.records[0]["operations"]}
    assert set(ops) == {"parent-context", "child-step"}
    assert ops["child-step"]["parentId"] == "p"


def test_unnamed_operation_dropped():
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    unnamed = _step(None, op_id="u")  # type: ignore[arg-type]
    _run(plugin, ops=[_step("named-step"), unnamed])
    names = [op["name"] for op in exporter.records[0]["operations"]]
    assert names == ["named-step"]
