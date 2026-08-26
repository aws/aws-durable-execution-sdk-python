# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for WorkflowInsightPlugin record building.

Drives the plugin with the SDK's real hook dataclasses and a capturing exporter
(a test double only at the destination boundary — the plugin logic under test is
exercised end to end, nothing about SDK behavior is mocked). Operations reach the
plugin the way the real SDK delivers them: as the point-in-time ``operations``
map on ``InvocationStartInfo`` / ``InvocationEndInfo`` / ``OperationChangeInfo``.
"""

from __future__ import annotations

import datetime
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
    OperationType,
)

from aws_durable_execution_sdk_python_insight import (
    ContentConfig,
    ContentOperations,
    LambdaLogExporter,
    OperationOverride,
    WorkflowInsightConfig,
    workflow_insight,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
ARN_B = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-2/inv-1"
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
    op_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    end_time=T1,
) -> OperationInfo:
    return OperationEndInfo(
        operation_id=op_id or name,
        operation_type=op_type,
        sub_type=sub_type,
        name=name,
        parent_id=parent_id,
        start_time=T0,
        is_replayed=False,
        status=status,
        end_time=end_time,
        result=result,
        error=error,
        attempt=attempt,
    )


def _ops(*ops: OperationInfo) -> dict[str, OperationInfo]:
    return {op.operation_id: op for op in ops}


def _start(
    arn=ARN,
    *,
    operations: dict[str, OperationInfo] | None = None,
    is_first=True,
    input_value="World",
    execution_start_time=T0,
) -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id=None,
        execution_arn=arn,
        is_first_invocation=is_first,
        execution_start_time=execution_start_time,
        execution_input=input_value,
        operations=operations or {},
    )


def _end(
    arn=ARN,
    *,
    operations: dict[str, OperationInfo] | None = None,
    status=InvocationStatus.SUCCEEDED,
    result='"Hello, World!"',
    error=None,
    is_first=True,
    execution_start_time=T0,
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id=None,
        execution_arn=arn,
        is_first_invocation=is_first,
        execution_start_time=execution_start_time,
        status=status,
        error=error,
        execution_result=result,
        operations=operations or {},
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
    """Single-invocation drive: the full operation map is present in both the
    start and the end snapshot (the terminal record is built from the end one)."""
    operations = _ops(*ops)
    plugin.on_invocation_start(_start(operations=operations, input_value=input_value))
    plugin.on_invocation_end(
        _end(operations=operations, status=status, result=result, error=error)
    )


# -- existing record-building coverage ---------------------------------------


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
    parent = _step(
        "parallel-work",
        op_id="p",
        op_type=OperationType.CONTEXT,
        sub_type=OperationSubType.PARALLEL,
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
    parent = _step(
        "parent-context",
        op_id="p",
        op_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
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


# -- cold resume: seed from the invocation snapshot (comment 1 + 7) ----------


def test_cold_resume_reports_prior_terminal_ops_with_fresh_plugin():
    # Invocation 1 (plugin A): a step completes, then a wait suspends -> PENDING.
    exporter1 = CaptureExporter()
    plugin1 = workflow_insight(WorkflowInsightConfig(exporters=[exporter1]))
    step = _step("greet", op_id="op-step")
    wait_pending = _step(
        "pause",
        op_id="op-wait",
        op_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        status=OperationStatus.PENDING,
        end_time=None,
    )
    plugin1.on_invocation_start(_start(operations={}))
    plugin1.on_invocation_end(
        _end(
            operations=_ops(step, wait_pending),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    assert exporter1.records == []  # on-complete emits nothing for a suspend
    assert plugin1._state == {}  # and retains nothing

    # Invocation 2 on a *fresh* plugin instance (new Lambda environment): the
    # resume start snapshot carries the prior terminal step + resolved wait.
    exporter2 = CaptureExporter()
    plugin2 = workflow_insight(WorkflowInsightConfig(exporters=[exporter2]))
    step_done = _step("greet", op_id="op-step")
    wait_done = _step(
        "pause",
        op_id="op-wait",
        op_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    resume_ops = _ops(step_done, wait_done)
    plugin2.on_invocation_start(
        _start(operations=resume_ops, is_first=False, execution_start_time=T0)
    )
    plugin2.on_invocation_end(
        _end(operations=resume_ops, is_first=False, execution_start_time=T0)
    )
    assert len(exporter2.records) == 1
    rec = exporter2.records[0]
    names = [op["name"] for op in rec["operations"]]
    assert names == ["greet", "pause"]  # prior terminal ops present on cold resume
    # Comment 7: start time is the original execution start (T0), not the resume
    # time -> duration is measured from T0 and is non-negative.
    assert rec["startTime"] == "2026-01-01T00:00:00Z"
    assert rec["durationMs"] is not None and rec["durationMs"] >= 0


# -- on-change emits an updated record per change (comment 2) ----------------


def test_on_change_emits_running_on_each_change():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    op1 = _step("s1", op_id="1")
    op2 = _step("s2", op_id="2")

    plugin.on_invocation_start(_start(operations={}))  # RUNNING #1 (start)
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op1), operations=_ops(op1)
        )
    )  # RUNNING #2
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op2), operations=_ops(op1, op2)
        )
    )  # RUNNING #3
    plugin.on_invocation_end(_end(operations=_ops(op1, op2)))  # SUCCEEDED #4

    statuses = [r["status"] for r in exporter.records]
    assert statuses == ["RUNNING", "RUNNING", "RUNNING", "SUCCEEDED"]
    # The record emitted after the 2nd change already carries both operations.
    assert [op["name"] for op in exporter.records[2]["operations"]] == ["s1", "s2"]
    final = exporter.records[-1]
    assert [op["name"] for op in final["operations"]] == ["s1", "s2"]
    # No duplicate operation entries within a record (no end/change double-count).
    ids = [op["id"] for op in final["operations"]]
    assert len(ids) == len(set(ids))


# -- no cross-execution contamination (comment 3) ----------------------------


def test_concurrent_executions_do_not_cross_contaminate():
    # A and B both suspend; B is the most-recently started (the old insertion-
    # order heuristic would have attributed A's resume to B). A then resumes to
    # a terminal state. Its record must contain only A's data.
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    a_op = _step("a-step", op_id="a1")
    b_op = _step("b-step", op_id="b1")

    plugin.on_invocation_start(_start(arn=ARN, operations={}, input_value="A"))
    plugin.on_invocation_start(_start(arn=ARN_B, operations={}, input_value="B"))
    plugin.on_invocation_end(
        _end(
            arn=ARN_B,
            operations=_ops(b_op),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    plugin.on_invocation_end(
        _end(
            arn=ARN,
            operations=_ops(a_op),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    assert exporter.records == []  # both suspended, nothing terminal yet

    a_done = _step("a-step", op_id="a1")
    plugin.on_invocation_start(
        _start(arn=ARN, operations=_ops(a_done), is_first=False, input_value="A")
    )
    plugin.on_invocation_end(
        _end(arn=ARN, operations=_ops(a_done), is_first=False, result='"A-done"')
    )

    assert len(exporter.records) == 1
    rec = exporter.records[0]
    assert rec["executionArn"] == ARN
    assert rec["input"] == "A"
    assert [op["name"] for op in rec["operations"]] == ["a-step"]


# -- state lifecycle: clear after every invocation end (comment 4) -----------


def test_state_cleared_after_pending_and_retry():
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    op = _step("s", op_id="1")

    plugin.on_invocation_start(_start(operations={}))
    plugin.on_invocation_end(
        _end(operations=_ops(op), status=InvocationStatus.PENDING, result=None)
    )
    assert plugin._state == {}  # no leak after suspend

    plugin.on_invocation_start(_start(operations=_ops(op), is_first=False))
    plugin.on_invocation_end(
        _end(
            operations=_ops(op),
            status=InvocationStatus.RETRY,
            result=None,
            is_first=False,
        )
    )
    assert plugin._state == {}  # no leak after retry
    assert exporter.records == []  # on-complete emits nothing for non-terminal


def test_sampled_out_processes_nothing_and_retains_no_state():
    exporter = CaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=0)
    )
    op = _step("s", op_id="1")
    plugin.on_invocation_start(_start(operations={}))
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    plugin.on_invocation_end(_end(operations=_ops(op)))
    assert exporter.records == []
    assert plugin._state == {}


# -- default exporter parity with JS (comment 6) -----------------------------


def test_default_exporter_when_config_omits_exporters():
    plugin = workflow_insight(WorkflowInsightConfig())
    assert len(plugin._exporters) == 1
    assert isinstance(plugin._exporters[0], LambdaLogExporter)


def test_default_exporter_when_exporters_explicitly_empty():
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[]))
    assert len(plugin._exporters) == 1
    assert isinstance(plugin._exporters[0], LambdaLogExporter)


def test_explicit_exporters_are_preserved():
    exporter = CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    assert plugin._exporters == [exporter]


def test_default_exporter_actually_emits_to_stdout(capsys):
    plugin = workflow_insight(WorkflowInsightConfig())
    _run(plugin, ops=[_step("greet")])
    out = capsys.readouterr().out
    assert '"recordType":"WorkflowInsight"' in out  # compact JSON via LambdaLogExporter
    assert '"operationsByName"' in out
