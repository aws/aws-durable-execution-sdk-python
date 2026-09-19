# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for WorkflowInsightPlugin record building.

Drives the plugin with the SDK's real hook dataclasses and a capturing exporter
(a test double only at the destination boundary — the plugin logic under test is
exercised end to end, nothing about SDK behavior is mocked). Operations reach the
plugin the way the real SDK delivers them: as the point-in-time ``operations``
map on ``InvocationStartInfo`` / ``InvocationEndInfo`` / ``OperationChangeInfo``.

``workflow_insight()`` returns a factory, so these tests hold two things where
they used to hold one: the handler-lifetime factory (``factory``, carrying the
resolved config, the exporters and the export scheduler) and the per-invocation
instance the SDK builds from it (``plugin``). ``_invocation()`` does what the SDK
does -- build the instance from the invocation's start info, then dispatch that
same info to its first hook.
"""

from __future__ import annotations

import asyncio
import datetime
import itertools
import threading
import time
from typing import Any

import pytest

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
from aws_durable_execution_sdk_python_insight._export_scheduler import _ExportState
from aws_durable_execution_sdk_python_insight.plugin import (
    WorkflowInsightPlugin,
    _resolve_sampling_rate,
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
        self.flush_count = 0

    def render(self, record: dict[str, Any]) -> Any:
        return self._render(record)

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flush_count += 1


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


def _invocation(factory, info: InvocationStartInfo) -> WorkflowInsightPlugin:
    """Enter one invocation the way the SDK does.

    The SDK builds one plugin instance per invocation from that invocation's
    start info and dispatches the very same object to its first hook. A test that
    drives hooks directly does both.
    """
    plugin = factory.create_plugin(info)
    plugin.on_invocation_start(info)
    return plugin


def _run(
    factory,
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
    plugin = _invocation(
        factory, _start(operations=operations, input_value=input_value)
    )
    plugin.on_invocation_end(
        _end(operations=operations, status=status, result=result, error=error)
    )
    return plugin


# -- existing record-building coverage ---------------------------------------


def test_basic_success_record():
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    _run(factory, ops=[_step("greet")])
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
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-failure")
    )
    _run(factory, ops=[_step("greet")], status=InvocationStatus.SUCCEEDED)
    assert exporter.records == []
    # No record, but the invocation end still flushed once: a sampled-in
    # invocation end flushes whether or not this emit mode produced a record
    # (JS/Java cadence), because the exporter may be buffering another
    # execution's records.
    assert exporter.flush_count == 1
    assert _wait_until(lambda: not factory._scheduler._worker_alive())


def test_invocation_end_that_emits_no_record_still_flushes_exactly_once():
    # on-complete mode with a PENDING end: the execution suspended, so nothing is
    # emitted. JS and Java flush once per sampled-in invocation end regardless of
    # whether a record was emitted, and a buffering exporter has to see the same
    # rhythm in every SDK, so this end must still flush -- exactly once, not
    # twice, and not zero times.
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_invocation_end(
        _end(operations={}, status=InvocationStatus.PENDING, result=None)
    )
    assert exporter.records == []
    assert exporter.flush_count == 1
    # The worker retires, so no later flush can arrive after the invocation
    # returned.
    assert _wait_until(lambda: not factory._scheduler._worker_alive())
    assert exporter.flush_count == 1


def test_sampled_out_invocation_end_neither_exports_nor_flushes():
    # The sampled-out path is the one exception to the cadence above: a sampled
    # out execution exports nothing and must not flush either, so instrumenting
    # a fraction of executions costs the rest nothing.
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=0)
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_invocation_end(_end(operations=_ops(op)))
    assert exporter.records == []
    assert exporter.flush_count == 0
    assert not factory._scheduler._worker_alive()


def test_sampling_zero_emits_nothing():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=0)
    )
    _run(factory, ops=[_step("greet")])
    assert exporter.records == []


def test_resolve_sampling_rate_nan_fails_open_to_one():
    # NaN compares False to everything; without the guard this would sample OUT
    # every execution. It must fail open to full sampling (JS parity).
    assert _resolve_sampling_rate(float("nan")) == 1.0


def test_nan_sampling_rate_emits_instead_of_silently_disabling():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=float("nan"))
    )
    _run(factory, ops=[_step("greet")])
    # A NaN rate must not disable instrumentation: the record is still emitted.
    assert len(exporter.records) == 1


def test_content_omit_input_output_without_drop_flags():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter], content=ContentConfig(input=False, output=False)
        )
    )
    _run(factory, ops=[_step("greet")])
    rec = exporter.records[0]
    assert "input" not in rec and "output" not in rec
    assert "droppedInput" not in rec and "droppedOutput" not in rec


def test_result_opt_in():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            content=ContentConfig(
                operations=ContentOperations(
                    overrides=[OperationOverride("compute", result=lambda r: r)]
                )
            ),
        )
    )
    _run(factory, ops=[_step("compute", result="42")], result="42")
    op = exporter.records[0]["operations"][0]
    assert op["result"] == 42  # checkpointed JSON string parsed


def test_include_errors_false_drops_op_error_keeps_record_error():
    exporter = CaptureExporter()
    factory = workflow_insight(
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
        factory,
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
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    parent = _step(
        "parallel-work",
        op_id="p",
        op_type=OperationType.CONTEXT,
        sub_type=OperationSubType.PARALLEL,
    )
    child = _step("branch-a-step", parent_id="p", op_id="c")
    _run(factory, ops=[parent, child])
    names = [op["name"] for op in exporter.records[0]["operations"]]
    assert names == ["parallel-work"]


def test_full_tree_includes_children_with_parent_id():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], operation_detail="full-tree")
    )
    parent = _step(
        "parent-context",
        op_id="p",
        op_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    child = _step("child-step", parent_id="p", op_id="c")
    _run(factory, ops=[parent, child])
    ops = {op["name"]: op for op in exporter.records[0]["operations"]}
    assert set(ops) == {"parent-context", "child-step"}
    assert ops["child-step"]["parentId"] == "p"


def test_unnamed_operation_dropped():
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    unnamed = _step(None, op_id="u")  # type: ignore[arg-type]
    _run(factory, ops=[_step("named-step"), unnamed])
    names = [op["name"] for op in exporter.records[0]["operations"]]
    assert names == ["named-step"]


# -- cold resume: seed from the invocation snapshot (comment 1 + 7) ----------


def test_cold_resume_reports_prior_terminal_ops_with_fresh_plugin():
    # Invocation 1 (environment A): a step completes, then a wait suspends ->
    # PENDING.
    exporter1 = CaptureExporter()
    factory1 = workflow_insight(WorkflowInsightConfig(exporters=[exporter1]))
    step = _step("greet", op_id="op-step")
    wait_pending = _step(
        "pause",
        op_id="op-wait",
        op_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        status=OperationStatus.PENDING,
        end_time=None,
    )
    plugin1 = _invocation(factory1, _start(operations={}))
    plugin1.on_invocation_end(
        _end(
            operations=_ops(step, wait_pending),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    assert exporter1.records == []  # on-complete emits nothing for a suspend
    # And nothing is retained: the instance that served the suspending invocation
    # is dropped by the SDK, and the scheduler holds no execution either.
    assert _wait_until(lambda: _scheduler_is_empty(factory1))

    # Invocation 2 in a *fresh* Lambda environment -- a new factory, and so also a
    # new instance: the resume start snapshot carries the prior terminal step +
    # resolved wait.
    exporter2 = CaptureExporter()
    factory2 = workflow_insight(WorkflowInsightConfig(exporters=[exporter2]))
    step_done = _step("greet", op_id="op-step")
    wait_done = _step(
        "pause",
        op_id="op-wait",
        op_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    resume_ops = _ops(step_done, wait_done)
    plugin2 = _invocation(
        factory2, _start(operations=resume_ops, is_first=False, execution_start_time=T0)
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


# -- on-change schedules progress and delivers terminal state ----------------


def test_on_change_schedules_running_and_delivers_terminal():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    op1 = _step("s1", op_id="1")
    op2 = _step("s2", op_id="2")

    plugin = _invocation(factory, _start(operations={}))
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op1), operations=_ops(op1)
        )
    )
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op2), operations=_ops(op1, op2)
        )
    )
    plugin.on_invocation_end(_end(operations=_ops(op1, op2)))

    statuses = [record["status"] for record in exporter.records]
    assert statuses
    assert statuses[-1] == "SUCCEEDED"
    assert set(statuses[:-1]) <= {"RUNNING"}
    final = exporter.records[-1]
    assert [op["name"] for op in final["operations"]] == ["s1", "s2"]
    ids = [op["id"] for op in final["operations"]]
    assert len(ids) == len(set(ids))


# -- no cross-execution contamination (comment 3) ----------------------------


def test_concurrent_executions_do_not_cross_contaminate():
    # A and B both suspend; B is the most-recently started (the old insertion-
    # order heuristic would have attributed A's resume to B). A then resumes to
    # a terminal state. Its record must contain only A's data. Each invocation
    # gets its own instance from the one shared factory, which is what the SDK
    # does for concurrent executions in one environment.
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    a_op = _step("a-step", op_id="a1")
    b_op = _step("b-step", op_id="b1")

    a_first = _invocation(factory, _start(arn=ARN, operations={}, input_value="A"))
    b_first = _invocation(factory, _start(arn=ARN_B, operations={}, input_value="B"))
    b_first.on_invocation_end(
        _end(
            arn=ARN_B,
            operations=_ops(b_op),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    a_first.on_invocation_end(
        _end(
            arn=ARN,
            operations=_ops(a_op),
            status=InvocationStatus.PENDING,
            result=None,
        )
    )
    assert exporter.records == []  # both suspended, nothing terminal yet

    a_done = _step("a-step", op_id="a1")
    a_resume = _invocation(
        factory,
        _start(arn=ARN, operations=_ops(a_done), is_first=False, input_value="A"),
    )
    a_resume.on_invocation_end(
        _end(arn=ARN, operations=_ops(a_done), is_first=False, result='"A-done"')
    )

    assert len(exporter.records) == 1
    rec = exporter.records[0]
    assert rec["executionArn"] == ARN
    assert rec["input"] == "A"
    assert [op["name"] for op in rec["operations"]] == ["a-step"]


# -- nothing retained after an invocation end (comment 4) --------------------


def test_nothing_retained_after_pending_and_retry():
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    op = _step("s", op_id="1")

    suspend = _invocation(factory, _start(operations={}))
    suspend.on_invocation_end(
        _end(operations=_ops(op), status=InvocationStatus.PENDING, result=None)
    )
    # The instance that served the suspending invocation is dropped by the SDK,
    # so the only thing that could retain anything for this execution is the
    # scheduler, and it holds nothing either.
    assert _wait_until(lambda: _scheduler_is_empty(factory))

    retry = _invocation(factory, _start(operations=_ops(op), is_first=False))
    retry.on_invocation_end(
        _end(
            operations=_ops(op),
            status=InvocationStatus.RETRY,
            result=None,
            is_first=False,
        )
    )
    assert _wait_until(lambda: _scheduler_is_empty(factory))
    assert exporter.records == []  # on-complete emits nothing for non-terminal


def test_sampled_out_processes_nothing_and_retains_no_state():
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], sampling_rate=0)
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    plugin.on_invocation_end(_end(operations=_ops(op)))
    assert exporter.records == []
    # A sampled-out invocation adopts no operations and schedules nothing.
    assert plugin._operations == {}
    assert _scheduler_is_empty(factory)


# -- default exporter parity with JS (comment 6) -----------------------------


def test_default_exporter_when_config_omits_exporters():
    factory = workflow_insight(WorkflowInsightConfig())
    assert len(factory._exporters) == 1
    assert isinstance(factory._exporters[0], LambdaLogExporter)


def test_default_exporter_when_exporters_explicitly_empty():
    factory = workflow_insight(WorkflowInsightConfig(exporters=[]))
    assert len(factory._exporters) == 1
    assert isinstance(factory._exporters[0], LambdaLogExporter)


def test_explicit_exporters_are_preserved():
    exporter = CaptureExporter()
    factory = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    assert factory._exporters == [exporter]


def test_default_exporter_actually_emits_to_stdout(capsys):
    factory = workflow_insight(WorkflowInsightConfig())
    _run(factory, ops=[_step("greet")])
    out = capsys.readouterr().out
    assert '"recordType":"WorkflowInsight"' in out  # compact JSON via LambdaLogExporter
    assert '"operationsByName"' in out


# -- terminal timestamps: end time only for SUCCEEDED/FAILED (comment 3) ------


def _last_record_for_end_status(status, *, emit_mode="on-change", result=None):
    """Drive one invocation to the given end status and return the last record.

    Runs in on-change mode by default so a non-terminal (PENDING/RETRY) end still
    emits a RUNNING record whose timestamps we can assert on -- on-complete would
    emit nothing for a non-terminal end.
    """
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode=emit_mode)
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_invocation_end(_end(operations=_ops(op), status=status, result=result))
    return exporter.records[-1]


def test_pending_end_maps_to_running_and_omits_end_time():
    rec = _last_record_for_end_status(InvocationStatus.PENDING)
    assert rec["status"] == "RUNNING"
    assert "endTime" not in rec
    assert "durationMs" not in rec


def test_retry_end_maps_to_running_and_omits_end_time():
    rec = _last_record_for_end_status(InvocationStatus.RETRY)
    assert rec["status"] == "RUNNING"
    assert "endTime" not in rec
    assert "durationMs" not in rec


def test_succeeded_end_is_terminal_with_end_time_and_duration():
    rec = _last_record_for_end_status(
        InvocationStatus.SUCCEEDED, result='"Hello, World!"'
    )
    assert rec["status"] == "SUCCEEDED"
    assert rec["endTime"] is not None
    assert rec["durationMs"] is not None
    assert rec["output"] == "Hello, World!"


def test_failed_end_is_terminal_with_end_time_and_duration():
    err = ErrorObject(message="boom", type="StepError", data=None, stack_trace=None)
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    op = _step("s", op_id="1", status=OperationStatus.FAILED)
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_invocation_end(
        _end(
            operations=_ops(op),
            status=InvocationStatus.FAILED,
            result=None,
            error=err,
        )
    )
    rec = exporter.records[-1]
    assert rec["status"] == "FAILED"
    assert rec["endTime"] is not None
    assert rec["durationMs"] is not None
    assert rec["error"]["name"] == "StepError"


# -- concurrent executions in one environment (LMI) ---------------------------


class ConcurrentCaptureExporter:
    """CaptureExporter for multi-threaded drives; appends under a lock."""

    max_record_size_bytes = None

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.flushes = 0
        self._lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.records.append(record)

    def flush(self) -> None:
        with self._lock:
            self.flushes += 1

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.records)


def test_concurrent_executions_each_deliver_their_terminal_record():
    # One factory (one scheduler) serves every execution its environment hosts,
    # and LMI runs several at once. Drive the real hooks concurrently, each
    # execution on its own instance: every execution's terminal record must
    # arrive exactly once.
    executions = 5
    exporter = ConcurrentCaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    arns = [
        f"arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-{index}/inv-1"
        for index in range(executions)
    ]
    ready = threading.Barrier(executions)

    def run(arn: str) -> None:
        op = _step("s", op_id="1")
        ready.wait(10.0)
        plugin = _invocation(factory, _start(arn=arn, operations={}))
        plugin.on_operation_change(
            OperationChangeInfo(
                execution_arn=arn, updated_operations=_ops(op), operations=_ops(op)
            )
        )
        plugin.on_invocation_end(_end(arn=arn, operations=_ops(op)))

    threads = [threading.Thread(target=run, args=(arn,)) for arn in arns]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30.0)
    assert not any(thread.is_alive() for thread in threads)

    terminal = [
        record["executionArn"]
        for record in exporter.snapshot()
        if record["status"] == "SUCCEEDED"
    ]
    assert sorted(terminal) == sorted(arns)  # each exactly once, none lost
    # Nothing per-execution is retained: each instance is dropped by the SDK with
    # its invocation, and the scheduler holds no execution either.
    assert _wait_until(lambda: _scheduler_is_empty(factory))


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _scheduler_is_empty(factory) -> bool:
    scheduler = factory._scheduler
    with scheduler._condition:
        # One structure: the queue of executions with a record waiting. An
        # execution's export bookkeeping lives on the per-invocation instance
        # itself, which the SDK drops when the invocation scope exits, so an
        # empty queue means the scheduler retains nothing.
        return not scheduler._pending


def _force_drain(factory) -> None:
    """Push everything this factory's instances scheduled out to the exporters.

    drain() takes the per-execution object, which in production is the plugin
    instance itself. These tests present a bare ``_ExportState`` instead, exactly
    as an execution that scheduled nothing of its own would: the flush it requests
    is held back until every record pending when it was called has been exported.
    """
    factory._scheduler.drain(_ExportState())


class PinnedWorkerExporter:
    """Blocks inside every ``export()`` until released, pinning the one worker."""

    max_record_size_bytes = None

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.entered.set()
        self.release.wait(30.0)

    def flush(self) -> None:
        pass


def test_reentrant_finalizer_in_a_hook_does_not_deadlock():
    # _emit runs the scheduler's schedule() while holding the execution's lock,
    # and schedule() releases the record it displaces inside that hold, on
    # purpose: a record can carry customer objects whose finalizers run arbitrary
    # code. A finalizer that re-enters a hook for the same execution therefore
    # re-acquires that lock on the thread that already owns it, which a
    # non-reentrant lock turns into a permanent hang of the invocation thread.
    exporter = PinnedWorkerExporter()
    reentered = threading.Event()
    holder: dict[str, Any] = {}

    class ReentrantPayload:
        """A customer object that reaches the record through a content transform."""

        def __del__(self) -> None:
            if reentered.is_set():
                return
            reentered.set()
            holder["plugin"].on_operation_change(
                OperationChangeInfo(
                    execution_arn=ARN,
                    updated_operations=holder["ops"],
                    operations=holder["ops"],
                )
            )

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=lambda _value: ReentrantPayload()),
        )
    )
    op = _step("s", op_id="1")
    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin
    holder["ops"] = _ops(op)
    change = OperationChangeInfo(
        execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
    )
    try:
        # The first emit pins the single worker inside export()...
        plugin.on_invocation_start(start)
        assert exporter.entered.wait(5.0)
        # ...so this emit stays this execution's pending record...
        plugin.on_operation_change(change)

        returned = threading.Event()

        def hook() -> None:
            # ...and this one displaces it, releasing the displaced record (and
            # running the payload's finalizer) on this thread, inside the lock.
            plugin.on_operation_change(change)
            returned.set()

        thread = threading.Thread(target=hook, daemon=True)
        thread.start()
        assert returned.wait(10.0), (
            "the hook never returned: a record finalizer that re-entered a hook "
            "for the same execution deadlocked the invocation thread"
        )
        assert reentered.is_set()  # the finalizer really did re-enter a hook
    finally:
        exporter.release.set()


# -- late hooks after the invocation ended (closed gate) ----------------------


def test_change_hook_reaching_the_lock_after_invocation_end_emits_nothing():
    # A change hook for a checkpoint that completed just before the invocation
    # ended can reach the execution's lock while on_invocation_end still holds it.
    # It must find the gate closed and emit nothing, so no RUNNING record can
    # follow the terminal one.
    exporter = ConcurrentCaptureExporter()
    in_terminal_emit = threading.Event()
    release_terminal = threading.Event()

    def blocking_output(value: Any) -> Any:
        # Runs inside _emit, which runs inside the execution's lock, and only for
        # a terminal record (a RUNNING record carries no output).
        in_terminal_emit.set()
        release_terminal.wait(10.0)
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(output=blocking_output),
        )
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))

    end_returned = threading.Event()

    def end() -> None:
        plugin.on_invocation_end(_end(operations=_ops(op)))
        end_returned.set()

    end_thread = threading.Thread(target=end, daemon=True)
    end_thread.start()
    # on_invocation_end has closed the gate and is building the terminal record,
    # still holding the execution's lock.
    assert in_terminal_emit.wait(5.0)

    change_returned = threading.Event()

    def change() -> None:
        plugin.on_operation_change(
            OperationChangeInfo(
                execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
            )
        )
        change_returned.set()

    change_thread = threading.Thread(target=change, daemon=True)
    change_thread.start()
    # It cannot get past the execution's lock while the end hook holds it.
    assert not change_returned.wait(0.25)

    release_terminal.set()
    end_thread.join(5.0)
    change_thread.join(5.0)
    assert end_returned.is_set()
    assert change_returned.is_set()
    _force_drain(factory)

    statuses = [record["status"] for record in exporter.snapshot()]
    assert "SUCCEEDED" in statuses
    # Nothing at all after the terminal record.
    assert statuses[statuses.index("SUCCEEDED") + 1 :] == []


def test_invocation_end_waits_for_an_in_flight_change_hook_emit():
    # The mirror interleaving: a change hook is already inside its emit, holding
    # the execution's lock, when the invocation ends. Closing the gate and
    # emitting the terminal record has to wait for it, otherwise the change hook
    # finishes afterwards and appends a RUNNING record after the terminal one.
    exporter = ConcurrentCaptureExporter()
    in_change_emit = threading.Event()
    release_change = threading.Event()
    calls = itertools.count()
    lock = threading.Lock()

    def blocking_input(value: Any) -> Any:
        # Runs inside _emit for every record. Block only on the change hook's
        # emit, which is the second one (invocation start emits the first).
        with lock:
            index = next(calls)
        if index == 1:
            in_change_emit.set()
            release_change.wait(10.0)
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=blocking_input),
        )
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))

    change_returned = threading.Event()

    def change() -> None:
        plugin.on_operation_change(
            OperationChangeInfo(
                execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
            )
        )
        change_returned.set()

    change_thread = threading.Thread(target=change, daemon=True)
    change_thread.start()
    assert in_change_emit.wait(5.0)

    end_returned = threading.Event()

    def end() -> None:
        plugin.on_invocation_end(_end(operations=_ops(op)))
        end_returned.set()

    end_thread = threading.Thread(target=end, daemon=True)
    end_thread.start()
    # The terminal record cannot be emitted while the change hook holds the lock.
    assert not end_returned.wait(0.25)

    release_change.set()
    change_thread.join(5.0)
    end_thread.join(10.0)
    assert change_returned.is_set()
    assert end_returned.is_set()
    _force_drain(factory)

    statuses = [record["status"] for record in exporter.snapshot()]
    assert "SUCCEEDED" in statuses
    assert statuses[statuses.index("SUCCEEDED") + 1 :] == []


def test_operation_change_after_invocation_end_emits_nothing():
    # A checkpoint that completed just before the invocation ended still delivers
    # its operation-change hook. It reaches the instance for the invocation that
    # just ended -- there is no registry it could recreate an entry in -- and must
    # find the gate closed: no fabricated start time, and no RUNNING record after
    # the terminal one.
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}))
    plugin.on_invocation_end(_end(operations=_ops(op)))
    before = list(exporter.records)
    assert before and before[-1]["status"] == "SUCCEEDED"

    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    _force_drain(factory)

    assert exporter.records == before  # nothing emitted after the terminal record
    assert [record["status"] for record in exporter.records][-1] == "SUCCEEDED"
    # No fabricated start time: every record still reports the execution start.
    assert {record["startTime"] for record in exporter.records} == {
        "2026-01-01T00:00:00Z"
    }


def test_invocation_start_after_the_gate_closed_changes_nothing():
    # Each invocation gets its own instance and exactly one invocation-start hook,
    # so a start hook arriving on a closed instance is no longer reachable through
    # a state registry -- there is none, and no instance can be handed to a second
    # invocation. The `closed` gate is what holds that contract from the plugin's
    # side: a start hook that arrives after the invocation ended must not re-seed
    # the closed instance and must emit nothing.
    exporter = CaptureExporter()
    factory = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    op = _step("s", op_id="1")
    plugin = _invocation(factory, _start(operations={}, input_value="World"))
    plugin.on_invocation_end(_end(operations=_ops(op)))
    assert plugin._closed

    plugin.on_invocation_start(
        _start(
            operations=_ops(_step("late", op_id="2")),
            input_value="late-input",
            execution_start_time=T1,
        )
    )
    _force_drain(factory)

    statuses = [record["status"] for record in exporter.records]
    assert "SUCCEEDED" in statuses
    # Nothing follows the terminal record...
    assert statuses[statuses.index("SUCCEEDED") + 1 :] == []
    # ...and the closed instance was not re-seeded. A late start that got past the
    # gate would adopt its own operation snapshot; the emission it would also have
    # produced is stopped a second time by the re-check in _emit, so the adopted
    # state is what pins the hook's own check. Input and start time are fixed by
    # the constructor from the invocation's own start info, so no later hook can
    # move them at all.
    assert plugin._cached_input == "World"
    assert plugin._start_time == T0
    assert [info.name for info in plugin._operations.values()] == ["s"]


def test_reentrant_invocation_end_stops_the_outer_running_record():
    # The gate at the top of each hook is a check-then-act, and _emit is the act.
    # Between them _emit runs customer code while holding the execution's lock --
    # here a content transform -- and the lock is reentrant, so that customer code
    # can run on_invocation_end to completion on this same thread: `_closed` set,
    # terminal record scheduled and drained. The outer frame then resumes with a
    # fully built RUNNING record, which must NOT reach the exporters after the
    # terminal one. One hook call, one instance, no concurrency.
    exporter = ConcurrentCaptureExporter()
    holder: dict[str, Any] = {}
    reentered = threading.Event()

    def reentering_input(value: Any) -> Any:
        if not reentered.is_set():
            reentered.set()
            holder["plugin"].on_invocation_end(_end(operations=_ops(_step("s"))))
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=reentering_input),
        )
    )
    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin

    # On a bounded thread, so a regression that makes the lock non-reentrant
    # again fails here instead of hanging the suite.
    returned = threading.Event()

    def hook() -> None:
        plugin.on_invocation_start(start)
        returned.set()

    thread = threading.Thread(target=hook, daemon=True)
    thread.start()
    assert returned.wait(10.0), (
        "the hook never returned: re-entering on_invocation_end from customer "
        "code inside _emit deadlocked the invocation thread"
    )
    thread.join(5.0)
    assert not thread.is_alive()
    assert reentered.is_set()  # the re-entrant end hook really did run
    # Force everything the plugin scheduled to reach the exporters, so a record
    # that slipped past the gate is observed here rather than left pending.
    _force_drain(factory)

    statuses = [record["status"] for record in exporter.snapshot()]
    assert statuses == ["SUCCEEDED"], (
        "a non-terminal record reached the exporters after the terminal one for "
        f"the same execution: {statuses}"
    )
    assert _wait_until(lambda: not factory._scheduler._worker_alive())


def test_a_reentrant_invocation_end_drains_after_the_lock_is_released():
    # The nested end hook used to drain from inside the outer frame's lock hold. A
    # drain waits for the export worker, and an exporter that re-enters a hook
    # blocks that worker on the very lock the waiting thread holds, so neither
    # side can proceed and the invocation hangs until Lambda times it out. The
    # drain a nested frame asks for is therefore deferred to the outermost hook
    # frame, which runs it with every lock hold on this thread released.
    exporter = ConcurrentCaptureExporter()
    holder: dict[str, Any] = {}
    reentered = threading.Event()
    events: list[str] = []
    lock_free_at_drain: list[bool] = []

    def reentering_input(value: Any) -> Any:
        if not reentered.is_set():
            reentered.set()
            holder["plugin"].on_invocation_end(_end(operations=_ops(_step("s"))))
            events.append("nested-end-returned")
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=reentering_input),
        )
    )
    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin

    original_drain = factory._scheduler.drain

    def recording_drain(execution: Any) -> None:
        events.append("drain")
        lock_free_at_drain.append(_free_for_another_thread(plugin._lock))
        original_drain(execution)

    factory._scheduler.drain = recording_drain  # type: ignore[method-assign]

    returned = threading.Event()

    def hook() -> None:
        plugin.on_invocation_start(start)
        returned.set()

    thread = threading.Thread(target=hook, daemon=True)
    thread.start()
    assert returned.wait(10.0), "the hook never returned"
    thread.join(5.0)
    assert not thread.is_alive()
    assert reentered.is_set()

    assert events == ["nested-end-returned", "drain"], (
        "the nested end hook drained before its frame unwound, so the drain ran "
        f"inside the outer lock hold: {events}"
    )
    assert lock_free_at_drain == [True], (
        "the drain ran while this thread still held the execution's lock, which "
        "an exporter re-entering a hook turns into a deadlock"
    )


def test_an_end_transform_that_fails_still_drains_what_was_scheduled():
    # `_emit` runs customer code between the snapshot and the hand-off: the content
    # transforms, a result override, and __del__ on an object a displaced record
    # carried. A failure there leaves the hook through the SDK's containment, and
    # the drain used to be requested after the build, so that failure skipped it --
    # leaving records this execution had already scheduled in a buffering exporter
    # when the environment froze. The drain is now requested on entry.
    exporter = ConcurrentCaptureExporter()
    calls: list[str] = []

    def failing_on_the_second_call(value: Any) -> Any:
        calls.append("input")
        if len(calls) > 1:
            # CancelledError rather than an ordinary exception on purpose:
            # _apply_data_content contains Exception so that a failing redactor
            # cannot leak the raw value, and a BaseException is what escapes the
            # build and leaves the hook.
            raise asyncio.CancelledError("transform cancelled")
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=failing_on_the_second_call),
        )
    )
    start = _start(operations={})
    plugin = factory.create_plugin(start)

    # The first emit succeeds and schedules a RUNNING record.
    plugin.on_invocation_start(start)

    # The terminal emit fails inside the transform, so this hook raises. The SDK
    # contains that; here it is raised directly, which is the same code path.
    with pytest.raises(asyncio.CancelledError):
        plugin.on_invocation_end(_end(operations=_ops(_step("s"))))

    statuses = [record["status"] for record in exporter.snapshot()]
    assert statuses == ["RUNNING"], (
        "the record scheduled before the failing transform must have been drained "
        f"to the exporters, not left pending: {statuses}"
    )
    assert exporter.flushes >= 1, "the drain must have flushed"
    assert _wait_until(lambda: not factory._scheduler._worker_alive())


def _free_for_another_thread(lock: Any) -> bool:
    """Report whether a lock is unheld, as seen from a thread that never took it.

    Asked from another thread on purpose: the lock is reentrant, so the thread
    that owns it can always acquire it again and would learn nothing.
    """
    acquired: list[bool] = []

    def probe() -> None:
        got = lock.acquire(blocking=False)
        acquired.append(got)
        if got:
            lock.release()

    prober = threading.Thread(target=probe, daemon=True)
    prober.start()
    prober.join(5.0)
    return acquired == [True]


# -- a build overtaken by one customer code started from inside it -------------


class GatedCaptureExporter:
    """Records every export; optionally blocks inside one execution's export.

    Blocking there pins the single export worker, so records scheduled while it
    is held stay in their execution's pending slot and coalesce there.
    """

    max_record_size_bytes = None

    def __init__(self, hold_arn: str | None = None) -> None:
        self._hold_arn = hold_arn
        self.holding = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()
        self._arrived = threading.Condition(self._lock)
        self._records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        if self._hold_arn is not None and record["executionArn"] == self._hold_arn:
            self.holding.set()
            self.release.wait(30.0)
        with self._arrived:
            self._records.append(record)
            self._arrived.notify_all()

    def flush(self) -> None:
        pass

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._records)

    def wait_for(self, arn: str, count: int, timeout: float = 10.0) -> bool:
        with self._arrived:
            return self._arrived.wait_for(
                lambda: (
                    sum(1 for r in self._records if r["executionArn"] == arn) >= count
                ),
                timeout,
            )


def _records_for(exporter: GatedCaptureExporter, arn: str) -> list[dict[str, Any]]:
    return [record for record in exporter.snapshot() if record["executionArn"] == arn]


def _force_drain_bounded(factory, timeout: float = 20.0) -> None:
    """``_force_drain`` on a bounded thread, so a stalled drain fails the test.

    ``drain()`` parks on a condition with no deadline: that is correct in
    production, where the only thing that can release it is the export worker, but
    a regression that loses a record leaves it parked for good. Running it on
    another thread turns that into an assertion failure instead of a hung suite.
    """
    drained = threading.Event()

    def drain() -> None:
        _force_drain(factory)
        drained.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    assert drained.wait(timeout), "the drain never returned"
    thread.join(5.0)
    assert not thread.is_alive()


def _exported_operation_names(
    exporter: GatedCaptureExporter, arn: str
) -> list[list[str]]:
    return [
        [op["name"] for op in record["operations"]]
        for record in _records_for(exporter, arn)
    ]


def _newer_change() -> OperationChangeInfo:
    """An operation-change carrying a strictly newer map than the drives below start from."""
    return OperationChangeInfo(
        execution_arn=ARN,
        updated_operations=_ops(_step("s1")),
        operations=_ops(_step("s1")),
    )


def test_a_build_overtaken_by_a_nested_one_does_not_coalesce_the_newer_away():
    # _emit runs customer code -- here the content.input transform -- between the
    # operation snapshot it takes and the hand-off to the scheduler, and the
    # execution's lock is reentrant, so that code can run on_operation_change to
    # completion on this same thread. The nested hook adopts a newer operation map
    # and schedules its record first. The outer frame then hands over the older
    # snapshot it built, and the pending slot takes whichever record arrives last,
    # so without a comparison of build ages the newer record is coalesced away and
    # the exporter only ever sees the older one. One hook call, one instance, no
    # concurrency.
    exporter = GatedCaptureExporter(hold_arn=ARN_B)
    holder: dict[str, Any] = {}
    reentered = threading.Event()

    def reentering_input(value: Any) -> Any:
        # The primer execution below runs this same transform, so re-enter only
        # once the instance under test has been published.
        if holder.get("plugin") is None or reentered.is_set():
            return value
        reentered.set()
        holder["plugin"].on_operation_change(_newer_change())
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=reentering_input),
        )
    )
    # Pin the one export worker on another execution's record, so both records
    # this execution builds are still in its pending slot when the outer frame
    # hands its own over.
    primer = factory.create_plugin(_start(arn=ARN_B))
    primer.on_invocation_start(_start(arn=ARN_B))
    assert exporter.holding.wait(10.0), "the export worker never reached the exporter"

    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin

    # On a bounded thread, so a regression that deadlocks the hook fails here
    # instead of hanging the suite.
    returned = threading.Event()

    def hook() -> None:
        plugin.on_invocation_start(start)
        returned.set()

    thread = threading.Thread(target=hook, daemon=True)
    thread.start()
    assert returned.wait(10.0), "the hook never returned"
    thread.join(5.0)
    assert not thread.is_alive()
    assert reentered.is_set()  # the nested change hook really did run
    exporter.release.set()
    _force_drain_bounded(factory)

    assert _exported_operation_names(exporter, ARN) == [["s1"]], (
        "the newer snapshot the nested hook built was coalesced away by the "
        "older one the outer frame built: "
        f"{_exported_operation_names(exporter, ARN)}"
    )


def test_a_build_overtaken_by_a_nested_one_is_not_exported_after_it():
    # Same overtaking, with the newer record already handed to the exporter before
    # the outer frame reaches the hand-off. Nothing coalesces, so without a
    # comparison of build ages the exporter sees the newer snapshot and then the
    # older one, and an exporter that upserts by execution ARN ends up storing the
    # older state.
    exporter = GatedCaptureExporter()
    holder: dict[str, Any] = {}
    reentered = threading.Event()

    def reentering_input(value: Any) -> Any:
        if reentered.is_set():
            return value
        reentered.set()
        holder["plugin"].on_operation_change(_newer_change())
        # Wait for the newer record to reach the exporter, so this frame's older
        # record cannot displace it in the pending slot and the two records are
        # ordered at the exporter instead.
        assert exporter.wait_for(ARN, 1), "the nested record never reached the exporter"
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=reentering_input),
        )
    )
    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin

    returned = threading.Event()

    def hook() -> None:
        plugin.on_invocation_start(start)
        returned.set()

    thread = threading.Thread(target=hook, daemon=True)
    thread.start()
    assert returned.wait(10.0), "the hook never returned"
    thread.join(5.0)
    assert not thread.is_alive()
    assert reentered.is_set()
    _force_drain_bounded(factory)

    assert _exported_operation_names(exporter, ARN) == [["s1"]], (
        "the exporter saw the older snapshot after the newer one for the same "
        f"execution: {_exported_operation_names(exporter, ARN)}"
    )


def test_the_closing_record_is_exempt_from_the_build_age_check():
    # The closing record must reach the exporters whatever the build ages say.
    # Customer code inside its build can start a newer non-terminal build, which
    # would leave the closing record's age stale, and a checked hand-off would
    # then drop it and leave a RUNNING snapshot as this execution's last exported
    # state. Nothing else can rescue it: it is the last record this instance ever
    # builds.
    #
    # The drive raises the build counter above the closing record's own first, by
    # overtaking one build with a nested one exactly as the two tests above do, so
    # a closing record that is age-checked against a counter it never incremented
    # is dropped here.
    exporter = GatedCaptureExporter()
    holder: dict[str, Any] = {}
    reentered = threading.Event()

    def reentering_input(value: Any) -> Any:
        if reentered.is_set():
            return value
        reentered.set()
        holder["plugin"].on_operation_change(_newer_change())
        assert exporter.wait_for(ARN, 1), "the nested record never reached the exporter"
        return value

    factory = workflow_insight(
        WorkflowInsightConfig(
            exporters=[exporter],
            emit_mode="on-change",
            content=ContentConfig(input=reentering_input),
        )
    )
    start = _start(operations={})
    plugin = factory.create_plugin(start)
    holder["plugin"] = plugin

    returned = threading.Event()

    def hook() -> None:
        plugin.on_invocation_start(start)
        # Push whatever the start hook handed over to the exporter before the end
        # hook schedules the closing record, so a record the outer frame scheduled
        # is observed here instead of being coalesced away by the closing one.
        _force_drain(factory)
        # on_invocation_end drains, so every record is at the exporter once this
        # returns.
        plugin.on_invocation_end(_end(operations=_ops(_step("s1"), _step("s2"))))
        returned.set()

    thread = threading.Thread(target=hook, daemon=True)
    thread.start()
    assert returned.wait(10.0), "the hooks never returned"
    thread.join(5.0)
    assert not thread.is_alive()
    assert reentered.is_set()

    records = _records_for(exporter, ARN)
    assert [record["status"] for record in records] == ["RUNNING", "SUCCEEDED"], (
        "the closing record was dropped, or the superseded RUNNING record was "
        f"exported: {[record['status'] for record in records]}"
    )
    assert [op["name"] for op in records[0]["operations"]] == ["s1"]
    assert sorted(op["name"] for op in records[1]["operations"]) == ["s1", "s2"]
