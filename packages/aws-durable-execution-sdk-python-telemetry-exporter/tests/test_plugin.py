# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for DurableExecutionTelemetryPlugin record building.

Drives the plugin with the SDK's real hook dataclasses and a capturing exporter
(a test double only at the destination boundary — the plugin logic under test is
exercised end to end, nothing about SDK behavior is mocked). Operations reach the
plugin the way the real SDK delivers them: as the point-in-time ``operations``
map on ``InvocationStartInfo`` / ``InvocationEndInfo`` / ``OperationChangeInfo``.
"""

from __future__ import annotations

import datetime
import decimal
import json
import math
import threading
import time
import uuid
from typing import Any

import pytest
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.config import BatchItemStatus
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
from aws_durable_execution_sdk_python.serdes import (
    EXTENDED_TYPES_SERDES,
    SerDesContext,
)

from aws_durable_execution_sdk_python_telemetry_exporter import (
    ContentConfig,
    ContentOperations,
    LambdaLogExporter,
    OperationOverride,
    DurableExecutionTelemetryConfig,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_telemetry_exporter import (
    plugin as plugin_module,
)
from aws_durable_execution_sdk_python_telemetry_exporter.plugin import (
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
    _run(plugin, ops=[_step("greet")])
    assert len(exporter.records) == 1
    rec = exporter.records[0]
    assert rec["recordType"] == "DurableExecutionTelemetry"
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode="on-failure")
    )
    _run(plugin, ops=[_step("greet")], status=InvocationStatus.SUCCEEDED)
    assert exporter.records == []
    assert exporter.flush_count == 0
    assert not plugin._scheduler._worker_alive()


def test_sampling_zero_emits_nothing():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], sampling_rate=0)
    )
    _run(plugin, ops=[_step("greet")])
    assert exporter.records == []


def test_resolve_sampling_rate_nan_fails_open_to_one():
    # NaN compares False to everything; without the guard this would sample OUT
    # every execution. It must fail open to full sampling (JS parity).
    assert _resolve_sampling_rate(float("nan")) == 1.0


def test_nan_sampling_rate_emits_instead_of_silently_disabling():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter], sampling_rate=float("nan")
        )
    )
    _run(plugin, ops=[_step("greet")])
    # A NaN rate must not disable instrumentation: the record is still emitted.
    assert len(exporter.records) == 1


def test_content_omit_input_output_without_drop_flags():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter], content=ContentConfig(input=False, output=False)
        )
    )
    _run(plugin, ops=[_step("greet")])
    rec = exporter.records[0]
    assert "input" not in rec and "output" not in rec
    assert "droppedInput" not in rec and "droppedOutput" not in rec


def test_result_opt_in():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter],
            content=ContentConfig(operations=ContentOperations(include_errors=False)),
        )
    )
    err = ErrorObject(message="boom", type="StepError", data=None, stack_trace=None)
    op_err = ErrorObject(
        message="boom", type="TelemetryTestError", data=None, stack_trace=None
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter], operation_detail="full-tree"
        )
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
    unnamed = _step(None, op_id="u")  # type: ignore[arg-type]
    _run(plugin, ops=[_step("named-step"), unnamed])
    names = [op["name"] for op in exporter.records[0]["operations"]]
    assert names == ["named-step"]


# -- cold resume: seed from the invocation snapshot (comment 1 + 7) ----------


def test_cold_resume_reports_prior_terminal_ops_with_fresh_plugin():
    # Invocation 1 (plugin A): a step completes, then a wait suspends -> PENDING.
    exporter1 = CaptureExporter()
    plugin1 = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter1])
    )
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
    plugin2 = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter2])
    )
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


# -- on-change delivers every record when nothing coalesces ------------------


def test_on_change_delivers_every_record_when_drained_between_hooks():
    # Draining after each hook removes the worker race, so this pins the
    # "no back-pressure means no loss" contract: one RUNNING per start/change
    # plus the terminal record, in order. (The coalescing contract itself is
    # pinned in test_export_scheduler.py with a blocked exporter.)
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode="on-change")
    )
    op1 = _step("s1", op_id="1")
    op2 = _step("s2", op_id="2")

    plugin.on_invocation_start(_start(operations={}))
    plugin._scheduler.drain()
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op1), operations=_ops(op1)
        )
    )
    plugin._scheduler.drain()
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op2), operations=_ops(op1, op2)
        )
    )
    plugin._scheduler.drain()
    plugin.on_invocation_end(_end(operations=_ops(op1, op2)))

    statuses = [record["status"] for record in exporter.records]
    assert statuses == ["RUNNING", "RUNNING", "RUNNING", "SUCCEEDED"]
    final = exporter.records[-1]
    assert [op["name"] for op in final["operations"]] == ["s1", "s2"]
    ids = [op["id"] for op in final["operations"]]
    assert len(ids) == len(set(ids))


# -- concurrent executions on one plugin keep their terminal records ---------


@pytest.mark.parametrize("emit_mode", ["on-complete", "on-change"])
def test_concurrent_executions_on_one_plugin_each_deliver_terminal_record(
    emit_mode,
):
    # Two executions in flight on one plugin (as the local runner does). Each
    # invocation end must deliver its own terminal record; a shared pending
    # slot let one execution's RUNNING snapshot displace the other's terminal.
    rounds = 50
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode=emit_mode)
    )

    def drive(arn: str) -> None:
        op = _step(f"{arn}-step", op_id=f"{arn}-1")
        for _ in range(rounds):
            plugin.on_invocation_start(_start(arn, operations={}))
            plugin.on_operation_change(
                OperationChangeInfo(
                    execution_arn=arn, updated_operations=_ops(op), operations=_ops(op)
                )
            )
            plugin.on_invocation_end(_end(arn, operations=_ops(op)))

    threads = [threading.Thread(target=drive, args=(arn,)) for arn in (ARN, ARN_B)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30.0)
    assert not any(thread.is_alive() for thread in threads)

    for arn in (ARN, ARN_B):
        terminal = [
            r
            for r in exporter.records
            if r["executionArn"] == arn and r["status"] == "SUCCEEDED"
        ]
        assert len(terminal) == rounds
    assert plugin._state == {}


# -- no cross-execution contamination (comment 3) ----------------------------


def test_concurrent_executions_do_not_cross_contaminate():
    # A and B both suspend; B is the most-recently started (the old insertion-
    # order heuristic would have attributed A's resume to B). A then resumes to
    # a terminal state. Its record must contain only A's data.
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], sampling_rate=0)
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
    plugin = durable_execution_telemetry(DurableExecutionTelemetryConfig())
    assert len(plugin._exporters) == 1
    assert isinstance(plugin._exporters[0], LambdaLogExporter)


def test_default_exporter_when_exporters_explicitly_empty():
    plugin = durable_execution_telemetry(DurableExecutionTelemetryConfig(exporters=[]))
    assert len(plugin._exporters) == 1
    assert isinstance(plugin._exporters[0], LambdaLogExporter)


def test_explicit_exporters_are_preserved():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
    assert plugin._exporters == [exporter]


def test_default_exporter_actually_emits_to_stdout(capsys):
    plugin = durable_execution_telemetry(DurableExecutionTelemetryConfig())
    _run(plugin, ops=[_step("greet")])
    out = capsys.readouterr().out
    # Lambda JSON log envelope whose message is the compact record JSON.
    line = json.loads(out.strip())
    record = json.loads(line["message"])
    assert record["recordType"] == "DurableExecutionTelemetry"
    assert "operationsByName" in record


# -- endTime/durationMs: every invocation-end record carries them (JS parity) -


def _last_record_for_end_status(status, *, emit_mode="on-change", result=None):
    """Drive one invocation to the given end status and return the last record.

    Runs in on-change mode by default so a non-terminal (PENDING/RETRY) end still
    emits a RUNNING record whose timestamps we can assert on -- on-complete would
    emit nothing for a non-terminal end.
    """
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode=emit_mode)
    )
    op = _step("s", op_id="1")
    plugin.on_invocation_start(_start(operations={}))
    plugin.on_invocation_end(_end(operations=_ops(op), status=status, result=result))
    return exporter.records[-1]


def test_pending_end_maps_to_running_with_end_time_and_duration():
    # JS onInvocationEnd sets endTime for every status, so a suspended
    # invocation's RUNNING record carries endTime/durationMs up to the suspend.
    rec = _last_record_for_end_status(InvocationStatus.PENDING)
    assert rec["status"] == "RUNNING"
    assert rec["endTime"].endswith("Z")
    assert isinstance(rec["durationMs"], int) and rec["durationMs"] >= 0
    assert "output" not in rec and "error" not in rec


def test_retry_end_maps_to_running_with_end_time_and_duration():
    rec = _last_record_for_end_status(InvocationStatus.RETRY)
    assert rec["status"] == "RUNNING"
    assert rec["endTime"].endswith("Z")
    assert isinstance(rec["durationMs"], int) and rec["durationMs"] >= 0
    assert "output" not in rec and "error" not in rec


def test_start_and_operation_change_records_have_no_end_time():
    exporter = CaptureExporter()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode="on-change")
    )
    op = _step("s", op_id="1")
    plugin.on_invocation_start(_start(operations={}))
    plugin._scheduler.drain()
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    plugin._scheduler.drain()
    plugin.on_invocation_end(
        _end(operations=_ops(op), status=InvocationStatus.PENDING, result=None)
    )
    assert [r["status"] for r in exporter.records] == ["RUNNING"] * 3
    start_rec, change_rec, end_rec = exporter.records
    for rec in (start_rec, change_rec):
        assert "endTime" not in rec and "durationMs" not in rec
    assert "endTime" in end_rec and "durationMs" in end_rec


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
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode="on-change")
    )
    op = _step("s", op_id="1", status=OperationStatus.FAILED)
    plugin.on_invocation_start(_start(operations={}))
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


# -- opted-in operation results are the plain JSON value (JS parity) ---------


def _serialize(value: Any) -> str:
    """Checkpoint a step result exactly as the SDK's default serializer does."""
    return EXTENDED_TYPES_SERDES.serialize(value, SerDesContext("1", ARN))


def _override_plugin(exporter, transform=lambda r: r, name="charge"):
    return durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter],
            content=ContentConfig(
                operations=ContentOperations(
                    overrides=[OperationOverride(name, result=transform)]
                )
            ),
        )
    )


def _charge_result(raw: str | None, transform=lambda r: r) -> Any:
    exporter = CaptureExporter()
    _run(
        _override_plugin(exporter, transform),
        ops=[_step("charge", result=raw)],
    )
    op = exporter.records[0]["operations"][0]
    return op.get("result", "<absent>")


def test_result_envelope_is_decoded_to_plain_json():
    raw = _serialize({"amount": 99.99})
    assert json.loads(raw) == {"t": "m", "v": {"amount": {"t": "f", "v": 99.99}}}
    assert _charge_result(raw) == {"amount": 99.99}


def test_override_transform_receives_the_plain_value():
    seen: list[Any] = []

    def transform(value: Any) -> Any:
        seen.append(value)
        return {"masked": value["amount"] > 50}

    result = _charge_result(_serialize({"amount": 99.99}), transform)
    assert seen == [{"amount": 99.99}]
    assert result == {"masked": True}


def test_result_non_json_types_become_json_compatible_values():
    value = {
        "price": decimal.Decimal("99.99"),
        "qty": decimal.Decimal("3"),
        "nan": decimal.Decimal("NaN"),
        "at": datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.UTC),
        "day": datetime.date(2026, 1, 2),
        "id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "blob": b"\x00\x01",
        "pair": (1, "a"),
        "nested": [{"k": [1, {"deep": True}]}, None],
    }
    assert _charge_result(_serialize(value)) == {
        "price": 99.99,
        "qty": 3,
        "nan": None,
        "at": "2026-01-02T03:04:05.123456+00:00",
        "day": "2026-01-02",
        "id": "12345678-1234-5678-1234-567812345678",
        "blob": "AAE=",
        "pair": [1, "a"],
        "nested": [{"k": [1, {"deep": True}]}, None],
    }


def test_result_top_level_non_json_type_is_converted():
    assert _charge_result(_serialize(decimal.Decimal("12.5"))) == 12.5
    assert _charge_result(_serialize(("x", 2))) == ["x", 2]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # A float is used only when it converts back to exactly the same Decimal.
        (decimal.Decimal("99.99"), 99.99),
        (decimal.Decimal("-0.5"), -0.5),
        (decimal.Decimal("3"), 3),
        (decimal.Decimal("12345678901234567890"), 12345678901234567890),
        # Otherwise the exact string form, so precision and sign are kept.
        (
            decimal.Decimal("3.14159265358979323846264338327950288"),
            "3.14159265358979323846264338327950288",
        ),
        (decimal.Decimal("12345678901234567.89"), "12345678901234567.89"),
        (decimal.Decimal("1E-400"), "1E-400"),
        (decimal.Decimal("-0.0"), -0.0),
        (decimal.Decimal("-Infinity"), None),
        (decimal.Decimal("sNaN"), None),
        # The numeric value is kept, not the scale.
        (decimal.Decimal("1.10"), 1.1),
        (decimal.Decimal("1E+2"), 100),
        # An integral Decimal of up to 300 digits is an integer; a longer one
        # keeps its string form.
        (decimal.Decimal("9" * 300), int("9" * 300)),
        (decimal.Decimal("1" + "0" * 300), 1e300),
        (decimal.Decimal("1" * 301), "1" * 301),
    ],
)
def test_result_decimal_keeps_its_exact_value(value, expected):
    result = _charge_result(_serialize({"d": value}))["d"]
    assert result == expected
    assert type(result) is type(expected)
    if isinstance(expected, float):
        assert math.copysign(1, result) == math.copysign(1, expected)


@pytest.mark.parametrize(
    "raw",
    [
        _serialize({"x": float("nan"), "y": float("inf"), "z": [float("-inf"), 1.5]}),
        '{"x": NaN, "y": Infinity, "z": [-Infinity, 1.5]}',  # plain JSON payload
    ],
)
def test_result_non_finite_floats_become_null(raw):
    exporter = CaptureExporter(render=LambdaLogExporter().render)
    _run(_override_plugin(exporter), ops=[_step("charge", result=raw)])
    result = exporter.records[0]["operations"][0]["result"]
    assert result == {"x": None, "y": None, "z": [None, 1.5]}
    # The emitted line is strict JSON.
    line = json.dumps(exporter.render(exporter.records[0]), allow_nan=False)
    assert "NaN" not in line and "Infinity" not in line


@pytest.mark.parametrize("exponent", [5000, 1_000_000])
def test_result_decimal_with_huge_exponent_is_fast_and_emitted(exponent):
    value = decimal.Decimal(f"1E+{exponent}")
    exporter = CaptureExporter(render=LambdaLogExporter().render)
    start = time.perf_counter()
    _run(_override_plugin(exporter), ops=[_step("charge", result=_serialize({"x": value}))])
    line = json.dumps(exporter.render(exporter.records[0]), allow_nan=False)
    elapsed = time.perf_counter() - start
    assert exporter.records[0]["operations"][0]["result"] == {"x": f"1E+{exponent}"}
    assert json.loads(line)
    # Converting to int would take tens of seconds for the larger value.
    assert elapsed < 1


def test_result_batch_result_becomes_its_dict_form():
    batch = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result={"a": 1})],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )
    assert _charge_result(_serialize(batch)) == {
        "all": [{"index": 0, "status": "SUCCEEDED", "result": {"a": 1}, "error": None}],
        "completionReason": "ALL_COMPLETED",
    }


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("42", 42),  # primitives are plain JSON already
        ('"hi"', "hi"),
        ("[1,2,3]", [1, 2, 3]),
        ('{"amount":1}', {"amount": 1}),  # e.g. a custom JSON SerDes
        ('{"t":"m","v":{},"x":1}', {"t": "m", "v": {}, "x": 1}),  # not an envelope
        ("not json", "not json"),  # raw string fallback
    ],
)
def test_result_plain_payloads_keep_previous_behaviour(raw, expected):
    assert _charge_result(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        '{"t":"zz","v":1}',  # unknown tag
        '{"t":"d","v":"not-a-decimal"}',  # decode failure
        '{"t":"m","v":[1,2]}',  # wrong payload for the tag
        '{"t":1,"v":2}',  # non-string tag
    ],
)
def test_result_undecodable_envelope_falls_back_to_parsed_json(raw):
    assert _charge_result(raw) == json.loads(raw)


def test_result_unsupported_decoded_type_falls_back_to_parsed_json(monkeypatch):
    class _OpaqueSerDes:
        def deserialize(self, data: str, context: Any) -> Any:
            return object()

    monkeypatch.setattr(plugin_module, "EXTENDED_TYPES_SERDES", _OpaqueSerDes())
    raw = '{"t":"m","v":{}}'
    assert _charge_result(raw) == {"t": "m", "v": {}}


def test_result_in_operations_by_name_is_plain():
    exporter = CaptureExporter(render=LambdaLogExporter().render)
    _run(
        _override_plugin(exporter),
        ops=[_step("charge", result=_serialize({"amount": 99.99}))],
    )
    rendered = exporter.render(exporter.records[0])
    assert rendered["operationsByName"]["charge"]["result"] == {"amount": 99.99}
