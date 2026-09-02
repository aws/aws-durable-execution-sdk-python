# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Plugin-level tests for the asynchronous export path.

These drive the plugin through the real SDK hook dataclasses and assert the
scheduler-backed behavior the design requires: non-``on-change`` modes do no
operation-change work, a blocked exporter never blocks a hook, the
invocation-end drain is bounded by ``export_timeout_seconds``, and a buffered
exporter only publishes after the invocation-end flush.
"""

from __future__ import annotations

import datetime
import threading
import time
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import (
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
    WorkflowInsightConfig,
    workflow_insight,
)


ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
ARN_A = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-a/inv-1"
ARN_B = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-b/inv-1"
T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
T1 = datetime.datetime(2026, 1, 1, 0, 0, 1, tzinfo=datetime.UTC)


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _step(name: str, op_id: str) -> OperationInfo:
    return OperationEndInfo(
        operation_id=op_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=name,
        parent_id=None,
        start_time=T0,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=T1,
        result=None,
        error=None,
        attempt=1,
    )


def _ops(*ops: OperationInfo) -> dict[str, OperationInfo]:
    return {op.operation_id: op for op in ops}


def _start(operations: dict[str, OperationInfo]) -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=T0,
        execution_input="World",
        operations=operations,
    )


def _end(operations: dict[str, OperationInfo]) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=T0,
        status=InvocationStatus.SUCCEEDED,
        error=None,
        execution_result='"Hello, World!"',
        operations=operations,
    )


def _start_arn(arn: str, operations: dict[str, OperationInfo]) -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id=None,
        execution_arn=arn,
        is_first_invocation=True,
        execution_start_time=T0,
        execution_input="World",
        operations=operations,
    )


def _end_arn(arn: str, operations: dict[str, OperationInfo]) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id=None,
        execution_arn=arn,
        is_first_invocation=True,
        execution_start_time=T0,
        status=InvocationStatus.SUCCEEDED,
        error=None,
        execution_result='"Hello, World!"',
        operations=operations,
    )


class _BlockingExporter:
    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self._release = threading.Event()
        self.started = threading.Event()
        self.exported: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self._release.wait(5.0)
        self.exported.append(record)

    def flush(self) -> None:
        return None

    def release(self) -> None:
        self._release.set()


class _BufferedExporter:
    """Buffers exports and only publishes them when ``flush`` is called."""

    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self._buffer: list[dict[str, Any]] = []
        self.published: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self._buffer.append(record)

    def flush(self) -> None:
        self.published.extend(self._buffer)
        self._buffer.clear()


# -- non-on-change modes do no operation-change work -------------------------


def test_non_on_change_mode_skips_operation_change_work():
    exporter = _BufferedExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter])
    )  # on-complete
    op = _step("s", "1")
    plugin.on_invocation_start(_start({}))
    # An operation-change in a non-on-change mode must not create/adopt state.
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    state = plugin._state.get(ARN)
    assert state is not None and state.operations == {}  # snapshot not adopted
    assert state.scheduled is False  # nothing scheduled on the change


# -- a blocked exporter never blocks a hook ----------------------------------


def test_operation_change_returns_immediately_with_blocked_exporter():
    exporter = _BlockingExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], emit_mode="on-change")
    )
    plugin.on_invocation_start(_start({}))  # schedules RUNNING; worker blocks on it
    assert _wait_until(exporter.started.is_set)
    op = _step("s", "1")
    start = time.monotonic()
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN, updated_operations=_ops(op), operations=_ops(op)
        )
    )
    assert time.monotonic() - start < 0.5  # returned without waiting on the export
    exporter.release()
    plugin.on_invocation_end(_end(_ops(op)))


# -- invocation-end drain is bounded by export_timeout_seconds ----------------


def test_invocation_end_bounded_by_export_timeout():
    exporter = _BlockingExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], export_timeout_seconds=0.2)
    )
    op = _step("s", "1")
    plugin.on_invocation_start(_start({}))
    assert not exporter.started.is_set()  # on-complete: nothing scheduled at start
    start = time.monotonic()
    plugin.on_invocation_end(_end(_ops(op)))  # schedules terminal; worker blocks
    elapsed = time.monotonic() - start
    assert elapsed < 2.0  # bounded by the 0.2s shared deadline
    assert plugin._state == {}  # state cleared even on degraded delivery
    exporter.release()


# -- buffered exporter publishes only after the invocation-end flush ----------


def test_buffered_exporter_publishes_after_flush():
    exporter = _BufferedExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    op = _step("s", "1")
    plugin.on_invocation_start(_start({}))
    plugin.on_invocation_end(_end(_ops(op)))  # drains + flushes before returning
    assert len(exporter.published) == 1
    assert exporter.published[0]["status"] == "SUCCEEDED"


# -- warm-container cross-invocation isolation --------------------------------


class _OrderedBlockingExporter:
    """Blocks the first export until released; records export order and flushes.

    Once released, subsequent exports return immediately (the release event stays
    set), so a lane can drain a backlog without re-blocking.
    """

    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self._release = threading.Event()
        self.started = threading.Event()
        self._lock = threading.Lock()
        self.exported: list[str] = []
        self.flush_calls = 0

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self._release.wait(5.0)
        with self._lock:
            self.exported.append(record.get("executionArn", ""))

    def flush(self) -> None:
        with self._lock:
            self.flush_calls += 1

    def release(self) -> None:
        self._release.set()

    def exported_arns(self) -> list[str]:
        with self._lock:
            return list(self.exported)


def test_warm_container_cross_invocation_isolation_and_ordering():
    # One warm plugin instance handles two executions on the same exporter lane.
    # Execution A blocks the lane and times out at invocation end; execution B
    # schedules behind it. Both invocation-end waits stay bounded, B is neither
    # lost nor merged into A, and after unblock the lane drains FIFO and flushes.
    exporter = _OrderedBlockingExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(exporters=[exporter], export_timeout_seconds=0.2)
    )
    op = _step("s", "1")

    # -- Execution A: terminal end schedules a record; the lane blocks on it. --
    plugin.on_invocation_start(_start_arn(ARN_A, {}))
    a_start = time.monotonic()
    plugin.on_invocation_end(_end_arn(ARN_A, _ops(op)))  # blocks; times out
    a_elapsed = time.monotonic() - a_start
    assert a_elapsed < 2.0  # bounded by the shared 0.2s deadline, not the export
    assert _wait_until(exporter.started.is_set)  # A is in flight
    assert exporter.exported_arns() == []  # still blocked -> nothing delivered

    # -- Execution B arrives on the warm container while A is blocked. --------
    plugin.on_invocation_start(_start_arn(ARN_B, {}))
    exporter.release()  # let the lane drain A first
    assert _wait_until(lambda: exporter.exported_arns() == [ARN_A])

    b_start = time.monotonic()
    plugin.on_invocation_end(_end_arn(ARN_B, _ops(op)))  # bounded; drains + flush
    b_elapsed = time.monotonic() - b_start
    assert b_elapsed < 2.0

    # B was delivered as its own record after A (FIFO), never merged into A.
    assert _wait_until(lambda: exporter.exported_arns() == [ARN_A, ARN_B])
    # B's invocation-end flush completed (its barrier was not cancelled).
    assert _wait_until(lambda: exporter.flush_calls >= 1)
    assert plugin._state == {}  # both executions cleared their state


# -- scheduled flag is read/written under the plugin lock --------------------


def test_scheduled_flag_lock_helpers_track_scheduling():
    # The ``scheduled`` gate is now mutated/read through the plugin lock like
    # every other _ExecutionState field. Pin the observable behavior: it starts
    # False, flips True once a record is scheduled, and the lock-guarded read
    # helper agrees with the raw attribute. The SDK serializes hooks, so this is
    # a defensive/consistency check rather than a concurrency race test.
    exporter = _BufferedExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    op = _step("s", "1")

    plugin.on_invocation_start(_start({}))  # on-complete: nothing scheduled yet
    state = plugin._state[ARN]
    assert state.scheduled is False
    assert plugin._was_scheduled(state) is False

    plugin.on_invocation_end(_end(_ops(op)))  # schedules the terminal record
    # State is cleared at invocation end, but the local reference still reflects
    # the flip performed via the lock helper before the drain.
    assert state.scheduled is True
    assert plugin._was_scheduled(state) is True
    assert len(exporter.published) == 1


def test_no_op_invocation_leaves_scheduled_false():
    # on-complete + non-terminal (PENDING/RETRY) end schedules nothing, so the
    # gate stays False and no flush/lane work is triggered.
    exporter = _BufferedExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[exporter]))
    plugin.on_invocation_start(_start({}))
    state = plugin._state[ARN]
    pending_end = InvocationEndInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=T0,
        status=InvocationStatus.PENDING,
        error=None,
        execution_result=None,
        operations={},
    )
    plugin.on_invocation_end(pending_end)
    assert state.scheduled is False
    assert exporter.published == []
