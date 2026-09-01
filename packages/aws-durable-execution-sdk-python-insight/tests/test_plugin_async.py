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
