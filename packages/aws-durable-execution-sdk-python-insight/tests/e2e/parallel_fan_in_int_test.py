# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test for on-change export bursts during parallel fan-in."""

from __future__ import annotations

import threading
import time
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

from aws_durable_execution_sdk_python_insight import (
    WorkflowInsightConfig,
    workflow_insight,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


_BRANCH_COUNT = 6


class _BlockingCaptureExporter:
    """Blocks the first export so the real hook burst queues deterministically."""

    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.started = threading.Event()
        self.release = threading.Event()
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        if not self.started.is_set():
            self.started.set()
            self.release.wait(10.0)
        with self._lock:
            self.records.append(record)

    def flush(self) -> None:
        return None

    def snapshots(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.records)


def _branch(index: int):
    def run(context: DurableContext) -> int:
        return context.step(lambda _step_context: index, name=f"step-{index}")

    return run


def _parallel_handler(event: Any, context: DurableContext) -> list[int]:  # noqa: ARG001
    return context.parallel(
        [_branch(index) for index in range(_BRANCH_COUNT)],
        name="fan-in",
    ).get_results()


def _wait_until(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def test_parallel_fan_in_preserves_every_on_change_snapshot() -> None:
    capture = _BlockingCaptureExporter()
    plugin = workflow_insight(
        WorkflowInsightConfig(
            exporters=[capture],
            emit_mode="on-change",
            operation_detail="full-tree",
        )
    )
    handler = durable_execution(_parallel_handler, plugins=[plugin])
    results: list[DurableFunctionTestResult] = []

    with DurableFunctionTestRunner(handler=handler, execution_timeout=15) as runner:
        run_thread = threading.Thread(
            target=lambda: results.append(runner.run(input="{}")),
            daemon=True,
        )
        run_thread.start()
        try:
            assert capture.started.wait(5.0)
            lane = plugin._scheduler._lanes[0]
            # Invocation start is in flight. Three real PluginExecutor changes
            # and the terminal record must queue behind it before release.
            assert _wait_until(lambda: lane._pending_record_count() == 4)
        finally:
            capture.release.set()
            run_thread.join(10.0)

    assert not run_thread.is_alive()
    assert len(results) == 1
    assert results[0].status is InvocationStatus.SUCCEEDED

    records = capture.snapshots()
    assert [record["status"] for record in records] == [
        "RUNNING",
        "RUNNING",
        "RUNNING",
        "RUNNING",
        "SUCCEEDED",
    ]
    final_names = {operation["name"] for operation in records[-1]["operations"]}
    assert "fan-in" in final_names
    assert {f"step-{index}" for index in range(_BRANCH_COUNT)} <= final_names
