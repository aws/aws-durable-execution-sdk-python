# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test: the terminal Workflow Insight record captures prior work.

Drives the plugin through the repository's LOCAL durable runner
(``DurableFunctionTestRunner``) and the real ``@durable_execution`` /
``PluginExecutor`` lifecycle -- not by calling hooks directly. A durable
function runs a named step, then suspends on a named wait, then resumes and
completes. The wait completes while the execution is suspended, so the run
takes two invocations (suspend + resume). On the resuming (terminal)
invocation the plugin emits one ``SUCCEEDED`` record, and that record must
include both the earlier step and the now-completed wait -- proving the plugin
sources operations from the SDK's authoritative invocation snapshots across a
suspend/resume boundary.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
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


_STEP_NAME = "greet"
_WAIT_NAME = "pause"


class _CaptureExporter:
    """Records every insight record delivered to it (destination boundary only)."""

    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return None


def _insight_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    """A named step, then a wait that suspends the execution, then completion."""
    context.step(lambda _step_ctx: "greeted", name=_STEP_NAME)
    context.wait(Duration.from_seconds(1), name=_WAIT_NAME)
    return "done"


def test_terminal_record_includes_prior_step_and_completed_wait() -> None:
    capture = _CaptureExporter()
    plugin = workflow_insight(WorkflowInsightConfig(exporters=[capture]))
    # Functional form (not the decorator-factory form) so the wrapped handler's
    # static type stays a plain 2-arg callable for the runner.
    handler = durable_execution(_insight_handler, plugins=[plugin])

    with DurableFunctionTestRunner(handler=handler, execution_timeout=15) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}")

    assert result.status is InvocationStatus.SUCCEEDED

    # on-complete default: nothing is emitted for the suspending invocation, one
    # terminal record is emitted on the resuming invocation.
    assert len(capture.records) == 1
    record = capture.records[0]
    assert record["status"] == "SUCCEEDED"
    # Terminal record carries an end time / duration (comment 3).
    assert "endTime" in record
    assert record["durationMs"] is not None

    ops_by_name = {op["name"]: op for op in record["operations"]}
    assert _STEP_NAME in ops_by_name, "prior step missing from terminal record"
    assert _WAIT_NAME in ops_by_name, "completed wait missing from terminal record"
    assert ops_by_name[_STEP_NAME]["status"] == "SUCCEEDED"
    assert ops_by_name[_WAIT_NAME]["status"] == "SUCCEEDED"
