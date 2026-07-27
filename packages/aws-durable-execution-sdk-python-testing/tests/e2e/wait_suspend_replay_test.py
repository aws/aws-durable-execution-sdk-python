"""End-to-end test: a wait completed while suspended is delivered as new.

A top-level wait suspends the execution. The wait completes between
invocations, and the next invocation must observe the completion as a
NEW operation update rather than a replayed one, finishing the
execution in exactly two invocations. Guards the plugin-visible
contract: ``on_operation_end`` fires once for the wait with
``is_replayed`` False and an end time set.
"""

from __future__ import annotations

from typing import Any, ClassVar

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
)

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

_WAIT_NAME: str = "suspend-wait"


class RecordingWaitPlugin(DurableInstrumentationPlugin):
    """Records end notifications for the wait and counts invocations."""

    invocation_count: ClassVar[int] = 0
    wait_end_infos: ClassVar[list[OperationEndInfo]] = []

    @classmethod
    def reset(cls) -> None:
        cls.invocation_count = 0
        cls.wait_end_infos.clear()

    def on_invocation_start(self, info: InvocationStartInfo) -> None:  # noqa: ARG002
        type(self).invocation_count += 1

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type is OperationType.WAIT and info.name == _WAIT_NAME:
            self.wait_end_infos.append(info)


def _wait_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    """Suspend on a top-level wait, then finish."""
    context.wait(Duration.from_seconds(1), name=_WAIT_NAME)
    return "done"


wait_handler = durable_execution(_wait_handler, plugins=[RecordingWaitPlugin()])


def test_wait_completed_during_suspend_is_delivered_as_new() -> None:
    """The resumed invocation sees the wait completion as a new update.

    The runner takes exactly one suspend and one resume invocation, and
    the plugin observes exactly one SUCCEEDED end notification for the
    wait with ``is_replayed`` False.
    """
    RecordingWaitPlugin.reset()

    with DurableFunctionTestRunner(
        handler=wait_handler, execution_timeout=15
    ) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}")

    assert result.status is InvocationStatus.SUCCEEDED

    wait_op = result.get_wait(_WAIT_NAME)
    assert wait_op.status is OperationStatus.SUCCEEDED

    # Exactly one suspending invocation and one resuming invocation.
    assert RecordingWaitPlugin.invocation_count == 2

    succeeded_infos: list[OperationEndInfo] = [
        info
        for info in RecordingWaitPlugin.wait_end_infos
        if info.status is OperationStatus.SUCCEEDED
    ]
    assert len(succeeded_infos) == 1
    assert succeeded_infos[0].is_replayed is False
    assert succeeded_infos[0].end_time is not None
