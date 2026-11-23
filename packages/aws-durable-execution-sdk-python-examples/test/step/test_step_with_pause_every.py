"""Tests for the step_with_pause_every example.

The local runner pauses the execution when the step asks it to. The test
resumes it, like an operator would with ResumeDurableExecution.
"""

import threading
import time

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestRunner,
)

from aws_durable_execution_sdk_python.execution import InvocationStatus
from src.step import step_with_pause_every
from test.conftest import deserialize_operation_payload


def _count_events(runner: DurableFunctionTestRunner, arn: str, event_type: str) -> int:
    return sum(
        1
        for event in runner.get_execution_history(arn).events
        if event.event_type == event_type
    )


def _wait_for_failed_attempts(
    runner: DurableFunctionTestRunner, arn: str, count: int
) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _count_events(runner, arn, "StepFailed") >= count:
            return
        threading.Event().wait(0.01)
    assert _count_events(runner, arn, "StepFailed") >= count


def test_step_with_pause_every_pauses_after_third_attempt():
    """The execution pauses after attempt 3 and finishes after resume."""
    with DurableFunctionTestRunner(
        handler=step_with_pause_every.handler, execution_timeout=30
    ) as runner:
        arn = runner.run_async(input="{}")

        _wait_for_failed_attempts(runner, arn, 3)
        threading.Event().wait(0.3)
        # Paused: attempt 4 does not start until the execution is resumed.
        assert _count_events(runner, arn, "StepFailed") == 3
        assert _count_events(runner, arn, "ExecutionSucceeded") == 0

        runner.resume_execution(arn)
        result = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Partner API answered"
    assert result.get_step("call_partner_api").attempt == 5
