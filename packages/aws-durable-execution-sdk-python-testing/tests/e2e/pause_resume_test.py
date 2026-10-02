"""End-to-end tests for pausing and resuming a running execution.

``DurableFunctionTestRunner.pause_execution()`` makes the local
checkpoint server answer the execution's checkpoints without a token,
and ``resume_execution()`` reverses that. Experimental; may change or
be removed in a future release.

Covers:

* Pausing mid-step: the handler's in-flight step finishes and
  checkpoints, but the next step does not start until resumed.
* A callback answered while paused does not trigger a new invocation
  until resumed.
* A wait elapsing while paused does not trigger a new invocation
  until resumed.
"""

from __future__ import annotations

import json
import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

from aws_durable_execution_sdk_python_testing.execution import Execution

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def _two_step_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    """A step that takes a moment, then a second, instant step."""

    def step1(_step_context: Any) -> str:
        time.sleep(0.3)
        return "s1"

    def step2(_step_context: Any) -> str:
        return "s2"

    first = context.step(step1, name="step1")
    second = context.step(step2, name="step2")
    return first + second


two_step_handler = durable_execution(_two_step_handler)


def test_pause_mid_step_defers_the_next_step_until_resumed() -> None:
    """Pausing while step1 is in flight holds back step2 until resumed."""
    with DurableFunctionTestRunner(
        handler=two_step_handler, execution_timeout=15
    ) as runner:
        arn = runner.run_async(input="{}")

        # Let step1 start before pausing.
        time.sleep(0.05)
        runner.pause_execution(arn)

        execution: Execution = runner._store.load(arn)  # noqa: SLF001
        assert execution.is_complete is False
        assert execution.paused is True

        operation_names = {op.name for op in execution.get_navigable_operations()}
        assert "step2" not in operation_names

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("s1s2")


def _callback_submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """No-op: the test drives the callback directly through the runner."""


def _callback_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    config = WaitForCallbackConfig(timeout=Duration.from_seconds(30))
    result = context.wait_for_callback(
        _callback_submitter, name="ext-call", config=config
    )
    return f"got {result}"


callback_handler = durable_execution(_callback_handler)


def test_callback_answered_while_paused_defers_invocation_until_resumed() -> None:
    """A callback success delivered while paused does not reinvoke until resumed."""
    with DurableFunctionTestRunner(
        handler=callback_handler, execution_timeout=15
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, timeout=15)

        runner.pause_execution(arn)
        runner.send_callback_success(callback_id, result=b"ok")

        # The callback answer must not complete the execution while paused.
        time.sleep(0.2)
        execution: Execution = runner._store.load(arn)  # noqa: SLF001
        assert execution.is_complete is False
        assert execution.paused is True

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("got ok")


def _wait_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    context.wait(Duration.from_seconds(1), name="pause-wait")
    return "done"


wait_handler = durable_execution(_wait_handler)


def test_wait_elapsing_while_paused_defers_invocation_until_resumed() -> None:
    """A wait that elapses while paused does not reinvoke until resumed."""
    with DurableFunctionTestRunner(
        handler=wait_handler, execution_timeout=15, skip_time=True
    ) as runner:
        arn = runner.run_async(input="{}")

        # Let the first invocation suspend on the wait before pausing.
        time.sleep(0.1)
        runner.pause_execution(arn)

        # Let the (skipped) wait timer elapse while paused.
        time.sleep(0.2)
        execution: Execution = runner._store.load(arn)  # noqa: SLF001
        assert execution.is_complete is False
        assert execution.paused is True

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("done")
