"""End-to-end tests for pausing and resuming a running execution.

``DurableFunctionTestRunner.pause_execution()`` makes the local
checkpoint server answer the execution's checkpoints without a token,
and ``resume_execution()`` reverses that.

Experimental; may change or be removed in a future release.

Covers:
* Pausing mid-step: the next step does not start while the execution is
  paused, and runs once resumed.
* A callback answered while paused does not trigger a new invocation
  until resumed.
* A wait elapsing while paused does not trigger a new invocation
  until resumed.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from threading import Event as ThreadingEvent
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def _wait_until(predicate: Callable[[], bool], timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        ThreadingEvent().wait(0.01)
    assert predicate()


def _has_history_event(
    runner: DurableFunctionTestRunner,
    execution_arn: str,
    event_type: str,
    name: str | None = None,
) -> bool:
    return any(
        event.event_type == event_type and (name is None or event.name == name)
        for event in runner.get_execution_history(execution_arn).events
    )


def _assert_execution_does_not_succeed_for(
    runner: DurableFunctionTestRunner,
    execution_arn: str,
    timeout: float = 0.2,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        assert not _has_history_event(runner, execution_arn, "ExecutionSucceeded")
        ThreadingEvent().wait(0.01)
    assert not _has_history_event(runner, execution_arn, "ExecutionSucceeded")


def test_pause_mid_step_holds_back_the_next_step_until_resumed() -> None:
    """Pausing while step1 is in flight holds back step2 until resumed."""

    def _two_step_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        """A step that takes a moment, then a second, instant step."""

        def step1(_step_context: Any) -> str:
            ThreadingEvent().wait(0.3)
            return "s1"

        def step2(_step_context: Any) -> str:
            return "s2"

        first = context.step(step1, name="step1")
        second = context.step(step2, name="step2")
        return first + second

    with DurableFunctionTestRunner(
        handler=durable_execution(_two_step_handler), execution_timeout=15
    ) as runner:
        arn = runner.run_async(input="{}")
        _wait_until(lambda: _has_history_event(runner, arn, "StepStarted", "step1"))

        runner.pause_execution(arn)

        assert not _has_history_event(runner, arn, "StepStarted", "step2")
        _assert_execution_does_not_succeed_for(runner, arn)

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("s1s2")


def test_callback_answered_while_paused_defers_invocation_until_resumed() -> None:
    """A callback success delivered while paused does not reinvoke until resumed."""

    def _callback_submitter(
        _callback_id: str, _context: WaitForCallbackContext
    ) -> None:
        """No-op: the test drives the callback directly through the runner."""

    def _callback_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        config = WaitForCallbackConfig(timeout=Duration.from_seconds(30))
        result = context.wait_for_callback(
            _callback_submitter, name="ext-call", config=config
        )
        return f"got {result}"

    with DurableFunctionTestRunner(
        handler=durable_execution(_callback_handler), execution_timeout=15
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, timeout=15)

        runner.pause_execution(arn)
        runner.send_callback_success(callback_id, result=b"ok")

        _assert_execution_does_not_succeed_for(runner, arn)

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("got ok")


def test_wait_elapsing_while_paused_defers_invocation_until_resumed() -> None:
    """A wait that elapses while paused does not reinvoke until resumed."""
    wait_seconds = 1
    wait_elapse_grace_seconds = 0.2

    def _wait_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        context.wait(Duration.from_seconds(wait_seconds), name="pause-wait")
        return "done"

    with DurableFunctionTestRunner(
        handler=durable_execution(_wait_handler), execution_timeout=15, skip_time=False
    ) as runner:
        arn = runner.run_async(input="{}")
        _wait_until(
            lambda: _has_history_event(runner, arn, "WaitStarted", "pause-wait")
        )

        runner.pause_execution(arn)
        # Observe longer than the durable wait so it can elapse while paused.
        _assert_execution_does_not_succeed_for(
            runner, arn, timeout=wait_seconds + wait_elapse_grace_seconds
        )

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("done")

def test_pause_execution_on_unknown_arn_raises_not_found() -> None:

    def _handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return "done"

    with DurableFunctionTestRunner(
        handler=durable_execution(_handler), execution_timeout=15
    ) as runner:
        with pytest.raises(ResourceNotFoundException) as exc_info:
            runner.pause_execution("arn:aws:states:us-west-2:123456789012:express:unknown-fn:unknown-exec:0000")
        assert exc_info.value.Message == "Durable Execution does not exist"


def test_resume_execution_on_unknown_arn_raises_not_found() -> None:

    def _handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return "done"

    with DurableFunctionTestRunner(
        handler=durable_execution(_handler), execution_timeout=15
    ) as runner:
        with pytest.raises(ResourceNotFoundException) as exc_info:
            runner.resume_execution("arn:aws:states:us-west-2:123456789012:express:unknown-fn:unknown-exec:0000")
        assert exc_info.value.Message == "Durable Execution does not exist"
