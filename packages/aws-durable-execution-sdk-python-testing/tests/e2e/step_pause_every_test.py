"""End-to-end tests for pausing a step from the retry decision with the local runner.

The in-memory service client pauses the execution when the SDK asks it to.
The execution then stays paused until the test calls resume_execution().

Experimental; may change or be removed in a future release.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext, StepContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.retries import (
    RetryDecision,
    RetryStrategyConfig,
    create_retry_strategy,
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
        threading.Event().wait(0.01)
    assert predicate()


def _has_succeeded(runner: DurableFunctionTestRunner, arn: str) -> bool:
    return any(
        event.event_type == "ExecutionSucceeded"
        for event in runner.get_execution_history(arn).events
    )


def _handler(
    attempts: list[int],
    succeed_on: int,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
) -> Callable[..., Any]:
    config = StepConfig(
        retry_strategy=retry_strategy
        or create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=10,
                initial_delay=Duration.from_seconds(1),
                retryable_error_types=[RuntimeError],
                pause_every=2,
            )
        ),
    )

    def flaky(step_context: StepContext) -> str:
        attempts.append(step_context.attempt)
        if step_context.attempt < succeed_on:
            msg = f"attempt {step_context.attempt} failed"
            raise RuntimeError(msg)
        return "done"

    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(flaky, name="flaky", config=config)

    return durable_execution(handler)


def test_step_pauses_after_every_second_failed_attempt() -> None:
    """Attempts 2 and 4 pause the execution until it is resumed."""
    attempts: list[int] = []

    with DurableFunctionTestRunner(
        handler=_handler(attempts, succeed_on=5), execution_timeout=15
    ) as runner:
        arn = runner.run_async(input="{}")

        _wait_until(lambda: attempts == [1, 2])
        threading.Event().wait(0.3)
        assert attempts == [1, 2]
        assert not _has_succeeded(runner, arn)

        runner.resume_execution(arn)
        _wait_until(lambda: attempts == [1, 2, 3, 4])
        threading.Event().wait(0.3)
        assert attempts == [1, 2, 3, 4]
        assert not _has_succeeded(runner, arn)

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("done")
    assert attempts == [1, 2, 3, 4, 5]
    assert result.get_step("flaky").attempt == 5


def test_step_does_not_pause_when_it_succeeds_before_a_multiple() -> None:
    """A step that succeeds on attempt 2 never reaches a pause."""
    attempts: list[int] = []

    with DurableFunctionTestRunner(
        handler=_handler(attempts, succeed_on=2), execution_timeout=15
    ) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}")

    assert result.status is InvocationStatus.SUCCEEDED
    assert attempts == [1, 2]


def test_custom_strategy_pauses_when_its_decision_says_so() -> None:
    """A custom strategy that returns pause=True on attempt 3 pauses there only."""
    attempts: list[int] = []

    def pause_on_third(_error: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.retry(Duration.from_seconds(1), pause=attempt == 3)

    with DurableFunctionTestRunner(
        handler=_handler(attempts, succeed_on=5, retry_strategy=pause_on_third),
        execution_timeout=15,
    ) as runner:
        arn = runner.run_async(input="{}")

        _wait_until(lambda: attempts == [1, 2, 3])
        threading.Event().wait(0.3)
        assert attempts == [1, 2, 3]
        assert not _has_succeeded(runner, arn)

        runner.resume_execution(arn)
        result: DurableFunctionTestResult = runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert attempts == [1, 2, 3, 4, 5]
