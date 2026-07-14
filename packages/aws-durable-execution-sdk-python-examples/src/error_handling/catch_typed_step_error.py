"""Demonstrates catching a typed StepError raised by a failed step.

A failure inside a durable operation surfaces as a typed
DurableOperationError subclass (StepError, InvokeError, ChildContextError,
WaitForConditionError) rather than a single catch-all error, so callers can
handle each operation's failure distinctly.

The handler also crosses a wait/replay boundary after catching the error. This
shows the typed StepError is deterministic across replay: on the resume
invocation the handler replays from the top, the failed step is reconstructed
from its checkpoint (its body is NOT re-executed), and it surfaces the identical
StepError, which the same ``except`` catches again.
"""

from typing import Any

from aws_durable_execution_sdk_python import StepError
from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from aws_durable_execution_sdk_python.types import StepContext


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Run a step that fails, catch the typed StepError, then replay across a wait."""

    def failing_step(_step_context: StepContext) -> None:
        raise ValueError("payment declined")

    # A single attempt so the step fails without scheduling retries.
    config = StepConfig(
        retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
    )

    outcome: dict[str, Any] = {"handled": False}
    try:
        context.step(failing_step, name="charge-card", config=config)
    except StepError as error:
        # The class identifies the operation kind (StepError); error_type carries
        # the original failure type (ValueError).
        outcome = {
            "handled": True,
            "caught": type(error).__name__,
            "error_type": error.error_type,
            "message": error.message,
        }

    # Replay boundary: the execution suspends here and resumes as a new
    # invocation that replays from the top. On resume, the failed step above is
    # reconstructed from its checkpoint and raises the same StepError, which the
    # except catches again — so `outcome` is identical on first run and replay.
    context.wait(duration=Duration.from_seconds(1), name="post-failure-cooldown")

    return outcome
