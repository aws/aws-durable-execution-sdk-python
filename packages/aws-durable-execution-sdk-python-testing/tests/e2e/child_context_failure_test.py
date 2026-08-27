"""End-to-end child context failure handling through the test runner."""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationStatus,
)
from aws_durable_execution_sdk_python.retries import RetryPresets
from aws_durable_execution_sdk_python.types import StepContext

from aws_durable_execution_sdk_python_testing.runner import (
    ContextOperation,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def test_caught_child_context_failure_does_not_fail_root_execution() -> None:
    @durable_step
    def failing_step(step_context: StepContext) -> str:  # noqa: ARG001
        msg = "Child step failed"
        raise RuntimeError(msg)

    @durable_with_child_context
    def failing_child(ctx: DurableContext) -> str:
        return ctx.step(
            failing_step(),
            config=StepConfig(retry_strategy=RetryPresets.none()),
        )

    @durable_step
    def recovery_step(step_context: StepContext, value: str) -> str:  # noqa: ARG001
        return value

    @durable_execution
    def handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        try:
            context.run_in_child_context(failing_child(), name="failing-child")
        except Exception:
            pass

        return context.step(recovery_step("handled"))

    with DurableFunctionTestRunner(handler=handler, execution_timeout=10) as runner:
        result: DurableFunctionTestResult = runner.run(input="input str")

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("handled")

    child_op: ContextOperation = result.get_context("failing-child")
    assert child_op.status is OperationStatus.FAILED
    assert child_op.error is not None
    assert child_op.error.message is not None
    assert "Child step failed" in child_op.error.message
