"""End-to-end failed invocation handling through the test runner."""

from typing import Any

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.execution import ExecutionStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def test_failed_invocation_without_error_sets_execution_status() -> None:
    def handler(event: Any, context: Any) -> dict[str, str]:  # noqa: ARG001
        return {"Status": "FAILED"}

    with DurableFunctionTestRunner(handler=handler, execution_timeout=10) as runner:
        execution_arn = runner.run_async(input="input str")
        result: DurableFunctionTestResult = runner.wait_for_result(
            execution_arn, timeout=10
        )

    assert result.status is InvocationStatus.FAILED
    assert result.error is None
    assert result.execution_status is ExecutionStatus.FAILED
