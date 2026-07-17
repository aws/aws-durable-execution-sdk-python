"""Tests for step_with_retry example."""

from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.context import StepContext
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.step import step_with_retry
from test.conftest import deserialize_operation_payload


def test_unreliable_operation_uses_durable_attempt():
    """Retry behavior is independent of process-level state."""
    operation = step_with_retry.unreliable_operation()
    first_attempt = StepContext(logger=Mock(), attempt=1)
    second_attempt = StepContext(logger=Mock(), attempt=2)

    with pytest.raises(RuntimeError, match="Attempt 1 failed"):
        operation(first_attempt)

    assert operation(second_attempt) == "Operation succeeded"

    with pytest.raises(RuntimeError, match="Attempt 1 failed"):
        operation(first_attempt)


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_retry.handler,
    lambda_function_name="step with retry",
)
def test_step_with_retry(durable_runner):
    """Test step with retry configuration.

    With durable-attempt-based deterministic behavior:
    - Attempt 1: step context attempt = 1, so the operation raises RuntimeError.
    - Attempt 2: step context attempt = 2, so the operation succeeds.

    The function deterministically fails once then succeeds on the second attempt.
    """
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    # With durable-attempt-based deterministic behavior, succeeds on attempt 2
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Operation succeeded"

    # Verify step operation exists with retry details
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1

    # The step should have succeeded on attempt 2 (after 1 failure)
    # Attempt numbering: 1 (initial attempt), 2 (first retry)
    step_op = step_ops[0]
    assert step_op.attempt == 2  # Succeeded on first retry (1-indexed: 2=first retry)
