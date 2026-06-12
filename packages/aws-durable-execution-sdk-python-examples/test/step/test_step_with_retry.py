"""Tests for step_with_retry example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.step import step_with_retry
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_retry.handler,
    lambda_function_name="step with retry",
)
def test_step_with_retry(durable_runner):
    """Test step with retry configuration.

    With counter-based deterministic behavior:
    - Attempt 1: counter = 1 < 2 → raises RuntimeError ❌
    - Attempt 2: counter = 2 >= 2 → succeeds ✓

    The function deterministically fails once then succeeds on the second attempt.
    """
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    # With counter-based deterministic behavior, succeeds on attempt 2
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
