"""Tests for step_semantics_at_most_once example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src.step import step_semantics_at_most_once
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_semantics_at_most_once.handler,
    lambda_function_name="step semantics at most once",
)
def test_step_semantics_at_most_once(durable_runner):
    """Test step with at-most-once semantics."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        deserialize_operation_payload(result.result)
        == "Result: AT_MOST_ONCE_PER_RETRY semantics"
    )

    # Verify step operation exists with correct name
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "at_most_once_step"
