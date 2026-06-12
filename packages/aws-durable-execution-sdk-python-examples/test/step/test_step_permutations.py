"""Tests for step operation permutations."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src.step import step_no_name, step_with_exponential_backoff, step_with_name
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_no_name.handler,
    lambda_function_name="step no name",
)
def test_step_no_name(durable_runner):
    """Test step without explicit name."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Result: Step without name"

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    # Should use function name when no name provided
    assert step_ops[0].name is None or step_ops[0].name == "<lambda>"


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_name.handler,
    lambda_function_name="step with name",
)
def test_step_with_name(durable_runner):
    """Test step with explicit name."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        deserialize_operation_payload(result.result)
        == "Result: Step with explicit name"
    )

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "custom_step"


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_exponential_backoff.handler,
    lambda_function_name="step with exponential backoff",
)
def test_step_with_exponential_backoff(durable_runner):
    """Test step with exponential backoff retry strategy."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        deserialize_operation_payload(result.result)
        == "Result: Step with exponential backoff"
    )

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "retry_step"
