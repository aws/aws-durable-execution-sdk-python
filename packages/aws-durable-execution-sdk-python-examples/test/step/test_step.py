"""Tests for step example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.step import step
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step.handler,
    lambda_function_name="Basic Step",
)
def test_step(durable_runner):
    """Test basic step example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 8

    step_result = result.get_step("add_numbers")
    assert deserialize_operation_payload(step_result.result) == 8
