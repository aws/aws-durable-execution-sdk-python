"""Tests for wait_for_condition."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_condition import wait_for_condition
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_condition.handler,
    lambda_function_name="wait for condition",
)
def test_wait_for_condition(durable_runner):
    """Test wait_for_condition pattern."""
    pass
    # TODO: fix bug in local runner so that local tests can pass
    # with durable_runner:
    #     result = durable_runner.run(input="test", timeout=30)

    # assert result.status is InvocationStatus.SUCCEEDED
    # # Should reach state 3 after 3 increments
    # assert deserialize_operation_payload(result.result) == 3
