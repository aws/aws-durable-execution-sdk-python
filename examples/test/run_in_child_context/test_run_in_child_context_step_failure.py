"""Tests for run_in_child_context_failing_step."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.run_in_child_context import run_in_child_context_step_failure
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=run_in_child_context_step_failure.handler,
    lambda_function_name="Run In Child Context With Failing Step",
)
def test_succeed_despite_failing_step_in_child_context(durable_runner):
    """Test that execution succeeds despite failing step in child context."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == {"success": True, "error": "Step failed in child context"}
