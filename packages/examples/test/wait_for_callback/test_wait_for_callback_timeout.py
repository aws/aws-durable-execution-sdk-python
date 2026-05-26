"""Tests for wait_for_callback_timeout."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback_timeout
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_timeout.handler,
    lambda_function_name="Wait For Callback Timeout",
)
def test_handle_wait_for_callback_timeout_scenarios(durable_runner):
    """Test waitForCallback timeout scenarios."""
    test_payload = {"test": "timeout-scenario"}

    with durable_runner:
        execution_arn = durable_runner.run_async(input=test_payload, timeout=2)
        # Don't send callback - let it timeout
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    # Handler catches the timeout error, so execution succeeds with error in result
    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data["success"] is False
    assert isinstance(result_data["error"], str)
    assert len(result_data["error"]) > 0
    assert "Callback timed out: Callback.Timeout" == result_data["error"]
