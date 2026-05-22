"""Tests for wait_for_callback_failing_submitter."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback_submitter_failure_catchable
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_submitter_failure_catchable.handler,
    lambda_function_name="Wait For Callback Failing Submitter Catchable",
)
def test_handle_wait_for_callback_with_failing_submitter_function_errors(
    durable_runner,
):
    """Test waitForCallback with failing submitter function errors."""
    with durable_runner:
        execution_arn = durable_runner.run_async(input=None, timeout=30)
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    result_data = deserialize_operation_payload(result.result)

    assert result_data == {
        "success": False,
        "error": "Submitter failed",
    }
