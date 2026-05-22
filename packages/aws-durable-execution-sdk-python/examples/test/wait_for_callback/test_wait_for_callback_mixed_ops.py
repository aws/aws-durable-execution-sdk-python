"""Tests for wait_for_callback_mixed_ops."""

import json

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback_mixed_ops
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_mixed_ops.handler,
    lambda_function_name="Wait For Callback Mixed Ops",
)
def test_handle_wait_for_callback_mixed_with_steps_waits_and_other_operations(
    durable_runner,
):
    """Test waitForCallback mixed with steps, waits, and other operations."""
    with durable_runner:
        # Start the execution (this will pause at the callback)
        execution_arn = durable_runner.run_async(input=None, timeout=30)

        # Wait for callback and get callback_id
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        # Complete the callback
        callback_result = json.dumps({"processed": True})
        durable_runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        # Wait for the execution to complete
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify all expected fields
    assert result_data["stepResult"] == {"userId": 123, "name": "John Doe"}
    assert result_data["callbackResult"] == callback_result
    assert result_data["finalStep"]["status"] == "completed"
    assert isinstance(result_data["finalStep"]["timestamp"], int)
    assert result_data["workflowCompleted"] is True

    # Verify all operations were tracked - should have wait, step, waitForCallback (context + callback + submitter), wait, step
    completed_operations = [
        op for op in result.get_all_operations() if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) == 7
