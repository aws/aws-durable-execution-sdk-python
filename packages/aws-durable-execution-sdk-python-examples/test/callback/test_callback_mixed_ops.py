"""Tests for create_callback_mixed_ops."""

import json
import time

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback_mixed_ops
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_mixed_ops.handler,
    lambda_function_name="Create Callback Mixed Operations",
)
def test_handle_callback_operations_mixed_with_other_operation_types(durable_runner):
    """Test callback operations mixed with other operation types."""
    with durable_runner:
        execution_arn = durable_runner.run_async(input=None, timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        callback_result = json.dumps(
            {
                "processed": True,
            }
        )
        durable_runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data == {
        "stepResult": {"userId": 123, "name": "John Doe"},
        "callbackResult": callback_result,
        "completed": True,
    }

    completed_operations = result.operations
    assert len(completed_operations) == 3

    operation_types = [op.operation_type.value for op in completed_operations]
    assert "WAIT" in operation_types
    assert "STEP" in operation_types
    assert "CALLBACK" in operation_types
