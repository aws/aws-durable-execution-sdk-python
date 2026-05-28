"""Tests for create_callback_concurrent."""

import json

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback_concurrency
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_concurrency.handler,
    lambda_function_name="Create Callback Concurrency",
)
def test_handle_multiple_concurrent_callback_operations(durable_runner):
    """Test handling multiple concurrent callback operations."""
    with durable_runner:
        # Start the execution (this will pause at the callbacks)
        execution_arn = durable_runner.run_async(input=None, timeout=60)

        callback_id_1 = durable_runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-1"
        )
        callback_id_2 = durable_runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-2"
        )
        callback_id_3 = durable_runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-3"
        )

        callback_result_2 = json.dumps(
            {
                "id": 2,
                "data": "second",
            }
        )
        durable_runner.send_callback_success(
            callback_id=callback_id_2, result=callback_result_2.encode()
        )

        callback_result_1 = json.dumps(
            {
                "id": 1,
                "data": "first",
            }
        )
        durable_runner.send_callback_success(
            callback_id=callback_id_1, result=callback_result_1.encode()
        )

        callback_result_3 = json.dumps(
            {
                "id": 3,
                "data": "third",
            }
        )
        durable_runner.send_callback_success(
            callback_id=callback_id_3, result=callback_result_3.encode()
        )

        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data == {
        "results": [callback_result_1, callback_result_2, callback_result_3],
        "allCompleted": True,
    }

    # Verify all callback operations were tracked
    operations = result.get_context("parallel_callbacks")

    assert len(operations.child_operations) == 3

    # Verify all operations are CALLBACK type
    for op in operations.child_operations:
        assert op.operation_type.value == "CONTEXT"
        assert len(op.child_operations) == 1
        assert op.child_operations[0].operation_type.value == "CALLBACK"
