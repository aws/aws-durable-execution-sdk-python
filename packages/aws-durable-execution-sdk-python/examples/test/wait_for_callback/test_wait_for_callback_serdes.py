"""Tests for wait_for_callback_serdes."""

import json
from datetime import datetime, timezone

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback_serdes
from src.wait_for_callback.wait_for_callback_serdes import CustomSerdes
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_serdes.handler,
    lambda_function_name="Wait For Callback Serdes",
)
def test_handle_wait_for_callback_with_custom_serdes_configuration(durable_runner):
    """Test waitForCallback with custom serdes configuration."""
    with durable_runner:
        # Start the execution (this will pause at the callback)
        execution_arn = durable_runner.run_async(input=None, timeout=30)

        # Wait for callback and get callback_id
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        # Send data that requires custom serialization
        test_data = {
            "id": 42,
            "message": "Hello Custom Serdes",
            "timestamp": datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc),
            "metadata": {
                "version": "2.0.0",
                "processed": True,
            },
        }

        # Serialize the data using custom serdes for sending
        custom_serdes = CustomSerdes()
        serialized_data = custom_serdes.serialize(test_data)
        durable_runner.send_callback_success(
            callback_id=callback_id, result=serialized_data.encode()
        )

        # Wait for the execution to complete
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # The result will always get stringified since it's the lambda response
    # DateTime will be serialized to ISO string in the final result
    assert result_data["receivedData"]["id"] == 42
    assert result_data["receivedData"]["message"] == "Hello Custom Serdes"
    assert "2025-06-15T12:30:45" in result_data["receivedData"]["timestamp"]
    assert result_data["receivedData"]["metadata"]["version"] == "2.0.0"
    assert result_data["receivedData"]["metadata"]["processed"] is True
    assert result_data["isDateObject"] is True

    # Should have completed operations with successful callback
    completed_operations = [
        op for op in result.operations if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) > 0
