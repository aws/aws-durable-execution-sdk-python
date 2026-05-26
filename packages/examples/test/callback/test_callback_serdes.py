"""Tests for create_callback_serdes."""

import json
from datetime import datetime, timezone

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback.callback_serdes import CustomData, CustomDataSerDes
from src.callback import callback_serdes
from test.conftest import deserialize_operation_payload


class CustomDataTestSerDes(CustomDataSerDes):
    """Test version of CustomDataSerDes for use in tests."""

    pass


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_serdes.handler,
    lambda_function_name="Create Callback Custom Serdes",
)
def test_handle_callback_operations_with_custom_serdes(durable_runner):
    """Test callback operations with custom serdes."""
    with durable_runner:
        # Start the execution (this will pause at the callback)
        execution_arn = durable_runner.run_async(input=None, timeout=30)

        # Wait for callback and get callback_id
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        # Send data that requires custom serialization
        test_data = CustomData(
            id=42,
            message="Hello World",
            timestamp=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        )

        # Serialize the data using custom serdes for sending
        serdes = CustomDataTestSerDes()
        serialized_data = serdes.serialize(test_data, None)

        durable_runner.send_callback_success(
            callback_id=callback_id, result=serialized_data.encode()
        )

        # Wait for the execution to complete
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify the result structure
    assert result_data["receivedData"]["id"] == 42
    assert result_data["receivedData"]["message"] == "Hello World"
    assert "2025-01-01T00:00:00" in result_data["receivedData"]["timestamp"]
    assert result_data["isDateObject"] is True
