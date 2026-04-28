"""Tests for create_callback_heartbeat."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
import time
import json
from src.callback import callback_heartbeat
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_heartbeat.handler,
    lambda_function_name="Create Callback Heartbeat",
)
def test_handle_callback_operations_with_failure_uncaught(durable_runner):
    """Test handling callback operations with failure."""
    test_payload = {"shouldCatchError": False}

    heartbeat_interval = 5
    total_duration = 20
    num_heartbeats = total_duration // heartbeat_interval

    with durable_runner:
        execution_arn = durable_runner.run_async(input=test_payload, timeout=30)

        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        for i in range(num_heartbeats):
            print(
                f"Sending heartbeat {i + 1}/{num_heartbeats} at {(i + 1) * heartbeat_interval}s"
            )
            durable_runner.send_callback_heartbeat(callback_id=callback_id)
            time.sleep(heartbeat_interval)

        callback_result = json.dumps(
            {
                "status": "completed",
                "data": "success after heartbeats",
            }
        )
        durable_runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        result = durable_runner.wait_for_result(execution_arn=execution_arn)
    assert result.status is InvocationStatus.SUCCEEDED

    # Assert the callback result is returned
    result_data = deserialize_operation_payload(result.result)
    assert result_data == callback_result
