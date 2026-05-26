"""Tests for wait_for_callback_heartbeat_sends."""

import json
import time

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback_heartbeat
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_heartbeat.handler,
    lambda_function_name="Wait For Callback Heartbeat Sends",
)
def test_handle_wait_for_callback_heartbeat_scenarios_during_long_running_submitter(
    durable_runner,
):
    """Test waitForCallback heartbeat scenarios during long-running submitter execution."""

    with durable_runner:
        # Start the execution (this will pause at the callback)
        execution_arn = durable_runner.run_async(
            input={"input": "test_payload"}, timeout=60
        )

        # Wait for callback and get callback_id
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        # Send heartbeat to keep the callback alive during processing
        durable_runner.send_callback_heartbeat(callback_id=callback_id)

        # Wait a bit more to simulate callback processing time
        wait_time = 7.0
        time.sleep(wait_time)

        # Send another heartbeat
        durable_runner.send_callback_heartbeat(callback_id=callback_id)

        # Finally complete the callback
        callback_result = json.dumps({"processed": 1000})
        durable_runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        # Wait for the execution to complete
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data["callbackResult"] == callback_result
    assert result_data["completed"] is True

    # Should have completed operations with successful callback
    completed_operations = [
        op for op in result.operations if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) > 0
