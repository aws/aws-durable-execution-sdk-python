"""Tests for wait_for_callback_multiple_invocations."""

import json
import time

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import (
    wait_for_callback_multiple_invocations,
)
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_multiple_invocations.handler,
    lambda_function_name="Wait For Callback Multiple Invocations",
)
def test_handle_multiple_invocations_tracking_with_wait_for_callback_operations(
    durable_runner,
):
    """Test multiple invocations tracking with waitForCallback operations."""
    test_payload = {"test": "multiple-invocations"}

    with durable_runner:
        # Start the execution (this will pause at callbacks)
        execution_arn = durable_runner.run_async(input=test_payload, timeout=60)

        # Wait for first callback and get callback_id
        first_callback_id = durable_runner.wait_for_callback(
            execution_arn=execution_arn
        )

        # Complete first callback
        first_callback_result = json.dumps({"step": 1})
        durable_runner.send_callback_success(
            callback_id=first_callback_id, result=first_callback_result.encode()
        )

        # Wait for second callback and get callback_id
        second_callback_id = durable_runner.wait_for_callback(
            execution_arn=execution_arn, name="second-callback create callback id"
        )

        # Complete second callback
        second_callback_result = json.dumps({"step": 2})
        durable_runner.send_callback_success(
            callback_id=second_callback_id, result=second_callback_result.encode()
        )

        # Wait for the execution to complete
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data == {
        "firstCallback": '{"step": 1}',
        "secondCallback": '{"step": 2}',
        "stepResult": {"processed": True, "step": 1},
        "invocationCount": "multiple",
    }

    # Verify invocations were tracked - should be exactly 5 invocations
    # Note: Check if Python SDK provides invocations tracking
    if hasattr(result, "invocations"):
        invocations = result.invocations
        assert len(invocations) == 5

    # Verify operations were executed
    operations = result.operations
    assert len(operations) > 4  # wait + callback + step + wait + callback operations
