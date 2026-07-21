"""Tests for wait_for_callback_timeout."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
    OperationType,
)

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
        # Don't send callback - let it timeout.
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    # Handler catches the timeout error, so execution succeeds with error in result.
    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data["success"] is False
    assert isinstance(result_data["error"], str)
    assert len(result_data["error"]) > 0
    assert result_data["error"] == "Callback timed out"

    callback_operations = [
        operation
        for operation in result.get_all_operations()
        if operation.operation_type is OperationType.CALLBACK
    ]

    assert callback_operations
    assert any(
        operation.status is OperationStatus.TIMED_OUT
        for operation in callback_operations
    )

    # The wait_for_callback context records the graded CallbackTimeoutError,
    # identical on first run and replay.
    wait_for_callback_ops = [
        operation
        for operation in result.get_all_operations()
        if operation.sub_type is OperationSubType.WAIT_FOR_CALLBACK
    ]
    assert len(wait_for_callback_ops) == 1
    wait_for_callback_error = wait_for_callback_ops[0].error
    assert wait_for_callback_error is not None
    assert wait_for_callback_error.type == "CallbackTimeoutError"
