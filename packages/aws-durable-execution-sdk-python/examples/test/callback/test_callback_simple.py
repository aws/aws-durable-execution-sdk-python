"""Tests for callback example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback_simple
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_simple.handler,
    lambda_function_name="Callback Success",
)
def test_callback_success(durable_runner):
    callback_result = "successful"

    with durable_runner:
        execution_arn = durable_runner.run_async(input=None, timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        durable_runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == callback_result


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_simple.handler,
    lambda_function_name="Callback Success None",
)
def test_callback_success_none_result(durable_runner):
    with durable_runner:
        execution_arn = durable_runner.run_async(input=None, timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        durable_runner.send_callback_success(callback_id=callback_id, result=b"")
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data is None
