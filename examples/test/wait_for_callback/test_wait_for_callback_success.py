import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import wait_for_callback
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback.handler,
    lambda_function_name="Wait For Callback Success",
)
def test_wait_for_callback_success(durable_runner):
    with durable_runner:
        execution_arn = durable_runner.run_async(input="test", timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        durable_runner.send_callback_success(
            callback_id=callback_id, result="callback success".encode()
        )
        result = durable_runner.wait_for_result(execution_arn=execution_arn)
    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        deserialize_operation_payload(result.result)
        == "External system result: callback success"
    )
