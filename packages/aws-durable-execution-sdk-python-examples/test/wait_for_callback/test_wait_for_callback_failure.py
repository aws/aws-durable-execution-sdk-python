import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from src.wait_for_callback import wait_for_callback


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback.handler,
    lambda_function_name="Wait For Callback Failure",
)
def test_wait_for_callback_failure(durable_runner):
    with durable_runner:
        execution_arn = durable_runner.run_async(input="test", timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        durable_runner.send_callback_failure(
            callback_id=callback_id, error=ErrorObject.from_message("my callback error")
        )
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.FAILED
    assert isinstance(result.error, ErrorObject)
    # The callback failure raises CallbackError inside the child context that
    # wait_for_callback runs in; it is wrapped as ChildContextError so first run
    # and replay surface an identical type. The original CallbackError type is
    # preserved on the error's error_type/__cause__.
    assert result.error.to_dict() == {
        "ErrorMessage": "my callback error",
        "ErrorType": "ChildContextError",
    }
