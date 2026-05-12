"""Tests for with_retry_callback example.

Demonstrates that with_retry retries the entire wait_for_callback flow
when the callback fails. The external system fails 2 times before
succeeding on the 3rd attempt.
"""

import pytest
from src.with_retry import with_retry_callback
from test.conftest import deserialize_operation_payload

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import ErrorObject


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=with_retry_callback.handler,
    lambda_function_name="With Retry Callback",
)
def test_with_retry_callback_fails_twice_then_succeeds(durable_runner):
    """Test that with_retry retries the callback flow after failures.

    The external system sends callback failure 2 times, then succeeds
    on the 3rd attempt. with_retry handles the failures and retries
    the entire wait_for_callback block.
    """
    with durable_runner:
        execution_arn = durable_runner.run_async(input=None, timeout=60)

        # Attempt 1: external system fails
        callback_id_1 = durable_runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-1 create callback id",
        )
        durable_runner.send_callback_failure(
            callback_id=callback_id_1,
            error=ErrorObject.from_message("External system unavailable"),
        )

        # Attempt 2: external system fails again
        callback_id_2 = durable_runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-2 create callback id",
        )
        durable_runner.send_callback_failure(
            callback_id=callback_id_2,
            error=ErrorObject.from_message("External system timeout"),
        )

        # Attempt 3: external system succeeds
        callback_id_3 = durable_runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-3 create callback id",
        )
        durable_runner.send_callback_success(
            callback_id=callback_id_3,
            result="approval granted".encode(),
        )

        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == {
        "success": True,
        "result": "approval granted",
    }
